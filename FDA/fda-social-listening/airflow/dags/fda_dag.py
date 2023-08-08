from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.kubernetes import secret, pod
from datetime import datetime, timedelta


secret_vol = secret.Secret(
    # Expose the secret as environment variable.
    deploy_type='volume',
    # The name of the environment variable, since deploy_type is `env` rather
    # than `volume`.
    deploy_target='/secret/',
    # Name of the Kubernetes Secret
    secret='airflow-key',
    # Key of a secret stored in this Secret object
    key='key.json')

affinity = {
    'nodeAffinity': {
        # requiredDuringSchedulingIgnoredDuringExecution means in order
        # for a pod to be scheduled on a node, the node must have the
        # specified labels. However, if labels on a node change at
        # runtime such that the affinity rules on a pod are no longer
        # met, the pod will still continue to run on the node.
        'requiredDuringSchedulingIgnoredDuringExecution': {
            'nodeSelectorTerms': [{
                'matchExpressions': [{
                    # When nodepools are created in Google Kubernetes
                    # Engine, the nodes inside of that nodepool are
                    # automatically assigned the label
                    # 'cloud.google.com/gke-nodepool' with the value of
                    # the nodepool's name.
                    'key': 'cloud.google.com/gke-nodepool',
                    'operator': 'In',
                    # The label key's value that pods can be scheduled
                    # on.
                    'values': [
                        'pool-1',
                    ]
                }]
            }]
        }
    }
}

today = datetime.today()
CURRENT_DATE = today.strftime("%Y-%m-%d")
first = today.replace(day=1)
lastMonthEnd = first - timedelta(days=1)
lastMonthStart = lastMonthEnd.replace(day=1)
UNTIL = lastMonthEnd.strftime("%Y-%m-%d")
SINCE = lastMonthStart.strftime("%Y-%m-%d")
PROCESSING_DATE = first.strftime("%Y-%m-%d")
FILTER_DATE = '2016-01-01'


default_args = {
    "start_date": datetime(2019, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
    "exponential_retry_backoff": True,
    "depends_on_past": False,
    "priority_weight": 100,
    "secrets": [secret_vol],
    "env_vars": {
        'GOOGLE_APPLICATION_CREDENTIALS': '/secret/key.json',
        'PROJECT': 'brightfield-dev',
        'BUCKET': 'bfg-whoosh-index',
        'GCS_WHOOSH_INDEX_FOLDER_NAME': 'FDA'
    },
    "image_pull_policy": "Always",
    "execution_timeout": timedelta(hours=24),
    "startup_timeout_seconds": 300,
    "namespace": "default",
    "is_delete_operator_pod": True,
    "affinity": affinity,
}

dag = DAG(
    "fda_social_listening",
    default_args=default_args,
    description="social_listening_dag",
    schedule_interval=None,
    dagrun_timeout=timedelta(hours=96),
    catchup=False
)


extract_reddit_data = KubernetesPodOperator(
    task_id="extract_reddit_data",
    name="extract_reddit_data",
    cmds=["python3"],
    arguments=[
        "extractor.py",
        "--output_table=fda_social_raw.reddit_mention_raw",
        "--source=reddit",
        "--time_filter=month"
    ],
    image="gcr.io/brightfield-dev/fda-social-listening:initial-project",
    dag=dag,
)

extract_twitter_data = KubernetesPodOperator(
    task_id="extract_twitter_data",
    name="extract_twitter_data",
    cmds=["python3"],
    arguments=[
        "extractor.py",
        "--source=twitter",
        "--output_table=fda_social_raw.twitter_mention_raw",
        f"--since={SINCE}",
        f"--until={UNTIL}"
    ],
    image="gcr.io/brightfield-dev/fda-social-listening:initial-project",
    dag=dag,
)

extract_instagram_data = KubernetesPodOperator(
    task_id="extract_instagram_data",
    name="extract_instagram_data",
    cmds=["python3"],
    arguments=[
        "extractor.py",
        "--output_table=fda_social_raw.instagram_mention_raw",
        "--source=instagram",
    ],
    image="gcr.io/brightfield-dev/fda-social-listening:initial-project",
    dag=dag,
)

insert_reddit_data_query = """
MERGE fda_social_raw.social_data_raw AS Target
USING fda_social_raw.reddit_mention_raw AS Source
ON Source.id = Target.source_id
WHEN NOT MATCHED BY Target THEN
    INSERT (source_id,mention_title,mention_body,mention_type,created_datetime,username,mention_url,language,likes,
    dislikes,replies,shares,channel,data_source,processing_date) 
    VALUES (Source.id,Source.title, Source.body, Source.type,Source.created,Source.author, Source.url,'en',
    CAST(Source.score as int),null,CAST(Source.comms_num as int),null,Source.subreddit,'reddit',Source.processing_date);
"""
insert_reddit_data = BigQueryOperator(
    sql=insert_reddit_data_query,
    task_id="insert_reddit_data",
    dag=dag,
    use_legacy_sql=False
)
insert_twitter_data_query = """
MERGE fda_social_raw.social_data_raw AS Target
USING fda_social_raw.twitter_mention_raw AS Source
ON Source.id = Target.source_id
WHEN NOT MATCHED BY Target THEN
    INSERT (source_id,mention_title,mention_body,mention_type,created_datetime,username,mention_url,language,likes,
    dislikes,replies,shares,channel,data_source,processing_date) 
    VALUES (Source.id,null, Source.body, 
    CASE
    WHEN Source.retweet = true then 'retweet'
    else
    'tweet'
    end, 
    Source.date,Source.username, Source.link,'en',Source.nlikes,null,
    Source.nreplies,Source.nretweets,null,'twitter',Source.processing_date)
"""
insert_twitter_data = BigQueryOperator(
    sql=insert_twitter_data_query,
    task_id="insert_twitter_data",
    dag=dag,
    use_legacy_sql=False
)

insert_instagram_data_query = """
MERGE fda_social_raw.social_data_raw AS Target
USING fda_social_raw.instagram_mention_raw AS Source
ON Source.shortcode = Target.source_id
WHEN NOT MATCHED BY Target THEN
    INSERT (source_id,mention_title,mention_body,mention_type,created_datetime,username,mention_url,language,likes,
    dislikes,replies,shares,channel,data_source,processing_date) 
    VALUES (Source.shortcode,null, Source.body, 'post', Source.created,null, Source.url,'en',CAST(Source.likes as int),
    null,CAST(Source.comms_num as int),null,Source.hashtag, 'instagram',Source.processing_date);
"""
insert_instagram_data = BigQueryOperator(
    sql=insert_instagram_data_query,
    task_id="insert_instagram_data",
    dag=dag,
    use_legacy_sql=False
)

filter_query = f"""
MERGE fda_social_raw.social_data_processed AS Target
USING fda_social_raw.social_data_raw AS Source
ON Source.source_id = Target.source_id
WHEN NOT MATCHED BY Target AND Source.created_datetime >= '{FILTER_DATE}'
THEN
    INSERT (source_id,mention_title,mention_body,mention_type,created_datetime,username,mention_url,language,likes,
    dislikes,replies,shares,channel,data_source,processing_date) 
    VALUES (Source.source_id,Source.mention_title, Source.mention_body, Source.mention_type, Source.created_datetime,
    Source.username, Source.mention_url,Source.language,Source.likes,Source.dislikes,Source.replies,Source.shares,
    Source.channel, Source.data_source,Source.processing_date);
""".format(FILTER_DATE, FILTER_DATE)

filter_social_data = BigQueryOperator(
    sql=filter_query,
    task_id="filter_social_data",
    dag=dag,
    use_legacy_sql=False
)

sentiment_data_query = """SELECT * FROM `brightfield-dev.fda_social_raw.social_data_processed` 
                        where sentiments is null"""

analyze_sentiment = KubernetesPodOperator(
    task_id="analyze_sentiment",
    name="analyze_sentiment",
    cmds=["python3"],
    arguments=[
        "sentiment_analyzer.py",
        sentiment_data_query,
        "--output_table=fda_social_raw.social_data_raw_temp",
    ],
    image="gcr.io/brightfield-dev/fda-social-listening:initial-project",
    dag=dag,
)


merge_sentiment_query = """
MERGE fda_social_raw.social_data_processed AS Target
USING fda_social_raw.social_data_raw_temp AS Source
ON Source.source_id = Target.source_id		
WHEN MATCHED THEN UPDATE SET
    Target.sentiments = Source.sentiments,
    Target.sentiment_scores	= Source.sentiment_scores;
"""
merge_sentiment = BigQueryOperator(
    sql=merge_sentiment_query,
    task_id="merge_sentiment",
    dag=dag,
    use_legacy_sql=False
)

q = f"""
        SELECT * EXCEPT(source_id, created_datetime, mention_body, processing_date) , source_id as id, datetime(created_datetime) as created_datetime, datetime(processing_date) as whoosh_index_date, mention_body as text
        FROM `brightfield-dev.fda_social_raw.social_data_processed`
        WHERE DATE(processing_date) = "{CURRENT_DATE}"
    """


transforms = KubernetesPodOperator(
    task_id="transforms",
    name="transforms",
    cmds=["python3"],
    arguments=[
        "transforms.py",
        "--project=brightfield-dev",
        "--dataset=fda_social_reporting",
        f"--query={q}",
    ],
    image="gcr.io/brightfield-dev/fda-social-listening:initial-project",
    dag=dag,
)

extract_reddit_data >> insert_reddit_data
extract_twitter_data >> insert_twitter_data
extract_instagram_data >> insert_instagram_data

insert_reddit_data >> filter_social_data
insert_twitter_data >> filter_social_data
insert_instagram_data >> filter_social_data

filter_social_data >> analyze_sentiment >> merge_sentiment >> transforms