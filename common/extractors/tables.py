import os
import pandas_gbq
import pandas as pd
import json
from google.oauth2 import service_account

PROJECT_ID = 'brightfield-dev'

def get_credentials():
    if os.environ["GOOGLE_APPLICATION_CREDENTIALS"].endswith(".json"):
        credentials = service_account.Credentials.from_service_account_file(
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        )
    else:
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
        )
    return credentials


def save_data_to_big_query(topic_data, output_table, if_exist="replace"):
    """
    function is responsible fot saving the final dataframe into bigquery table
    Args:
        :param1 topic_data: dataframe containing data needed to be saved in bigquery
        :param2 output_table: big query table in which data will e pushed
        :param3 if_exist: whether the data should be appended or replaced
    """
    pandas_gbq.to_gbq(topic_data, output_table, project_id=PROJECT_ID, credentials=get_credentials(),
                      if_exists=if_exist)


def read_data_from_big_query(sql):
    """
    function is responsible for reading data from big query table.
    Args:
        :param1 sql: select sql statement for reading data from big query table
    Returns:
        it returns the dataframe containing the data
    """
    sql = sql
    df = pandas_gbq.read_gbq(
        sql,
        project_id=PROJECT_ID,
        credentials=get_credentials(),
    )
    return df

def load_query_keywords(source, keywords_sheet_path):
    """
    function is responsible for loading the keyword and hashtag file
    Args:
        :param1 source:source could be either reddit / twitter / instagram
    Returns:
        keywords or hashtags list
    """
    keyword_df = pd.read_csv(keywords_sheet_path)
    if source == "instagram":
        print("condition is true")
        keys = keyword_df[keyword_df['type'] == 'hashtags'].dropna()
        keys = keys['keywords'].drop_duplicates()
    else:
        keys = keyword_df[keyword_df['type'] != 'accounts'].dropna()
        keys = keys['keywords'].drop_duplicates()
    return keys