from datetime import datetime, timedelta
from configuration import *
import logging.config
from etl import ETL
from transforms import tagging_jobs, BRIDGE_TABLE_DEFINITIONS, MASTER_TABLE_DEFINITION


# logging.config.fileConfig(fname='logging.conf', disable_existing_loggers=False)
# logger = logging.getLogger(__name__)


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def main(project, dataset, query):
    etl = ETL(target_bq_dataset=dataset,
              query=query,
              project_id=project,
              bridge_tables=BRIDGE_TABLE_DEFINITIONS,
              master_tables=MASTER_TABLE_DEFINITION,
              tagging_jobs=tagging_jobs,
              )

    etl.run()


START = '2022-07-01'
END = '2022-07-01'

# START = '2022-05-05'
# END = '2022-05-08'

start_date = datetime.strptime(START, "%Y-%m-%d").date()
end_date = datetime.strptime(END, "%Y-%m-%d").date()
date_range = list(daterange(start_date, end_date))

for _d in date_range:
    processing_date = _d.strftime('%Y-%m-%d')
    logging.info(f"Processing date: {processing_date}")

    q = f"""
        SELECT * EXCEPT(source_id, created_datetime, mention_body, processing_date) , source_id as id, datetime(created_datetime) as created_datetime, datetime(processing_date) as whoosh_index_date, mention_body as text
        FROM `brightfield-dev.fda_social_raw.social_data_processed`
        WHERE DATE(processing_date) = "{processing_date}"
    """
    print(settings.PROJECT)
    main(settings.PROJECT, "fda_social_reporting", q)
