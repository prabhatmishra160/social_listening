import datetime
import functools
import logging
import sys
import time

import dateutil
import numpy as np
import pandas_gbq
import simplejson
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)


class _BFGEncoder(simplejson.JSONEncoder):
    """
    Provide custom serialization for datetime objects.
    """
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            if obj.tzinfo is None:
                obj = obj.replace(tzinfo=dateutil.tz.tzutc())
            return obj.isoformat()
        elif isinstance(obj, (np.int0, np.int8, np.int16, np.int32, np.int64,
                              np.uint0, np.uint8, np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64, np.float128)):
            return float(obj)
        return super().default(obj)


class BigQueryError(Exception):
    pass


class _BQ:
    STREAM_ROWS_LIMIT = 10000
    STREAM_SIZE_LIMIT = 10485760  # In bytes
    STREAM_SIZE_THRESHOLD = STREAM_SIZE_LIMIT * 0.80
    STREAM_RETRY_DELAY = 60
    IF_EXISTS_ALLOWED_VALUES = [
        'fail',
        'replace',
        'append',
    ]

    ALLOW_TABLE_DELETION = False

    def __init__(self):
        self.credentials_path = None
        self.project_id = None

    def conf(self, credentials_path, project_id):
        self.project_id = project_id
        self.credentials_path = credentials_path

    @property
    @functools.lru_cache()
    def client(self):
        if self.credentials_path is None or self.project_id is None:
            raise BigQueryError('Not configured')
        return bigquery.Client.from_service_account_json(self.credentials_path)

    @property
    @functools.lru_cache()
    def credentials(self):
        return self.client._credentials

    def stream(self, table, rows, step=None, get_id_function=None, retry=3, json_encoder_class=_BFGEncoder):
        """
        Stream data into a table in chuks taking into account streaming
        limitations and providing basic error handling.

        Parameters:
        table: google.cloud.bigquery.table.Table instance to stream data to
        rows: rows to stream
        step: set chunks sizes. Will lower automatically in case of exceeded
              BigQuery streaming limitations
        get_id_function: function to apply to each row to generate insertID
                         parameter.
                         NOTE: Does not appear to play well with a lot of types.
                               Best/safest bet is to use str results
        retry: number of attempts in case of HTTP 500 responses
        """
        if not rows:
            return

        step = int(step or self.STREAM_ROWS_LIMIT)
        if step == 0:
            raise BigQueryError('Row size is too big')

        for chunk_index in range(0, len(rows), step):
            chunk = rows[chunk_index: chunk_index + step]
            chunk_json = simplejson.dumps(chunk, ignore_nan=True, cls=json_encoder_class)
            chunk_size = sys.getsizeof(chunk_json)
            logger.debug('Streaming %s chunk %s', len(chunk), chunk_size)
            if chunk_size > self.STREAM_SIZE_THRESHOLD:
                self.stream(table, chunk, step / 2, get_id_function, retry)
            else:
                # Use simplejson to get rid of NaN and numpy ints and floats
                chunk = simplejson.loads(chunk_json)
                try:
                    row_ids = get_id_function and list(map(get_id_function, chunk))
                except Exception:
                    logger.exception('Can not generate row ids')
                    raise BigQueryError('Can not generate row ids')

                errors = self.client.insert_rows(table, chunk, row_ids=row_ids)
                if errors:
                    do_retry = any([
                        'backendError' in str(errors),
                        'internalError' in str(errors),
                        'timeout' in str(errors),
                    ])
                    if retry and do_retry:
                        logger.warning('Failed to stream rows (retries left %s). Delaying and retrying!', retry)
                        time.sleep(self.STREAM_RETRY_DELAY)
                        self.stream(table, chunk, step, get_id_function, retry - 1)
                    else:
                        raise BigQueryError('Error saving {} ({})'.format(table, errors))

    def read_sql(self, sql):
        logger.debug('Reading %s', sql)
        return pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credentials)

    def write_df_to_table(self, df, dataset, table_name, if_exists, table_schema=None):
        logger.debug('Writting(%s) %s to %s.%s', if_exists, df.shape, dataset, table_name)
        if if_exists not in self.IF_EXISTS_ALLOWED_VALUES:
            raise Exception('if_exists {} not allowed'.format(if_exists))

        table = '{}.{}'.format(dataset, table_name)
        pandas_gbq.to_gbq(df, table,
                          project_id=self.project_id,
                          if_exists=if_exists,
                          table_schema=table_schema,
                          credentials=self.credentials)

    def create_table(self, dataset, table_name, schema=None, partition_by=None):
        logger.info('Creating table %s.%s', dataset, table_name)
        dataset = self.client.dataset(dataset)
        table_ref = dataset.table(table_name)
        table = bigquery.Table(table_ref, schema=schema)
        if partition_by:
            logger.info("Creating partitioned table")
            table.time_partitioning = bigquery.table.TimePartitioning(type_='DAY', field=partition_by)
        table = self.client.create_table(table)

        return table

    def get_table(self, dataset, table_name):
        logger.info('Getting table %s.%s', dataset, table_name)
        dataset = self.client.dataset(dataset)
        table_ref = dataset.table(table_name)

        try:
            table = self.client.get_table(table_ref)
            return table
        except NotFound:
            raise BigQueryError("Couldn't find table", dataset, table_name)

    def get_or_create_table(self, dataset, table_name, schema=None, partition_by=None):
        try:
            table = self.get_table(dataset, table_name)
        except BigQueryError as e:
            logger.info(e)
            table = self.create_table(dataset, table_name, schema, partition_by)

        return table

    def get_schema(self, schema):
        """builds a BQ schema from a json schema"""
        return [
            bigquery.SchemaField(field["name"], field["type"])
            for field in schema
        ]

    def delete_table(self, dataset, table_name):
        logger.debug('Deleting table %s.%s', dataset, table_name)
        if not self.ALLOW_TABLE_DELETION:
            raise BigQueryError('Deleting tables is not enabled!')

        dataset = self.client.dataset(dataset)
        table_ref = dataset.table(table_name)
        table = bigquery.Table(table_ref)
        self.client.delete_table(table)

    def create_table_from_model(self, model):
        self.create_table(model.DATASET_ID, model.TABLE_ID, model.SCHEMA)

    def delete_table_from_model(self, model):
        self.delete_table(model.DATASET_ID, model.TABLE_ID)

    def recreate_table_from_model(self, model):
        try:
            self.delete_table_from_model(model)
        except Exception:
            logger.exception('Could not delete table %s. Continuing!', model.table_id())
        self.create_table_from_model(model)

    def job(self, query, destination_dataset, destination_table, if_exists):
        """
        Execute query into a destination table
        """
        job_config = bigquery.QueryJobConfig()

        write_disposition = {
            'replace': bigquery.WriteDisposition.WRITE_TRUNCATE,
            'append': bigquery.WriteDisposition.WRITE_APPEND,
            'fail': bigquery.WriteDisposition.WRITE_EMPTY,
        }[if_exists]

        job_config.destination = self.client.dataset(destination_dataset).table(destination_table)
        job_config.write_disposition = write_disposition

        query_job = BQ.client.query(query, job_config=job_config)
        result = query_job.result()

        return result

    def remove_duplication_from_table(self, dataset, table, subset=None):
        """
        Remove duplicated rows in a table based on the columns provided in the subset list.
        If subset is not provided, use all table columns.

        Subset can not include floating point, struct or array columns.
        """
        table_id = '{}.{}.{}'.format(BQ.client.project, dataset, table)
        over_arguments = 'PARTITION BY {0}'.format(','.join(subset)) if subset else ''
        query = ' '.join([
            'SELECT',
            '   *',
            'EXCEPT (row_number)',
            'FROM (',
            '   SELECT',
            '       *,',
            '       ROW_NUMBER() OVER ({over_arguments}) row_number',
            '   FROM `{table_id}`',
            ')',
            'WHERE',
            '   row_number = 1',
        ]).format(
            over_arguments=over_arguments,
            table_id=table_id,
        )

        return self.job(query, dataset, table, if_exists='replace')


BQ = _BQ()


class BigQueryModel:
    DISABLE_DROP_DUPLICATES = False

    @classmethod
    @functools.lru_cache()
    def table(cls):
        return BQ.client.get_table(cls.table_id())

    @classmethod
    @functools.lru_cache()
    def table_id(cls):
        return '{}.{}.{}'.format(BQ.client.project, cls.DATASET_ID, cls.TABLE_ID)

    @classmethod
    def drop_duplicates(cls):
        """
        Drop duplicated rows from table.
        Duplicated rows are identified by the list of columns in cls.IDENTIFIERS or
        all columns if cls.IDENTIFIERS is not defined.
        """
        if cls.DISABLE_DROP_DUPLICATES:
            logger.warning('Dropping duplicates is disabled for %s', cls)
        else:
            subset = getattr(cls, 'IDENTIFIERS', [column.name for column in cls.SCHEMA])
            BQ.remove_duplication_from_table(cls.DATASET_ID, cls.TABLE_ID, subset=subset)
