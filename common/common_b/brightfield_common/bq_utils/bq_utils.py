from google.cloud import bigquery


def query_into_destination_table(client, sql, destination_dataset, destination_table, encoding=bigquery.Encoding.UTF_8,
                                 write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, location='US'):
    '''
    Executes a query in BigQuery and saves the result in another table.
    :param client: Google API client
    :param sql: SQL text to be executed
    :param destination_dataset: Destination dataset where the query is going to get saved into
    :param destination_table: Destination table where the query is going to get saved into
    :param encoding: 'UTF-8' or 'ISO-8859-1'. Defaults to 'UTF-8'
    :param write_disposition: Describes what to do if a table already exists at the destination. Defaults to `bigquery.WriteDisposition.WRITE_TRUNCATE`
    :param location: Location of the dataset. Defaults to `US`
    :return: Job object from Google API. Can call `.result()` on it to wait for it to finish
    '''
    job_config = bigquery.QueryJobConfig()

    destination_table_ref = client.dataset(destination_dataset).table(destination_table)
    job_config.destination = destination_table_ref
    job_config.write_disposition = write_disposition
    job_config.encoding = encoding

    query_job = client.query(sql, location=location, job_config=job_config)
    return query_job


def load_file_into_table(client, filename, dataset_id, table_id, source_format=bigquery.SourceFormat.CSV,
                         encoding=bigquery.Encoding.UTF_8, autodetect=True, schema=None,
                         write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE):
    '''
    Loads a local file into a BigQuery table.
    :param client: Google API client
    :param filename: The file path to the file that is going to be uploaded to BigQuery
    :param dataset_id: Destination dataset to load the file into
    :param table_id: Destination table to load the file into
    :param source_format: File source format. Defaults to `bigquery.SourceFormat.CSV`
    :param autodetect: Auto detect columns. Defaults to `True`. If this is `False`, `schema` must be provided
    :param encoding: 'UTF-8' or 'ISO-8859-1'. Defaults to 'UTF-8'
    :param schema: Array of SchemaFields objects that define the table's schema. Only relevant if `autodetect` is `False`.
                   See https://cloud.google.com/bigquery/docs/tables for more information.
    :param write_disposition: Describes what to do if a table already exists at the destination. Defaults to `bigquery.WriteDisposition.WRITE_TRUNCATE`
    :return: Job object from Google API. Can call `.result()` on it to wait for it to finish
    '''
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = source_format
    job_config.write_disposition = write_disposition
    job_config.skip_leading_rows = 1
    job_config.encoding = encoding

    if autodetect:
        job_config.autodetect = True
    else:
        assert schema is not None
        job_config.schema = schema

    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    return job
