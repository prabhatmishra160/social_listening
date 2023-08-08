# Company-wide common utilities

## Installation
Installing is currently done directly through GitHub:
`python3 -m pip install git+https://github.com/brightfieldgroup/common@master#egg=brightfield_common`

## BigQuery Utilities (`bq_utils` module)

### `query_into_destination_table`
Executes a query in BigQuery and saves the result in another table.

#### Arguments
* `client`: Google API client
* `sql`: SQL text to be executed
* `destination_dataset`: Destination dataset where the query is going to get saved into
* `destination_table`: Destination table where the query is going to get saved into
* `encoding`: Encoding for text ('UTF-8' or 'ISO-8859-1'). Defaults to 'UTF-8'.
* `write_disposition`: Describes what to do if a table already exists at the destination. Defaults to `bigquery.WriteDisposition.WRITE_TRUNCATE`
* `location`: Location of the dataset. Defaults to `US`

#### Return Value
* Job object from Google API. Can call `.result()` on it to wait for it to finish

### `load_file_into_table`
Loads a local file into a BigQuery table.

#### Arguments
* `client`: Google API client
* `filename`: The file path to the file that is going to be uploaded to BigQuery
* `dataset_id`: Destination dataset to load the file into
* `table_id`: Destination table to load the file into
* `source_format`: File source format. Defaults to `bigquery.SourceFormat.CSV`
* `encoding`: Encoding for text ('UTF-8' or 'ISO-8859-1'). Defaults to 'UTF-8'.
* `autodetect`: Auto detect columns. Defaults to `True`. If this is `False`, `schema` must be provided
* `schema`: Array of SchemaFields objects that define the table's schema. Only relevant if `autodetect` is `False`. See https://cloud.google.com/bigquery/docs/tables for more information.
* `write_disposition`: Describes what to do if a table already exists at the destination. Defaults to `bigquery.WriteDisposition.WRITE_TRUNCATE`

#### Return Value
* Job object from Google API. Can call `.result()` on it to wait for it to finish

## Google Utilities (`google_utils` module)

### `get_or_create_google_credentials`
Creates a Credentials object from a downloaded credentials.json file downloaded from Google Cloud, or retrieves it from cache.

#### Arguments
* `scopes` - Google API Scopes that are relevant
* `credentials_file_path` - Path to the credentials.json that was downloaded from Google Cloud. Defaults to `GOOGLE_APPLICATION_CREDENTIALS` environment variable, otherwise if one does not exist, `credentials.json` 
* `cache_file_path` - Path to a cached credentials file. It will be created if it does not exist after using this function once. Defaults to `credentials.pickle`
* `use_service_account` - Sometimes the credentials.json will belong to a service account for easier sign in. Turn this to true if using one.
* `console` - In case `use_service_account` is `False`, `True` will prompt for an authentication flow using the console - `False` will try to automatically get the url. Defaults to `False`

#### Return Value
* Credentials to be used in Google's API


## Pandas Utilities (`pd_utils` module)

### `google_sheet_to_df`
Converts a google sheet doc to a data frame.

#### Arguments
* `url` - Url of document including range arguments. Example: https://docs.google.com/spreadsheets/d/1GR8Nf79h202M5UDWIRXlbsi786kGF7GUtIlHLfaByIQ/edit#gid=1818936388&range=A:D
* `credentials` - a Credentials object
* `sheet_name` - The name of the sheet

#### Return Value
* A dataframe that is loaded from Google Sheets
