import json
import logging
import os
import pickle
from pathlib import Path

import pandas as pd
from google.auth import compute_engine
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


def get_or_create_google_credentials(scopes, credentials_file_path=None, cache_file_path='credentials.pickle',
                                     console=False, use_service_account=False):
    '''
    Creates a Credentials object from a downloaded credentials.json file downloaded from Google Cloud, or retrieves it from cache.
    :param scopes: Google API Scopes that are relevant
    :param credentials_file_path: Path to the credentials.json that was downloaded from Google Cloud. If set to `None`, will try to use `GOOGLE_APPLICATION_CREDENTIALS`.
    :param cache_file_path: Path to a cached credentials file. It will be created if it does not exist after using this function once. Defaults to `credentials.pickle`
    :param console: Sometimes the credentials.json will belong to a service account for easier sign in. Turn this to `True` if using one.
    :param use_service_account: In case `use_service_account` is `False`, `True` will prompt for an authentication flow using the console
                                - `False` will try to automatically get the url. Defaults to `False`
    :return: Credentials to be used in Google's API
    '''
    load_creds_from_cache = os.path.exists(cache_file_path) if cache_file_path else False
    cache_file_path = Path(cache_file_path) if cache_file_path else None

    if credentials_file_path is None:
        credentials_file_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'credentials.json')

    if use_service_account:
        credentials = service_account.Credentials.from_service_account_file(credentials_file_path)
        credentials = credentials.with_scopes(scopes)
    else:
        if load_creds_from_cache:
            with open(str(cache_file_path), 'rb') as pickled_credentials:
                credentials = pickle.load(pickled_credentials)
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file_path, scopes=scopes)
            if console:
                credentials = flow.run_console()
            else:
                credentials = flow.run_local_server()

            if cache_file_path:
                with open(str(cache_file_path), 'wb') as pickled_credentials:
                    pickle.dump(credentials, pickled_credentials)

    return credentials


class GSpreadSheet:
    """A wrapper for Google's Spreadsheet API.

    Data is received and returned as a Pandas Dataframe.
    
    Usage:
        spread_sheet = GSpreadSheet(credentials, spreadsheet_id)
        df = spread_sheet.get_sheet(sheet_name, sheet_range)
    If instead of giving a sheet id you give a spreadsheet title, it will be created.
    
    Methods:
        get_sheet:
            Returns a Pandas Dataframe with the sheet's data
        new_sheet:
            Create a new tab in spreadsheet and returns its id
        write_sheet:
            Writes a list of values to a sheet
        make_headers_bold:
            Makes the headers of a sheet bold

    """

    def __init__(self, spreadsheet_id=None, spreadsheet_title=None) -> None:
        credentials = self._get_credentials()
        service = build('sheets', 'v4', credentials=credentials)
        self.gsapi = service.spreadsheets()
        if spreadsheet_id is None:
            if spreadsheet_title is None:
                raise ValueError('Either spreadsheet_id or spreadsheet_title must be set')
            else:
                logging.info('No id provided creating new spreadsheet')
                spreadsheet_id = self._create_spreadsheet(spreadsheet_title)
        
        self.spreadsheet_id = spreadsheet_id

    def _create_spreadsheet(self, spreadsheet_title):
        """Creates a new spreadsheet and returns its id"""
        spreadsheet = {
            'properties': {
                'title': spreadsheet_title
            }
        }
        spreadsheet = self.gsapi.create(body=spreadsheet,
                                                    fields='spreadsheetId').execute()
        return spreadsheet.get('spreadsheetId')

    @staticmethod
    def _get_credentials():
        if os.environ["GOOGLE_APPLICATION_CREDENTIALS"].endswith(".json"):
            credentials = service_account.Credentials.from_service_account_file(
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
            )
        else:
            credentials = service_account.Credentials.from_service_account_info(
                json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
            )
        return credentials

    def get_sheet(self, sheet_name, sheet_range=None):
        """Returns a Pandas Dataframe with the sheet's data"""
        sheet_name_and_range = f'{sheet_name}!{sheet_range}' if sheet_range else sheet_name
        sheet = self.gsapi.values().get(spreadsheetId=self.spreadsheet_id,
                                        range=sheet_name_and_range).execute()
        values = sheet.get('values', [])
        return pd.DataFrame(values[1:], columns=values[0])

    def new_sheet(self, sheet_name):
        "Create a new tab in spreadsheet and returns its id"
        # Create new TAB
        body = {
        'requests': [{
            'addSheet': {
                'properties': {
                    'title': sheet_name,
                }
            }
        }]
        }

        result = self.gsapi.batchUpdate(
        spreadsheetId=self.spreadsheet_id,
        body=body).execute()
        return result['replies'][0]['addSheet']['properties']['sheetId']
    
    def write_sheet(self, sheet_name, range, values):
        """Writes a list of values to a sheet"""
        body = {
            'values': values
        }
        # Write values
        result = self.gsapi.values().update(
            spreadsheetId=self.spreadsheet_id, valueInputOption='RAW', range=f'{sheet_name}!{range}',  body=body).execute()
        logging.info('{0} cells updated.'.format(result.get('updatedCells')))
    
    def make_headers_bold(self, sheet_name):
        """Makes the headers of a sheet bold"""
        data = {'requests': [
        {'repeatCell': {
            'range': {'sheetId': sheet_name, 'endRowIndex': 1},
            'cell':  {'userEnteredFormat': {'textFormat': {'bold': True}}},
            'fields': 'userEnteredFormat.textFormat.bold',
        }}
        ]}

        self.gsapi.batchUpdate(
                spreadsheetId=self.spreadsheet_id, body=data).execute()
