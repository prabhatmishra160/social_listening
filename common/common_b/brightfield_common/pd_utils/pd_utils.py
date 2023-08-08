import os
import pandas as pd
import gspread
from googleapiclient.discovery import build


def google_sheet_to_df(url, credentials, sheet_name=""):
    '''
    Converts a google sheet doc to a data frame.
    :param url: Url of document. Example: https://docs.google.com/spreadsheets/d/1GR8Nf79h202M5UDWIRXlbsi786kGF7GUtIlHLfaByIQ/edit#gid=1818936388
    :param credentials: A credentials object, probably created by `get_get_or_create_google_credentials`
    :param sheet_name: The name of the sheet
    :return: A dataframe that is loaded from Google Sheets
    '''
    # The ID and range of a sample spreadsheet.
    # EX https://docs.google.com/spreadsheets/d/1GR8Nf79h202M5UDWIRXlbsi786kGF7GUtIlHLfaByIQ/edit#gid=1818936388&range=A:D
    sheet_id = url.split('/d/')[1].split('/edit')[0]
    sheet_name_and_range = sheet_name + "!" + url.split('&range=')[1]

    service = build('sheets', 'v4', credentials=credentials)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=sheet_id, range=sheet_name_and_range).execute()
    values = result.get('values', [])

    return pd.DataFrame(values, columns=values[0])[1:]


def df_to_google_sheets(df, url, credentials, sheet_name="", clear=True):
    def iter_pd(df):
        for val in df.columns:
            yield val
        for row in df.to_numpy():
            for val in row:
                if pd.isna(val):
                    yield ""
                else:
                    yield val

    sheet_id = url.split('/d/')[1].split('/edit')[0]

    gc = gspread.authorize(credentials)
    
    # Get sheet
    sheet = gc.open_by_key(sheet_id).sheet1
    # Updates all values in a workbook to match a pandas dataframe
    if clear:
        sheet.clear()
    
    row, col = df.shape
    cells = sheet.range("A1:{}".format(gspread.utils.rowcol_to_a1(row + 1, col)))
    for cell, val in zip(cells, iter_pd(df)):
        cell.value = val
    sheet.update_cells(cells)
