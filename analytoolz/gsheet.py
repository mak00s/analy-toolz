"""
Functions for Google Sheets
"""

import pandas as pd

from gspread_dataframe import set_with_dataframe
import gspread


class LaunchGS:
    """Class for Google Sheets client
    """

    def __init__(
            self,
            credentials
    ):
        self.client = None
        self.credentials = credentials
        self.url = None
        self.workbook = None
        self.sheet = None
        self.authorize()

    def authorize(self):
        self.client = gspread.authorize(self.credentials)

    def open(
            self,
            url: str,
            sheet: str = None
    ):
        """Get or create an api service

        Args:
            url (str): URL of the Google Sheets to open.
            sheet (str): Sheet name to open. optional
        """
        if not self.client:
            self.authorize()
        self.workbook = self.client.open_by_url(url)
        try:
            print(f"「{self.workbook.title}」を開きました。")
        except gspread.exceptions.APIError as e:
            ej = e.response.json()['error']
            if ej['status'] == 'PERMISSION_DENIED':
                if 'The caller does not have permission' in ej['message']:
                    print("該当スプレッドシートを読み込む権限がありません。")
                elif 'disabled' in ej['message']:
                    print("Google SheetsのAPIが有効化されていません。")
                print(ej['message'])
        if sheet:
            self.select_sheet(sheet)
            return self.sheet
        else:
            return self.workbook

    def list_sheets(self):
        """Returns a list of sheet names"""
        if self.workbook:
            return self.workbook.worksheets()

    def select_sheet(
            self,
            sheet_name: str
    ):
        try:
            self.sheet = self.workbook.worksheet(sheet_name)
            return self.sheet
        except gspread.exceptions.WorksheetNotFound as e:
            print(f"{sheet_name} シートが存在しません。")
        except gspread.exceptions.APIError as e:
            ej = e.response.json()['error']
            if ej['status'] == 'PERMISSION_DENIED':
                if 'The caller does not have permission' in ej['message']:
                    print("該当スプレッドシートを読み込む権限がありません。")
                elif 'disabled' in ej['message']:
                    print("Google SheetsのAPIが有効化されていません。")
                print(ej['message'])

    def get_sheet_id(self):
        return self.sheet._properties['sheetId']

    def get_data(self):
        """Returns a list of dictionaries, all of them having the contents of
                the spreadsheet with the head row as keys and each of these
                dictionaries holding the contents of subsequent rows of cells as
                values.
        """
        if not self.sheet:
            print("Please select a sheet first.")
            return
        data = self.sheet.get_all_records()
        return data

    def overwrite_data(self, df: pd.DataFrame, include_index: bool = False):
        return self.save_data(df, mode='w', include_index=include_index)

    def save_data(
            self,
            df: pd.DataFrame,
            mode: str = 'a',
            row: int = 1,
            include_index: bool = False
    ):
        """Saves a dataframe to the sheet"""
        if not len(df):
            print("no data to write.")
            return
        elif not self.sheet:
            print("Please select a sheet first.")
            return
        elif mode == 'w':
            try:
                self.sheet.clear()
            except gspread.exceptions.APIError as e:
                ej = e.response.json()['error']
                if ej['status'] == 'PERMISSION_DENIED':
                    if 'The caller does not have permission' in ej['message']:
                        print("該当スプレッドシートを編集する権限がありません。")
                    elif 'disabled' in ej['message']:
                        print("Google SheetsのAPIが有効化されていません。")
                    print(ej['message'])

            set_with_dataframe(
                self.sheet,
                df,
                include_index=include_index,
                include_column_header=True,
                row=row,
                resize=True
            )
            return df
        else:
            data_list = df.values.tolist()
            self.sheet.append_rows(data_list)
            return df

    def auto_resize(
            self,
            cols: list
    ):
        """Auto resize columns to fit text"""
        sheet_id = self.get_sheet_id()
        _requests = []
        for i in cols:
            dim = {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": i - 1,
                        "endIndex": i
                    }
                }
            }
            _requests.append(dim)
        self.workbook.batch_update({'requests': _requests})

    def resize(
            self,
            col: int,
            width: int
    ):
        """Auto resize columns to fit text"""
        sheet_id = self.get_sheet_id()
        _requests = []
        for i in [col]:
            dim = {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": i - 1,
                        "endIndex": i
                    },
                    "properties": {
                        "pixelSize": width
                    },
                    "fields": "pixelSize"
                }
            }
            _requests.append(dim)
        self.workbook.batch_update({'requests': _requests})

    def freeze(self, rows: int = None, cols: int = None):
        """Freeze rows and/or columns"""
        self.sheet.freeze(rows=rows, cols=cols)
