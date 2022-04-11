"""
Functions for Google Sheets
"""

from gspread_dataframe import set_with_dataframe
import gspread


class LaunchGS:
    """Class for Google Sheets client
    """

    def __init__(
            self,
            credentials,
            *args,
            **kwargs
    ):
        self.client = None
        self.credentials = credentials
        self.url = None
        self.workbook = gspread.authorize(self.credentials)
        self.sheet = None

    def open(
            self,
            url: str,
            sheet: str = None
    ):
        """Get or create a api service

        Args:
            url (str): URL of the Google Sheets to open.
            sheet (str): Sheet name to open. optional
        """
        if self.client is None:
            self.client = gspread.authorize(self.credentials)
        self.workbook = self.client.open_by_url(url)
        if sheet:
            self.select_sheet(sheet)
            return self.sheet
        else:
            return self.workbook

    def list_sheets(self):
        """Returns a list of sheet names"""
        return self.workbook.worksheets()

    def select_sheet(
            self,
            sheet_name: str
    ):
        self.sheet = self.workbook.worksheet(sheet_name)
        return self.sheet

    def get_sheet_id(self):
        return self.sheet._properties['sheetId']

    def get_data(self):
        """Returns a list of dictionaries, all of them having the contents of
                the spreadsheet with the head row as keys and each of these
                dictionaries holding the contents of subsequent rows of cells as
                values.
        """
        data = self.sheet.get_all_records()
        return data

    def overwrite_data(self, data, include_index: bool = False):
        self.save_data(data, mode='w', include_index=include_index)

    def save_data(
            self,
            df,
            mode: str = 'a',
            row: int = 1,
            include_index: bool = False
    ):
        """Saves a dataframe to the sheet"""
        if mode == 'w':
            self.sheet.clear()
            set_with_dataframe(
                self.sheet,
                df,
                include_index=include_index,
                include_column_header=True,
                row=row,
                resize=True
            )
        # elif mode == 'test':
        #     total_rows = self.sheet.row_count + 1
        #     first_empty_row = len(self.sheet.get_all_values()) + 1
        #     print(f"from row={first_empty_row}, total={total_rows}")
        #     self.sheet.add_rows(df.shape[0] + 1)
        #     print(f"adding {df.shape[0]} rows.")
        #     set_with_dataframe(
        #         self.sheet,
        #         df,
        #         include_index=False,
        #         include_column_header=False,
        #         row=first_empty_row,
        #         resize=False
        #     )
        else:
            data_list = df.values.tolist()
            self.sheet.append_rows(data_list)

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
