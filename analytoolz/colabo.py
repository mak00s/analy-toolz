"""
Functions for Google Colaboratory
"""

from IPython.display import clear_output
import os

from google.colab import data_table
from google.colab import drive
from google.colab import files


def init():
    data_table.enable_dataframe_formatter()
    data_table._DEFAULT_FORMATTERS[float] = lambda x: f"{x:.2f}"


def download(file):
    files.download(file)


def mount_gdrive(path: str = "/content/drive"):
    """ Mount Google Drive
    """
    if not os.path.isdir(path):
        try:
            print("Googleドライブがマウントされていないので接続します。")
            drive.mount(path)
            clear_output()
        except Exception as e:
            raise e
    return path


def table(df, rows: int = 10):
    return data_table.DataTable(
        df,
        include_index=False,
        num_rows_per_page=10
    )
