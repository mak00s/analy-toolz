"""
Functions for Google Colaboratory
"""

import os
from IPython.display import clear_output

from google.colab import drive


def mount_gdrive(path="/content/drive"):
    """ Mount Google Drive
    """
    if not os.path.isdir(path):
        try:
            print("Googleドライブがマウントされていないので接続します。")
            drive.mount(path)
            clear_output()
        except Exception as e:
            raise e
