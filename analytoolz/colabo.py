"""
Functions for Google Colaboratory
"""

from IPython.display import clear_output

import os

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
    return path
