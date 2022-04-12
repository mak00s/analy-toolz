"""
Common Functions
"""

import pandas as pd


def change_column_type(df):
    for col in df.columns:
        if col in ['date', 'firstSessionDate']:
            df[col] = pd.to_datetime(df[col], infer_datetime_format=True, errors='coerce').dt.date
        if col in ['dateHour', 'dateHourMinute']:
            df[col] = pd.to_datetime(df[col], infer_datetime_format=True, errors='coerce')

    return df
