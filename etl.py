import pandas as pd
import numpy as np
from pandas.core.frame import DataFrame
from pathlib import Path
from datetime import datetime


def extract_from_json(file_to_process):

    data = pd.read_json(file_to_process, typ="series")
    input_path = data['source']['path']+data['source']['dataset']+'.'+data['source']['format']
    transforms_df = pd.DataFrame(data['transforms'])
    output_path = data['sink']['path'] + data['sink']['dataset'] + '.' + data['sink']['format']

    return input_path, transforms_df, output_path


def add_age_column(fields_df: DataFrame, input_df: DataFrame):
    now = pd.Timestamp('now')
    dob = fields_df['fields'].head(1)[0][0].get('field')
    age = fields_df['fields'].head(1)[0][0].get('new_field')

    input_df[dob] = pd.to_datetime(input_df[dob])
    input_df[age] = input_df[dob].apply(lambda x: (now.year - x.year))

    return input_df


def add_binary_columns(fields_df: DataFrame, input_df: DataFrame):
    column_name = fields_df.values[1:2][0][0]
    values = input_df[column_name].unique()
    for val in values:
        new_col = 'is_' + val
        input_df[new_col] = np.where(input_df[column_name] == val, 1, 0)

    return input_df


def replace_nan_values(fields_df: DataFrame, input_df: DataFrame):
    fields_list = fields_df.values[2:][0]
    for ind in fields_list:
        input_df[ind['field']] = input_df[ind['field']].replace(np.nan, ind['value'], regex=True)

    return input_df


def load_data(targetfile, data_to_load):
    output_path = Path("~/basic-etl-pipeline/" + targetfile)
    data_to_load.to_json(output_path, orient='records', lines=True)


def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'

    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open("logfile.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')
