from etl import *


file_path = "~/basic-etl-pipeline/inputs/clean_bookings.json"


def run_pipeline(file_path: str):
    log("ETL Job Started")
    log("Extract phase Started")
    data = extract_from_json(file_path)
    input_df = pd.read_csv(data[0])
    log("Extract phase Ended")
    log("Transform phase Started")

    transforms_df = data[1]
    for x in transforms_df['transform']:
        if x == 'birthdate_to_age':
            input_df = add_age_column(pd.DataFrame(transforms_df['fields']), input_df)
        if x == 'hot_encoding':
            input_df = add_binary_columns(transforms_df['fields'], input_df)
        if x == 'fill_empty_values':
            input_df = replace_nan_values(transforms_df['fields'], input_df)
    log("Transform phase Ended")
    log("Load phase Started")
    load_data(data[-1], input_df)
    log("Load phase Ended")
    log("ETL Job Ended Successfully")


print(__name__)

if __name__ == "__main__":

    run_pipeline(file_path=file_path)
