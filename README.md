# ETL-pipeline

Python
Implement a software that can read a dataset from a specific file path, apply some transformations to its fields and write the result into another path.

Requirements:
- Datasets can be read and written in CSV and at least another format: Parquet or JSONL (JSON Lines).
- The program has a library of transformations that can potentially be applied to the dataset, but on each execution we select which ones we want to apply to which columns.
- Anyone without programming skills should be able to execute the script. To do that, the user will provide a JSON configuration file. Your solution has to parse this configuration and generate the result based on it.
- The transformations that you need to implement are:
- birthdate_to_age: Given a date, it computes the age of a person and
creates a new column with the result.
- hot_encoding: Given a categorical column with n possible values, it
replaces it with n binary columns. For example, given the column “color” with 3 possible values: “blue”, “red” and “green”, we will create 3 columns: “is_blue”, “is_red” and “is_green”, that will be 1 or 0 depending of the value of the original column.
- fill_empty_values: It replaces the empty values of a column with another value. The value to be replaced with is passed as a parameter and it can be a constant or one of the following keywords: “mean”, “median” or “mode”.
If we pass one of these keywords, we replace the empty values with the mean / median / mode of the rest of the elements of the column.
Example: To test your code, we provide you a small dataset called bookings.csv and the json config file (clean_bookings.json) that should be passed to your program as an argument to process it and generate a clean one.
