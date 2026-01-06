# etl_pipeline.py

import logging
import pandas as pd
import numpy as np

# -------------------------------------------------------------------
# LOGGING CONFIGURATION
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -------------------------------------------------------------------
# DATA QUALITY FUNCTIONS
# -------------------------------------------------------------------

def find_treat_missing_val(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values in numeric and categorical columns.
    """
    df = df.copy()

    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    categorical_cols = df.select_dtypes(include=["object", "category", "bool"]).columns

    for col in numeric_cols:
        if df[col].isnull().any():
            df[col].fillna(df[col].median(), inplace=True)

    for col in categorical_cols:
        if df[col].isnull().any():
            df[col].fillna(df[col].mode()[0], inplace=True)

    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows from dataframe.
    """
    return df.drop_duplicates()


# -------------------------------------------------------------------
# EXTRACT
# -------------------------------------------------------------------

def extract_data(file_path: str) -> pd.DataFrame:
    logging.info("Extracting data")
    return pd.read_csv(file_path)


# -------------------------------------------------------------------
# TRANSFORM
# -------------------------------------------------------------------

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transforming data")

    df = find_treat_missing_val(df)
    df = remove_duplicates(df)

    return df


# -------------------------------------------------------------------
# LOAD
# -------------------------------------------------------------------

def load_data(df: pd.DataFrame, output_path: str):
    logging.info("Loading data")
    df.to_csv(output_path, index=False)


# -------------------------------------------------------------------
# MAIN PIPELINE
# -------------------------------------------------------------------

def main():
    logging.info("ETL pipeline started")

    input_file = "input_data.csv"
    output_file = "processed_data.csv"

    df = extract_data(input_file)
    df = transform_data(df)
    load_data(df, output_file)

    logging.info("ETL pipeline completed successfully")


if __name__ == "__main__":
    main()
