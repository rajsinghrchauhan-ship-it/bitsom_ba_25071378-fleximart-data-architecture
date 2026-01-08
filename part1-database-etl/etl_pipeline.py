# ==============================
# Imports
# ==============================

import pandas as pd
import phonenumbers
import mysql.connector
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import os


# ==============================
# Path Setup
# ==============================

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent

DATA_DIR = PROJECT_ROOT / "data"
ETL_DIR = PROJECT_ROOT / "part1-database-etl"


# ==============================
# Utility Functions
# ==============================

def read_raw_data(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    return pd.read_csv(file_path)


def find_treat_missing_val(df: pd.DataFrame, dataset_name="Dataset"):
    df = df.copy()

    record_count = len(df)
    duplicate_rows = df.duplicated().sum()

    # Column-wise NULL count (only where > 0)
    nulls_by_column = df.isna().sum()
    nulls_by_column = nulls_by_column[nulls_by_column > 0]

    if not nulls_by_column.empty:
        null_summary = ", ".join(
            f"{col}: {cnt}" for col, cnt in nulls_by_column.items()
        )
    else:
        null_summary = "None"

    df = df.drop_duplicates()
    df = df.ffill()

    insert_count = len(df)

    dq_reports = {
        "Dataset Name": dataset_name,
        "Record Count": record_count,
        "Duplicate Rows": duplicate_rows,
        "Null Summary": null_summary,
        "Insert Count": insert_count
    }

    return df, dq_reports


def clean_registration_date(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    date_cols = df.columns[df.columns.str.contains("date", case=False)]

    if len(date_cols) > 0:
        col = date_cols[0]

        df[col] = pd.to_datetime(
            df[col],
            format="mixed",
            errors="coerce",
            dayfirst=True
        ).dt.date

        invalid_count = df[col].isna().sum()
        if invalid_count > 0:
            print(f"⚠️ Found {invalid_count} invalid values in `{col}`")

    return df


def generate_data_quality_report_txt(reports, file_name="data_quality_report.txt"):
    report_path = ETL_DIR / file_name

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("DATA QUALITY REPORT\n")
        f.write("=" * 60 + "\n")
        f.write(f"Generated On: {datetime.now()}\n\n")

        for idx, row in enumerate(reports, start=1):
            f.write(f"Dataset #{idx}\n")
            f.write(f"Records Processed      : {row['Record Count']}\n")
            f.write(f"Duplicates Removed     : {row['Duplicate Rows']}\n")
            f.write(f"Missing Values Handled : {row['Null Summary']}\n")
            f.write(f"Records Loaded         : {row['Insert Count']}\n")
            f.write("-" * 60 + "\n")

    print(f"✅ Data quality report saved at {report_path}")


def upload_data_db(df: pd.DataFrame, table_name: str):
    load_dotenv()

    conn = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
        use_pure=True
    )

    cursor = conn.cursor()

    columns = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))
    update_clause = ", ".join([f"{col}=VALUES({col})" for col in df.columns])

    sql = (
        f"INSERT INTO {table_name} ({columns}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_clause}"
    )

    cursor.executemany(sql, df.values.tolist())
    conn.commit()

    cursor.close()
    conn.close()

    print(f"✅ Loaded / Updated {len(df)} records into `{table_name}`")


# ==============================
# MAIN ETL PIPELINE
# ==============================

def main():
    print("🚀 ETL pipeline started")

    # Extract
    cust_df = read_raw_data(DATA_DIR / "customers_raw.csv")

    # Transform + Data Quality
    dq_reports = []

    cust_df_clean, report = find_treat_missing_val(
    cust_df,
    dataset_name="Customers"
    )
    dq_reports.append(report)

    # Phone formatting
    cust_df_clean["phone"] = cust_df_clean["phone"].apply(
        lambda x: phonenumbers.format_number(
            phonenumbers.parse(str(x), region="IN"),
            phonenumbers.PhoneNumberFormat.E164
        ) if pd.notna(x) else None
    )

    # Customer ID formatting
    cust_df_clean["customer_id"] = (
        cust_df_clean["customer_id"]
        .astype(str)
        .str.replace(r"\D+", "", regex=True)
    )

    # Clean mixed registration_date
    cust_df_clean = clean_registration_date(cust_df_clean)

    # Remove duplicates on primary key
    cust_df_clean = cust_df_clean.drop_duplicates(
        subset="customer_id",
        keep="first"
    )

    # Load
    upload_data_db(cust_df_clean, "customers")

    # Report
    generate_data_quality_report_txt(dq_reports)

    print("✅ ETL pipeline completed successfully")


# ==============================
# Entry Point
# ==============================

if __name__ == "__main__":
    main()
