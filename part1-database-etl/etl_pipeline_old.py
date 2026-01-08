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

    nulls_by_column = df.isna().sum()
    nulls_by_column = nulls_by_column[nulls_by_column > 0]

    null_summary = (
        ", ".join(f"{col}: {cnt}" for col, cnt in nulls_by_column.items())
        if not nulls_by_column.empty else "None"
    )

    df = df.drop_duplicates()
    df = df.ffill()

    insert_count = len(df)

    return df, {
        "Dataset Name": dataset_name,
        "Record Count": record_count,
        "Duplicate Rows": duplicate_rows,
        "Null Summary": null_summary,
        "Insert Count": insert_count
    }


def clean_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts ALL date columns to Python date objects
    Handles mixed formats like DD/MM/YYYY, YYYY-MM-DD
    """
    df = df.copy()

    date_cols = df.columns[df.columns.str.contains("date", case=False)]

    for col in date_cols:
        df[col] = pd.to_datetime(
            df[col],
            format="mixed",
            errors="coerce",
            dayfirst=True
        ).dt.date

        bad = df[col].isna().sum()
        if bad > 0:
            print(f"{bad} invalid values in `{col}`")

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

    print(f"Data quality report saved at {report_path}")


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
    update_clause = ", ".join(f"{c}=VALUES({c})" for c in df.columns)

    sql = (
        f"INSERT INTO {table_name} ({columns}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_clause}"
    )

    cursor.executemany(sql, df.values.tolist())
    conn.commit()

    cursor.close()
    conn.close()

    print(f"Loaded {len(df)} rows into `{table_name}`")


# ==============================
# MAIN ETL
# ==============================

def main():
    print("ETL pipeline started")
    dq_reports = []

    # -------- CUSTOMERS --------
    cust_df = read_raw_data(DATA_DIR / "customers_raw.csv")
    cust_df, report = find_treat_missing_val(cust_df, "Customers")

    cust_df["phone"] = cust_df["phone"].apply(
        lambda x: phonenumbers.format_number(
            phonenumbers.parse(str(x), "IN"),
            phonenumbers.PhoneNumberFormat.E164
        ) if pd.notna(x) else None
    )

    cust_df["customer_id"] = cust_df["customer_id"].astype(str).str.replace(r"\D+", "", regex=True)
    cust_df = clean_date_columns(cust_df)

    upload_data_db(cust_df, "customers")
    dq_reports.append(report)

    # -------- PRODUCTS --------
    prod_df = read_raw_data(DATA_DIR / "products_raw.csv")
    prod_df, report = find_treat_missing_val(prod_df, "Products")

    prod_df["product_id"] = prod_df["product_id"].astype(str).str.replace(r"\D+", "", regex=True)
    prod_df["category"] = prod_df["category"].str.title()

    upload_data_db(prod_df, "products")
    dq_reports.append(report)

    # -------- SALES → ORDERS + ORDER_ITEMS --------
    sales_df = read_raw_data(DATA_DIR / "sales_raw.csv")
    sales_df, report = find_treat_missing_val(sales_df, "Sales")

    sales_df = sales_df[~sales_df.eq("Unknown").any(axis=1)]
    sales_df["total_amount"] = sales_df["quantity"] * sales_df["unit_price"]

    sales_df[["transaction_id", "customer_id", "product_id"]] = (
        sales_df[["transaction_id", "customer_id", "product_id"]]
        .astype(str)
        .apply(lambda s: s.str.replace(r"\D+", "", regex=True))
    )

    sales_df.rename(
        columns={
            "transaction_id": "order_id",
            "transaction_date": "order_date"
        },
        inplace=True
    )

    sales_df = clean_date_columns(sales_df)

    orders_df = sales_df[["order_id", "customer_id", "order_date", "total_amount", "status"]]
    upload_data_db(orders_df, "orders")

    order_items_df = sales_df[["product_id", "order_id", "quantity", "unit_price"]]
    upload_data_db(order_items_df, "order_items")

    dq_reports.append(report)

    generate_data_quality_report_txt(dq_reports)
    print("ETL pipeline completed successfully")


if __name__ == "__main__":
    main()
