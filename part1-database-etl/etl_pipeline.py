# %% [markdown]
# ## Part 1: Database Design and ETL Pipeline

# %% [markdown]
# ### Extract: Read all three CSV files
# ### Transform:
# - Remove duplicate records
# - Handle missing values (use appropriate strategy: drop, fill, or default)
# - Standardize phone formats (e.g., +91-9876543210)
# - Standardize category names (e.g., "electronics", "Electronics", "ELECTRONICS" → "Electronics")
# - Convert date formats to YYYY-MM-DD
# - Add surrogate keys (auto-incrementing IDs)

# %%
# Imports needed

import pandas as pd
import numpy as np
import phonenumbers
from pathlib import Path
import mysql.connector
from dotenv import load_dotenv
import os

# Current file location
CURRENT_DIR = Path(__file__).resolve().parent

# Project root directory
PROJECT_ROOT = CURRENT_DIR.parent

# Standard project folders
DATA_DIR = PROJECT_ROOT / "data"
ETL_DIR = PROJECT_ROOT / "part1-database-etl"

# %%
# Read raw data

def read_raw_data(file_name):

    return pd.read_csv(file_name)
    

# %%
#Data Quality Report
data_quality_report = []

# Function to check and handling missing value in the columns of the dataframe
def find_treat_missing_val(df: pd.DataFrame):

    df = df.copy()

    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    categorical_cols = df.select_dtypes(include=["object", "category", "bool"]).columns

    missing_summary = []

    for col, cnt in df.isna().sum().items():
        if cnt > 0:
            missing_summary.append(f"{col}: {cnt}")
            if col in numeric_cols:
                df[col] = df[col].fillna(df[col].median())
            else:
                df[col] = df[col].fillna('Unknown')

    # Handle date column
    dte_col = df.columns[df.columns.str.contains('date', case=False)]
    if len(dte_col) > 0:
        df.loc[:, dte_col[0]] = pd.to_datetime(
            df[dte_col[0]],
            format='mixed',
            errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    transform_df = df.drop_duplicates().copy()

    data_quality_report.append({
        'Record Count': len(df),
        'Duplicate Rows': int(df.duplicated().sum()),
        'Null Count': ", ".join(missing_summary) if missing_summary else "No Nulls",
        'Insert Count': len(transform_df)
    })

    
    
    return transform_df, data_quality_report

# %% [markdown]
# ## Inserting the data to the MYSQL database tables

# %%
# Function to upload data to MySQL database
def upload_data_db(df: pd.DataFrame, table_name: str):
    load_dotenv()

    conn = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
        use_pure=True
    )

    cursor = conn.cursor()

    cols = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))

    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    cursor.executemany(sql, df.values.tolist())

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Loaded {len(df)} records into {table_name}")
   

# %% [markdown]
# ### Customer Data

# %%
# Generate Data Quality Report in TXT format
from datetime import datetime

def generate_data_quality_report_txt(report, file_name="data_quality_report.txt"):
    report_path = ETL_DIR / file_name

    with open(report_path, "w") as f:
        f.write("DATA QUALITY REPORT\n")
        f.write("=" * 60 + "\n")
        f.write(f"Generated On: {datetime.now()}\n\n")

        for idx, row in enumerate(report, start=1):
            f.write(f"Dataset #{idx}\n")
            f.write(f"Records Processed      : {row['Record Count']}\n")
            f.write(f"Duplicates Removed     : {row['Duplicate Rows']}\n")
            f.write(f"Missing Values Handled : {row['Null Count']}\n")
            f.write(f"Records Loaded         : {row['Insert Count']}\n")
            f.write("-" * 60 + "\n")

cust_df = read_raw_data(DATA_DIR / 'customers_raw.csv')

cust_df_clean, dq_report = find_treat_missing_val(cust_df)

#phone number formatting
cust_df_clean.loc[:, 'phone'] = cust_df_clean['phone'].apply(
    lambda x: phonenumbers.format_number(
        phonenumbers.parse(str(x), region='IN'),
        phonenumbers.PhoneNumberFormat.E164
    ) if pd.notna(x) else None
)

#customer_id formatting
cust_df_clean.loc[:, 'customer_id'] = (
    cust_df_clean['customer_id']
    .astype(str)
    .str.replace(r'\D+', '', regex=True)
)

#Remove duplicates before uploading to DB
cust_df_clean = cust_df_clean.drop_duplicates(
    subset='customer_id',
    keep='first'
)

#upload to DB
upload_data_db(cust_df_clean,'customers')
cust_df_clean.head()

# %% [markdown]
# ![image.png](attachment:image.png)

# %% [markdown]
# ### Product 

# %%
prod_df = read_raw_data(DATA_DIR /'products_raw.csv')
prod_df_clean, dq_report = find_treat_missing_val(prod_df)
prod_df_clean['product_id'] = prod_df_clean['product_id'].astype(str).str.replace(r'\D+', '', regex=True)
prod_df_clean['category'] = prod_df_clean['category'].str.title()
upload_data_db(prod_df_clean,'products')
prod_df_clean.head()

# %% [markdown]
# ![image-2.png](attachment:image-2.png)

# %% [markdown]
# ### Transcation Data

# %%
sales_df = read_raw_data(DATA_DIR /'sales_raw.csv')
sales_df_clean, dq_report = find_treat_missing_val(sales_df)



sales_df_clean = sales_df_clean[~sales_df_clean.eq('Unknown').any(axis=1)]

sales_df_clean['total_amount'] = sales_df_clean['quantity'] * sales_df_clean['unit_price'] 
sales_df_clean[['transaction_id', 'customer_id','product_id']] = sales_df_clean[['transaction_id', 'customer_id','product_id']].astype(str).apply(lambda s:s.str.replace(r'\D+', '', regex=True))
sales_df_clean.rename(columns={'transaction_id': 'order_id','transaction_date':'order_date'}, inplace=True)

# %%
upload_data_db(sales_df_clean[['order_id','customer_id','order_date','total_amount','status']],'orders')
upload_data_db(sales_df_clean[['product_id','order_id','quantity','unit_price']],'order_items')
sales_df_clean.head()

# %% [markdown]
# ![image.png](attachment:image.png)

# %% [markdown]
# ![image.png](attachment:image.png)

# %% [markdown]
# ### Data Quality Report

# %%
pd.DataFrame(data_quality_report)

# %%
generate_data_quality_report_txt(data_quality_report)


