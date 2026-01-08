# FlexiMart Data Architecture Project

**Student Name:** [RAJ SINGH CHAUHAN]
**Student ID:** [bitsom_ba_25071378]
**Email:** [raj.singh@msn.com]
**Date:** [05-Jan-2025]

## Project Overview

This project focuses on end-to-end data engineering and analytics. It includes building an ETL pipeline in Python to ingest raw CSV data into a relational database, followed by creating database documentation that captures schema and relationships. Business insights will be derived by writing SQL queries to answer specific questions. On the NoSQL side, the project involves analyzing product data requirements and implementing MongoDB operations for flexible data handling. Finally, a data warehouse will be designed using a star schema to support OLAP queries and generate analytical reports. All components will be supported with comprehensive documentation and code for clarity and maintainability.

## Repository Structure
├── part1-database-etl/
│   ├── etl_pipeline.py
│   ├── schema_documentation.md
│   ├── business_queries.sql
│   └── data_quality_report.txt
├── part2-nosql/
│   ├── nosql_analysis.md
│   ├── mongodb_operations.js
│   └── products_catalog.json
├── part3-datawarehouse/
│   ├── star_schema_design.md
│   ├── warehouse_schema.sql
│   ├── warehouse_data.sql
│   └── analytics_queries.sql
└── README.md

## Technologies Used
- Python 3.x, pandas, mysql-connector-python, phonenumbers
- MySQL 8.0
- MongoDB 6.0

## Requirements
- Python 3.9+
- MySQL 8.0
- MongoDB 6.0
- Libraries: pandas, numpy, phonenumbers, mysql-connector-python, python-dotenv

## Installation
Install Python dependencies:
```bash
pip install -r part1-database-etl/requirements.txt
```
Ensure MySQL and MongoDB are installed and running locally or on your preferred environment.

## Setup Instructions

### Database Setup

```bash
# Create databases
mysql -u root -p -e "CREATE DATABASE fleximart;"
mysql -u root -p -e "CREATE DATABASE fleximart_dw;"

# Run Part 1 - ETL Pipeline
python part1-database-etl/etl_pipeline.py

# Run Part 1 - Business Queries
mysql -u root -p fleximart < part1-database-etl/business_queries.sql

# Run Part 3 - Data Warehouse
mysql -u root -p fleximart_dw < part3-datawarehouse/warehouse_schema.sql
mysql -u root -p fleximart_dw < part3-datawarehouse/warehouse_data.sql
mysql -u root -p fleximart_dw < part3-datawarehouse/analytics_queries.sql


### MongoDB Setup

mongosh < part2-nosql/mongodb_operations.js

## Workflow
1. ETL pipeline → Load data into MySQL
2. Schema documentation → Understand relationships
3. Business queries → Generate insights
4. MongoDB operations → Handle flexible product data
5. Data warehouse → Star schema for analytics

## Key Learnings
- Learned how to design an ETL pipeline using Python and SQL.
- Understood schema documentation and its importance in relational databases.
- Gained experience in MongoDB for flexible data handling.
- Designed a star schema for OLAP queries and analytics.

## Challenges Faced
1. **Challenge:** Handling inconsistent CSV data  
   **Solution:** Implemented data validation and cleaning in ETL script.
2. **Challenge:** Optimizing SQL queries for large datasets  
   **Solution:** Used indexing and query optimization techniques.

