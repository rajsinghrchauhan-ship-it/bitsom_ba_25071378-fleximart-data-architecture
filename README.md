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

- Python 3.x, pandas, mysql-connector-python
- MySQL 8.0 / PostgreSQL 14
- MongoDB 6.0

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

## Key Learnings

[3-4 sentences on what you learned]

## Challenges Faced

1. [Challenge and solution]
2. [Challenge and solution]

