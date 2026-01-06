# Star Schema Design Documentation

## Section 1: Schema Overview

### Fact Table: `fact_sales`
- **Grain:** One row per product per order line item
- **Business Process:** Sales transactions

## Measures (Numeric Facts)
- `quantity_sold`: Number of units sold
- `unit_price`: Price per unit at the time of sale
- `discount_amount`: Discount applied
- `total_amount`: Final amount (quantity × unit_price - discount)

## Foreign Keys
- `date_key` → `dim_date`
- `product_key` → `dim_product`
- `customer_key` → `dim_customer`


#### DIMENSION TABLE: `dim_date`
- **Purpose:** Date dimension for time-based analysis
- **Type:** Conformed dimension

## Attributes:
- `date_key` (PK): Surrogate key (integer, format: YYYYMMDD)
- `full_date`: Actual date
- `day_of_week`: Monday, Tuesday, etc.
- `month`: 1–12
- `month_name`: January, February, etc.
- `quarter`: Q1, Q2, Q3, Q4
- `year`: 2023, 2024, etc.
- `is_weekend`: Boolean



#### DIMENSION TABLE: `dim_product`
- **Purpose:** Stores product-related information

## Attributes:
- `product_key` (PK): Surrogate key
- `product_id`: Product identifier
- `product_name`: Name of the product
- `category`: Product category (e.g. Electronics, Fashion, etc.)
- `subcategory`: Product falls under category (e.g. Laptop, Mobile, Fan, etc. under Category - Electronics)
- `unit_price`: Price of the product




#### DIMENSION TABLE: `dim_customer`
- **Purpose:** Stores customer master information

## Attributes
- `customer_key` (PK): Surrogate key
- `customer_id`: Customer identifier
- `customer_name`: Full name of the customer
- `email`: Customer email address
- `city`: Customer City
- `state`: Customer State
- `country`: Customer Country
- `customer_segment`: Retail, Wholesale, Corporate





### Section 2: Design Decisions

## 1. Choice of Granularity (Transaction line-ltem level)
The fact table is designed at the transaction line-item level, meaning each row represents a single product within an order. This granularity provides maximum analytical flexibility, allowing detailed analysis such as product-wise sales, order-level revenue, customer purchasing behavior, and customer segment sales. It also supports accurate aggregations without data loss when rolling up to higher levels like daily, monthly, or yearly sales.

## 2. Use of Surrogate Keys
Surrogate keys are used instead of natural keys to ensure consistency and stability in the data warehouse. Natural keys from source systems may change over time or differ across systems. Surrogate keys improve query performance, simplify joins, and support Slowly Changing Dimensions, making the schema more robust and scalable.

## 3. Support for Drill-Down and Roll-Up
The star schema enables efficient drill-down (year → quarter → month → day, category → product) and roll-up operations through well-defined dimension hierarchies, supporting fast and intuitive analytical queries.





### Section 3: Sample Data Flow (3 marks)

### Source Transaction
- Order #101, Customer "John Doe", Product "Laptop", Qty: 2, Price: 50000

## Becomes in Data Warehouse:
fact_sales: {
  date_key: 20240115,
  product_key: 5,
  customer_key: 12,
  quantity_sold: 2,
  unit_price: 50000,
  total_amount: 100000
}

dim_date: {date_key: 20240115, full_date: '2024-01-15', month: 1, quarter: 'Q1', year: 2024, ...}

dim_product: {product_key: 5, product_name: 'Laptop', category: 'Electronics'...}

dim_customer: {customer_key: 12, customer_name: 'John Doe', city: 'Mumbai'...}


This flow demonstrates how transactional data is transformed into a structured star schema suitable for analytical reporting.

