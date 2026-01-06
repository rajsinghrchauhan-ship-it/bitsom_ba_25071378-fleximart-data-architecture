# Database Schema Documentation

## 1. Entity–Relationship Description

### ENTITY: customers
**Purpose:** Stores customer master information.

**Attributes:**
- `customer_id` : Unique identifier for each customer (Primary Key)
- `first_name`: Customer's first name
- `last_name`: Customer's last name
- `email`: Customer's email address
- `phone`: Customer's contact number
- `city`: Customer's city
- `registration_date`: Record creation date

**Relationships:**
- One customer can place **MANY orders** (1:M relationship with `orders` table)

---

### ENTITY: products
**Purpose:** Stores product catalog details.

**Attributes:**
- `product_id` : Unique product identifier (Primary Key)
- `product_name`: Name of the product
- `category`: Product category (e.g. Electronics, Fashion etc.)
- `price`: Unit price
- `stock_quantity`: Available stock qty.

**Relationships:**
- One product can appear in **MANY order items** (1:M relantionship with `order_items`)

---

### ENTITY: orders
**Purpose:** Stores customer order header details.

**Attributes:**
- `order_id` : Unique order identifier (Primary Key)
- `customer_id` : References `customers.customer_id` (Foreign Key)
- `order_date`: Date of order placed
- `total_amount`: Total order value
- `status`: Order status (e.g. In process, completed, cancelled etc.)

**Relationships:**
- One order belongs to **one customer**
- One order can contain **MANY order items** (1:M relationship with `order_items`)

---

### ENTITY: order_items
**Purpose:** Stores line-level details for each product within an order.

**Attributes:**
- `order_item_id` : Unique order identifier (Surrogate Key)
- `order_id` : References `orders.order_id` (Foreign Key)
- `product_id` : References `products.product_id` (Foreign Key)
- `quantity`: Quantity ordered
- `unit_price`: Price per unit

**Relationships:**
- One order can have **MANY order_items** (1:M relationship with `orders`)
- One product can be part of **MANY order_items** (1:M with `products`)
- order_items resolves the **MANY-to-MANY** relationship between `orders` and `products`  

---

## 2. Normalization Explanation (3NF)

--The database schema is designed in accordance with **Third Normal Form (3NF)** to ensure high data integrity, minimize redundancy, and support efficient data management. In this design, each table represents a single real-world entity, and all non-key attributes are fully functionally dependent on the primary key of their respective tables. This ensures that the database structure is logical, consistent, and free from unnecessary duplication of data.

In the **customers** table, attributes such as `first name, last name, email, phone number`, and `city` depend exclusively on the unique identifier `customer_id`. No non-key attribute depends on another non-key attribute, thereby satisfying the conditions of **3NF**. Similarly, the **products** table stores product-related information such as `product name, category`, and `price`, all of which depend solely on `product_id`.

The **orders** table captures order-level information, including `order date, total amount`, and `status`, which are fully dependent on `order_id`. The **order_items** table acts as a junction table to resolve the **many-to-many** relationship between `orders` and `products`. Attributes like quantity and unit price are specific to the relationship between an order and a product and therefore correctly belong in this table.

--Functional dependencies are clearly defined: `customer_id → customer attributes`, `product_id → product attributes`, and `order_id → order attributes`. The `order_items` table resolves the **many-to-many** relationship between `orders` and `products`, ensuring atomicity of data.

--This design avoids **update anomalies** by preventing duplicated customer or product information across tables. **Insert anomalies** are avoided because new customers or products can be added independently of orders. **Delete anomalies** are prevented because deleting an order does not remove customer or product master data. By separating concerns into well-structured tables and enforcing foreign key relationships, the schema maintains consistency, scalability, and reliability.

---

## 3. Sample Data Representation

### customers
customer_id	    first_name	    last_name	    email	                phone	        city	    registration_date
001	            Rahul	        Sharma	        rahul.sharma@gmail.com	+919876543210	Bangalore	2023-01-15
002	            Priya	        Patel	        priya.patel@yahoo.com	+919988776564	Mumbai	    2023-02-20


### products
product_id	  product_name	        category	    price	    stock_quantity
1	          Samsung Galaxy S21	Electronics	    45999.00	150
2	          Nike Running Shoes	fashion	        3499.00	    80


### orders
order_id	customer_id	    order_date  total_amount	status
1	        001	    	    2024-01-15  45999.00    	Competed
2	        002	            2024-01-16  5998.00     	Completed


### order_items
order_item_id   order_id	product_id      Quantity    unit_price
1	            1	    	1                   1       45999.00
8	            10	        6                   5       899.00

