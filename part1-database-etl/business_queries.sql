-- Database - fleximart_dw

-- Query 1: Customer Purchase History

/*Business Question: "Generate a detailed report showing each customer's name, email, total number of orders placed, and total amount spent. 
Include only customers who have placed at least 2 orders and spent more than ₹5,000. Order by total amount spent in descending order."*/

SELECT 
    CONCAT(c.first_name,' ', c.last_name) AS customer_name,
    c.email,
    COUNT(o.order_id) AS total_orders,
    sum(oi.quantity*oi.unit_price) AS total_spent
FROM customers c 
JOIN orders o on 
	c.customer_id=o.customer_id
JOIN order_items oi on 
	o.order_id=oi.order_id
GROUP BY c.first_name, c.last_name, c.email
HAVING 
	COUNT(o.order_id) >= 2
    AND SUM(oi.quantity * oi.unit_price) > 50000
ORDER BY total_spent DESC;

--Refer for result screenshot: customer_purchase_history.png


-- Query 2: Product Sales Analysis

/*Business Question: "For each product category, show the category name, number of different products sold, total quantity sold, and total revenue generated. 
Only include categories that have generated more than ₹10,000 in revenue. Order by total revenue descending."*/

SELECT
    p.category,
    COUNT(DISTINCT p.product_id) AS num_products,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM products p
JOIN order_items oi
    ON p.product_id = oi.product_id
GROUP BY
    p.category
HAVING
    SUM(oi.quantity * oi.unit_price) > 10000
ORDER BY
    total_revenue DESC;

--Refer for result screenshot: product_sales_analysis.png


-- Query 3: Monthly Sales Trend

/*Business Question: "Show monthly sales trends for the year 2024. For each month, display the month name, total number of orders, total revenue, 
and the running total of revenue (cumulative revenue from January to that month)."*/

SELECT
    month_name,
    total_orders,
    monthly_revenue,
    SUM(monthly_revenue) OVER (ORDER BY month_num) AS cumulative_revenue
FROM (
    SELECT
        MONTH(o.order_date) AS month_num,
        MONTHNAME(o.order_date) AS month_name,
        COUNT(o.order_id) AS total_orders,
        SUM(oi.quantity * oi.unit_price) AS monthly_revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    WHERE YEAR(o.order_date) = 2024
    GROUP BY
        MONTH(o.order_date),
        MONTHNAME(o.order_date)
) t
ORDER BY month_num;

--Refer for result screenshot: monthly_sales_trend.png

