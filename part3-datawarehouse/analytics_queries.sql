-- Query 1: Monthly Sales Drill-Down

SELECT 
    a.year,
    a.quarter,
    a.month_name,
    SUM(b.total_amount) AS total_sales,
    SUM(b.quantity_sold) AS total_quantity
FROM fleximart_dw.dim_date a
INNER JOIN fleximart_dw.fact_sales b 
    ON a.date_key = b.date_key
GROUP BY a.year, a.quarter, a.month_name, a.month
ORDER BY a.year, a.quarter, a.month;

--Refer for result screenshot: monthly_sales_drill_down.png



-- Query 2: Top 10 Products by Revenue

SELECT
  product_name,
  category,
  units_sold,
  revenue,
  revenue_percentage
FROM (
  SELECT
    b.product_name,
    b.category,
    SUM(a.quantity_sold) AS units_sold,
    SUM(a.total_amount)  AS revenue,
    ROUND(
      SUM(a.total_amount) * 100.0
      / SUM(SUM(a.total_amount)) OVER (), 2
    ) AS revenue_percentage,
    DENSE_RANK() OVER (ORDER BY SUM(a.total_amount) DESC) AS rev_rank
  FROM fleximart_dw.fact_sales AS a
  JOIN fleximart_dw.dim_product AS b
    ON a.product_key = b.product_key         
  GROUP BY b.product_name, b.category
) AS ranked
WHERE rev_rank <= 10
ORDER BY revenue DESC;


--Refer for result screenshot: product_performance.png




-- Query 3: Customer Segmentation
SELECT
	CASE
		WHEN total_value >= 50000 THEN 'High Value'
		WHEN total_value >= 20000 AND total_value < 50000 THEN 'Medium Value'
		ELSE 'Low Value'
  END AS customer_segment,
  COUNT(customer_key) AS customer_count,
  SUM(total_value)    AS total_revenue,
  ROUND(SUM(total_value) / COUNT(customer_key), 2) AS average_revenue
FROM 
(SELECT  customer_key,
    SUM(total_amount) AS total_value
FROM fleximart_dw.fact_sales
GROUP BY customer_key
) t
GROUP BY customer_segment
ORDER BY FIELD(customer_segment, 'High Value', 'Medium Value', 'Low Value');

--Refer for result screenshot: customer_segmentation.png