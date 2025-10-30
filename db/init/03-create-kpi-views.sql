-- Connect to the 'booklatte' database to ensure views are created in the correct place.
\c booklatte;

-- 1. Daily Sales Summary
-- (Daily, Monthly, Quarterly, Annual Trends, and Monthly Transaction vs. Revenue).
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_sales_summary AS
SELECT
    date,
    -- Use DATE_TRUNC to get consistent start-of-period dates for easy grouping
    DATE_TRUNC('month', date) AS month,
    DATE_TRUNC('quarter', date) AS quarter,
    DATE_TRUNC('year', date) AS year,
    SUM(net_total) AS total_revenue,
    COUNT(DISTINCT receipt_no) AS total_transactions
FROM
    fact_transactions
GROUP BY
    date
ORDER BY
    date;

-- 2. Category Sales Summary
-- This view aggregates revenue for each product category. It directly powers the 'Revenue by Category' pie chart.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_category_sales_summary AS
SELECT
    p.category,
    SUM(f.line_total) AS total_revenue
FROM
    fact_transactions AS f
JOIN
    current_product_dimension AS p ON f.product_id = p.product_id
GROUP BY
    p.category;

-- 3. Product Performance Summary
-- all 'Top 10' bar charts by filtering for the desired category ('FOOD' or 'DRINK') and sorting.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product_sales_summary AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(f.qty) AS total_quantity_sold,
    SUM(f.line_total) AS total_revenue
FROM
    fact_transactions AS f
JOIN
    current_product_dimension AS p ON f.product_id = p.product_id
GROUP BY
    p.product_id,
    p.product_name,
    p.category;

--Note to self. Need to refresh materialized views in Python script after ETL dims are created.
-- REFRESH MATERIALIZED VIEW mv_daily_sales_summary;
-- REFRESH MATERIALIZED VIEW mv_category_sales_summary;
-- REFRESH MATERIALIZED VIEW mv_product_sales_summary;