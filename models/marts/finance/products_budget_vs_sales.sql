{{
  config(
    materialized='table'
  )
}}

WITH fct_orders_products AS (
    SELECT *
    FROM {{ ref('fct_orders_products') }}
),

int_orders_product AS (
    SELECT *
    FROM {{ ref('int_orders_product') }}
),

product_sales_month AS (
    SELECT
          product_id
        , year(fct_orders_products.created_at_utc) AS year_sale
        , month(fct_orders_products.created_at_utc) AS month_sale
        , SUM(fct_orders_products.price_usd) OVER (PARTITION BY product_id, year_sale, month_sale) AS total_sales_product_month_usd
        , SUM(fct_orders_products.shipping_cost_usd) OVER (PARTITION BY product_id, year_sale, month_sale) AS total_shipping_cost_product_month_usd
    FROM fct_orders_products
),

product_budget_vs_sales AS (
    SELECT * 
    FROM (
        SELECT 
              product_sales_month.product_id
            , int_orders_product.product_name
            , int_orders_product.order_year AS year
            , int_orders_product.order_month AS month
            , int_orders_product.product_budget_for_month AS budget
            , int_orders_product.total_quantity_ordered AS total_units_sold
            , CASE WHEN total_units_sold > budget THEN True ELSE False END AS over_budget
            , total_sales_product_month_usd
            , total_shipping_cost_product_month_usd
        FROM product_sales_month
        JOIN int_orders_product ON (product_sales_month.product_id = int_orders_product.product_id AND product_sales_month.year_sale = int_orders_product.order_year AND product_sales_month.month_sale = int_orders_product.order_month))
    GROUP BY product_id, product_name, year, month, budget, total_units_sold, over_budget, total_sales_product_month_usd, total_shipping_cost_product_month_usd
)

SELECT * FROM product_budget_vs_sales