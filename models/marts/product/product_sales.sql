{{
  config(
    materialized='table'
  )
}}

WITH stg_events AS (
    SELECT *
    FROM {{ ref('stg_events') }}
),

fct_orders_products AS (
    SELECT *
    FROM {{ ref('fct_orders_products') }}
),

stg_order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
),

int_orders_product AS (
    SELECT *
    FROM {{ ref('int_orders_product') }}
),

product_visits AS (
    SELECT *    
    FROM (SELECT
              product_id
            , COUNT(product_id) OVER (PARTITION BY product_id) AS visits
        FROM stg_events
        WHERE event_type = 'page_view')
    GROUP BY product_id, visits
    ORDER BY visits DESC
),

product_sales AS (
    SELECT *
    FROM (SELECT 
              fct_orders_products.product_id
            , product_visits.visits AS web_visits
            , int_orders_product.total_orders_with_product AS times_bought
            , int_orders_product.total_quantity_ordered AS total_units_sold
            , SUM(price_usd) OVER (PARTITION BY fct_orders_products.product_id) AS total_sales_usd
        FROM fct_orders_products
        JOIN product_visits ON product_visits.product_id = fct_orders_products.product_id
        JOIN int_orders_product ON int_orders_product.product_id = fct_orders_products.product_id)
    GROUP BY product_id, web_visits, times_bought, total_units_sold, total_sales_usd
    ORDER BY total_sales_usd DESC
)

SELECT * FROM product_sales