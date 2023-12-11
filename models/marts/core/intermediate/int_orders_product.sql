{{
    config (
      materialized='view'
    )
}}

WITH stg_order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
),

stg_orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),

stg_products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
),

stg_budget AS (
    SELECT *
    FROM {{ ref('stg_budget') }}
),

stg_fechas AS (
    SELECT *
    FROM {{ ref('stg_fechas') }}
),

orders_month AS (
    SELECT 
          order_id
        , mes as order_month
        , anio as order_year
    FROM stg_orders
    JOIN stg_fechas ON year(stg_orders.created_at_utc)*100+month(stg_orders.created_at_utc) = stg_fechas.id_anio_mes
    GROUP BY order_id, order_month, order_year
),

orders_month_product AS (
    SELECT 
          orders_month.order_id
        , product_id
        , quantity
        , order_month
        , order_year
    FROM orders_month
    JOIN stg_order_items ON orders_month.order_id = stg_order_items.order_id
),

orders_products_ordered_by_month AS (
    SELECT
          order_id
        , product_id
        , quantity
        , order_month
        , order_year
        , SUM(quantity) OVER (PARTITION BY product_id, order_month) AS total_quantity_in_month
        , COUNT(order_id) OVER (PARTITION BY product_id, order_month) AS orders_with_product_in_month
    FROM orders_month_product
),

total_products_ordered AS (
    SELECT *
    FROM (SELECT
                product_id
              , SUM(quantity) OVER (PARTITION BY product_id) AS total_quantity_ordered
              , COUNT(order_id) OVER (PARTITION BY product_id) AS total_orders_with_product
          FROM stg_order_items)
    GROUP BY product_id, total_quantity_ordered, total_orders_with_product
),

int_orders_product AS (
    SELECT
          orders_products_ordered_by_month.order_id
        , orders_products_ordered_by_month.product_id
        , stg_products.name as product_name
        , order_month
        , order_year
        , orders_products_ordered_by_month.quantity
        , total_products_ordered.total_quantity_ordered
        , total_quantity_in_month
        , orders_with_product_in_month
        , total_products_ordered.total_orders_with_product
        , stg_budget.quantity as product_budget_for_month
    FROM orders_products_ordered_by_month
    JOIN total_products_ordered ON orders_products_ordered_by_month.product_id = total_products_ordered.product_id
    JOIN stg_products ON orders_products_ordered_by_month.product_id = stg_products.product_id
    JOIN stg_budget ON (orders_products_ordered_by_month.product_id = stg_budget.product_id AND orders_products_ordered_by_month.order_year = year(stg_budget.month) AND orders_products_ordered_by_month.order_month = month(stg_budget.month))
)

SELECT * FROM int_orders_product