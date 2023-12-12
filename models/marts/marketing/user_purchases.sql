{{
  config(
    materialized='table'
  )
}}

WITH int_user_orders AS (
    SELECT * 
    FROM {{ ref('int_user_orders') }}
),

fct_orders_products AS (
    SELECT *
    FROM {{ ref('fct_orders_products') }}
),

users_products AS (
    SELECT 
          user_id
        , count_client_products
        , COUNT(product_id) OVER (PARTITION BY user_id) as count_diff_products_bought
    FROM (SELECT
              order_id
            , user_id
            , product_id
            , COUNT(product_id) OVER (PARTITION BY user_id) AS count_client_products 
            , ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY user_id) AS client_product
          FROM fct_orders_products)
    WHERE client_product = 1
),

user_purchases AS (
    SELECT
          int_user_orders.user_id
        , first_name
        , last_name
        , phone_number
        , email
        , total_orders_by_user
        , user_total_expenditure_usd
        , user_total_shipping_cost_usd
        , user_total_discount_usd
        , count_client_products AS user_total_products_bought
        , count_diff_products_bought AS user_diff_products_bought
    FROM int_user_orders
    JOIN users_products ON int_user_orders.user_id = users_products.user_id
    GROUP BY int_user_orders.user_id, first_name, last_name, phone_number, email, total_orders_by_user, user_total_expenditure_usd, user_total_shipping_cost_usd, user_total_discount_usd, user_total_products_bought, user_diff_products_bought
)

SELECT * FROM user_purchases