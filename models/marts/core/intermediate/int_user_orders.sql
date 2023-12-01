{{
  config(
    materialized='table'
  )
}}

WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_orders') }}
),

stg_users AS (
    SELECT *
    FROM {{ ref('stg_users') }}
),

stg_addresses AS (
    SELECT *
    FROM {{ ref('stg_addresses') }}
),

stg_promos AS (
    SELECT *
    FROM {{ ref('stg_promos') }}
),


renamed_casted AS (
    SELECT 
          stg_orders.user_id
        , stg_users.first_name
        , stg_users.last_name
        , stg_users.phone_number
        , stg_users.email
        , COUNT(stg_orders.order_id) as total_orders_by_user
        , CAST(SUM(total_order_cost_usd) AS DECIMAL(18, 2)) as user_total_expenditure_usd
        , CAST(SUM(shipping_cost_usd) AS DECIMAL(18, 2)) as user_total_shipping_cost_usd
        , CAST(SUM(stg_promos.discount_usd) AS DECIMAL(18, 2)) as user_total_discount_usd
        --, productos distintos comprados 
    FROM stg_orders
    JOIN stg_promos ON stg_orders.promo_id = stg_promos.promo_id
    JOIN stg_users ON stg_orders.user_id = stg_users.user_id
    GROUP BY stg_orders.user_id, stg_users.first_name, stg_users.last_name, stg_users.phone_number, stg_users.email
    ORDER BY user_total_expenditure_usd DESC
)

SELECT * FROM renamed_casted
