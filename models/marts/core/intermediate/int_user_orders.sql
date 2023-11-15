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

renamed_casted AS (
    SELECT 
          stg_orders.user_id
        , stg_users.first_name
        , stg_users.last_name
        , stg_users.phone_number
        , stg_addresses.state as user_residence_state
        , SUM(total_order_cost_usd) as user_total_expenditure_usd
    FROM stg_orders
    JOIN stg_users ON stg_orders.user_id = stg_users.user_id
    JOIN stg_addresses ON stg_orders.address_id = stg_addresses.address_id
    GROUP BY stg_orders.user_id, stg_users.first_name, stg_users.last_name, stg_users.phone_number, stg_addresses.state
    ORDER BY user_total_expenditure_usd DESC
)

SELECT * FROM renamed_casted