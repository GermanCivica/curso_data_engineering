{{
    config (
      materialized='view'
    )
}}

WITH stg_orders AS (
    SELECT 
          order_id
        , user_id
        , address_id
    FROM {{ ref('stg_orders') }}
),

stg_users AS (
    SELECT
          user_id
        , address_id
    FROM {{ ref('stg_users') }}
),

renamed_casted AS (
    SELECT 
          {{dbt_utils.generate_surrogate_key(['stg_users.address_id', 'stg_orders.address_id'])}} as user_order_address_id
        , stg_orders.user_id 
        , order_id
        , stg_orders.address_id AS order_address
        , stg_users.address_id AS user_registered_address
        , CASE 
              WHEN stg_orders.address_id = stg_users.address_id THEN True 
              ELSE False
          END AS registered_user_address
    FROM stg_orders
    JOIN stg_users ON stg_orders.user_id = stg_users.user_id
    GROUP BY stg_orders.user_id, order_id, stg_orders.address_id, stg_users.address_id
    ORDER BY stg_orders.user_id
)

SELECT * FROM renamed_casted