{{
  config(
    materialized='table'
  )
}}

WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_orders') }}
),

WITH stg_order_items AS (
    SELECT *
    FROM FROM {{ ref('stg_order_items') }}
)

renamed_casted AS (
    SELECT
          order_id 
        , user_id 
        , promo_id
        , address_id
        , created_at_utc
        , SUM(stg_order_items.quantity) as total_ordered_items
        , item_order_cost_usd
        , shipping_cost_usd
        , total_order_cost_usd
        , tracking_id
        , shipping_service
        , estimated_delivery_at_utc
        , delivered_at_utc
		, DATEDIFF(day, created_at_utc, delivered_at_utc) AS days_to_deliver
        , status_order
        , date_load
    FROM stg_orders
    JOIN stg_order_items ON stg_order_items.order_id = stg_orders.order_id
    )

SELECT * FROM renamed_casted