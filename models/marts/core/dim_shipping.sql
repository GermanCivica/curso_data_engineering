
{{
    config(
    materialized='view'
  )
}}

WITH stg_orders AS (
    SELECT
          order_id
        , tracking_id
        , shipping_service_id
        , shipping_cost_usd
        , status_order_id
        , estimated_delivery_at_utc
        , delivered_at_utc
        , address_id
    FROM {{ ref('stg_orders') }}
    WHERE tracking_id <> ''
),

stg_addresses AS (
    SELECT
          country
        , state
        , zipcode
        , address
        , address_id
    FROM {{ ref('stg_addresses') }}
),

renamed_casted AS (
    SELECT
          {{dbt_utils.generate_surrogate_key(['order_id', 'tracking_id'])}} AS shipping_id
        , stg_orders.tracking_id
        , stg_orders.shipping_service_id
        , stg_orders.shipping_cost_usd 
        , stg_orders.status_order_id
        , stg_orders.estimated_delivery_at_utc
        , stg_orders.delivered_at_utc
        , stg_addresses.country AS shipped_country
        , stg_addresses.state AS shipped_state
        , stg_addresses.zipcode AS shipped_zipcode
        , stg_addresses.address AS shipped_address
    FROM stg_orders
    JOIN stg_addresses ON stg_orders.address_id = stg_addresses.address_id
)

SELECT * FROM renamed_casted