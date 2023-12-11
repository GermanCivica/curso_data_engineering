
{{
    config(
    materialized='table',
    unique_key='shipping_id',
    on_schema_change='fail'
  )
}}

WITH stg_orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
{% if is_incremental() %}

    WHERE (tracking_id <> '' AND date_load_utc > (select max(date_load_utc) from {{ this }}))

{% endif %}
),

stg_addresses AS (
    SELECT
          country
        , state
        , zipcode
        , address
        , address_id
        , date_load_utc
    FROM {{ ref('stg_addresses') }}
{% if is_incremental() %}

	  where date_load_utc > (select max(date_load_utc) from {{ this }})

{% endif %}
),

stg_order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
{% if is_incremental() %}

	  where date_load_utc > (select max(date_load_utc) from {{ this }})

{% endif %}
),

products_per_order AS (
    SELECT 
          stg_orders.order_id
        , SUM(quantity) AS total_products_per_order
    FROM stg_orders
    JOIN stg_order_items ON stg_orders.order_id = stg_order_items.order_id
    GROUP BY stg_orders.order_id
),

renamed_casted AS (
    SELECT
          {{dbt_utils.generate_surrogate_key(['stg_orders.order_id', 'stg_orders.tracking_id'])}} AS shipping_id
        , stg_orders.order_id
        , stg_orders.tracking_id
        , products_per_order.total_products_per_order
        , stg_orders.shipping_service_id
        , stg_orders.shipping_cost_usd AS total_shipping_cost_usd
        , CAST(stg_orders.shipping_cost_usd/products_per_order.total_products_per_order AS DECIMAL(18, 2)) AS shipping_cost_per_item_usd
        , stg_orders.status_order_id
        , stg_orders.estimated_delivery_at_utc
        , stg_orders.delivered_at_utc
        , stg_addresses.country AS shipped_country
        , stg_addresses.state AS shipped_state
        , stg_addresses.zipcode AS shipped_zipcode
        , stg_addresses.address AS shipped_address
        , stg_orders.date_load_utc AS order_data_load_utc
    FROM stg_orders
    JOIN products_per_order ON stg_orders.order_id = products_per_order.order_id
    JOIN stg_addresses ON stg_orders.address_id = stg_addresses.address_id
)

SELECT * FROM renamed_casted