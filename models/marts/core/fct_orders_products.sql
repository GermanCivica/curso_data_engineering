
{{
    config(
        materialized='incremental',
    )
}}

WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_orders') }}
{% if is_incremental() %}

    where (tracking_id <> '' AND date_load_utc > (select max(created_at_utc) from {{ this }}))

{% endif %}
),

stg_products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
),

stg_events AS (
    SELECT *
    FROM {{ ref('stg_events') }}
{% if is_incremental() %}

    where date_load_utc > (select max(created_at_utc) from {{ this }})

{% endif %}
),

dim_shipping AS (
    SELECT *
    FROM {{ ref('dim_shipping') }}
{% if is_incremental() %}

	where order_data_load_utc > (select max(created_at_utc) from {{ this }})

{% endif %}
),

order_events AS (
    SELECT 
         session_id
        , event_id
        , event_type
        , user_id
        , product_id
        , order_id
        , created_at_utc
    FROM (SELECT
                  session_id
                , event_id
                , event_type
                , user_id
                , product_id
                , order_id
                , created_at_utc
                , ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY created_at_utc) AS action_counter
          FROM stg_events
          WHERE event_type = 'add_to_cart' OR event_type = 'checkout')
),

order_products_events AS (
    SELECT 
          order_events.session_id
        , order_events.event_id
        , order_events.event_type
        , order_events.user_id
        , order_events.product_id
        , CASE 
            WHEN order_events.order_id IS NULL
            THEN stg_events.order_id
            END as order_id
    FROM order_events
    JOIN stg_events ON order_events.session_id = stg_events.session_id
    WHERE order_events.product_id IS NOT NULL AND stg_events.order_id IS NOT NULL
),

renamed_casted AS (
    SELECT
          order_products_events.event_id
        , order_products_events.product_id
        , stg_orders.order_id 
        , stg_orders.created_at_utc
        , order_products_events.user_id
        , stg_orders.address_id
        , stg_orders.promo_id
        , stg_products.price_usd
        , dim_shipping.shipping_cost_per_item_usd AS shipping_cost_usd
        , stg_orders.shipping_service_id
        , stg_orders.status_order_id
        , stg_orders.estimated_delivery_at_utc
        , stg_orders.delivered_at_utc
        , DATEDIFF(day, stg_orders.created_at_utc, stg_orders.delivered_at_utc) AS days_to_deliver
    FROM order_products_events
    JOIN stg_orders ON order_products_events.order_id = stg_orders.order_id
    JOIN stg_products ON order_products_events.product_id = stg_products.product_id
    JOIN dim_shipping ON order_products_events.order_id = dim_shipping.order_id
    GROUP BY   order_products_events.event_id, order_products_events.product_id, stg_orders.order_id, stg_orders.created_at_utc, order_products_events.user_id, stg_orders.address_id
             , stg_orders.promo_id, stg_products.price_usd, dim_shipping.shipping_cost_per_item_usd, stg_orders.shipping_service_id, stg_orders.status_order_id, stg_orders.estimated_delivery_at_utc
             , stg_orders.delivered_at_utc, days_to_deliver
    )

SELECT * FROM renamed_casted