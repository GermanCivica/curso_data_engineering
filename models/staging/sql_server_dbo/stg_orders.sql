
{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

renamed_casted AS (
    SELECT
          order_id
        , DECODE(shipping_service, '', NULL, shipping_service) as shipping_service
        , shipping_cost as shipping_cost_usd
        , address_id
        , created_at as created_at_utc
        , DECODE(promo_id, '', {{dbt_utils.generate_surrogate_key(["'No Promo'"])}}, {{dbt_utils.generate_surrogate_key(['promo_id'])}}) as promo_id
        , estimated_delivery_at as estimated_delivery_at_utc
        , order_cost as item_order_cost_usd
        , user_id
        , order_total as total_order_cost_usd
        , delivered_at as delivered_at_utc
        , DECODE(tracking_id, '', NULL, {{dbt_utils.generate_surrogate_key(['tracking_id'])}}) as tracking_id
        , {{dbt_utils.generate_surrogate_key(['status'])}} as status_order
        , _fivetran_synced AS date_load
    FROM src_orders
    )

SELECT * FROM renamed_casted