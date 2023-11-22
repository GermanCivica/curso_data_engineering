
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
        , {{dbt_utils.generate_surrogate_key(['shipping_service'])}} as shipping_service
        , shipping_cost as shipping_cost_usd
        , address_id
        , {{ dbt_date.convert_timezone("created_at", "America/Los_Angeles", "UTC") }} as created_at_utc
        , DECODE(promo_id, '', {{dbt_utils.generate_surrogate_key(["'No Promo'"])}}, {{dbt_utils.generate_surrogate_key(['promo_id'])}}) as promo_id
        , {{ dbt_date.convert_timezone(" estimated_delivery_at", "America/Los_Angeles", "UTC") }} as estimated_delivery_at_utc
        , order_cost as item_order_cost_usd
        , user_id
        , order_total as total_order_cost_usd
        , {{ dbt_date.convert_timezone("delivered_at", "America/Los_Angeles", "UTC") }} as delivered_at_utc
        , {{dbt_utils.generate_surrogate_key(['tracking_id'])}} as tracking_id
        , {{dbt_utils.generate_surrogate_key(['status'])}} as status_order
        , {{ dbt_date.convert_timezone("_fivetran_synced", "America/Los_Angeles", "UTC") }} as date_load_utc
    FROM src_orders
    )

SELECT * FROM renamed_casted