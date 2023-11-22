
{{
  config(
    materialized='view'
  )
}}

WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

renamed_casted AS (
    SELECT
          event_id
        , page_url
        , event_type
        , user_id
        , DECODE(product_id, '', NULL, product_id) as product_id
        , session_id
        , {{ dbt_date.convert_timezone("created_at", "America/Los_Angeles", "UTC") }} as created_at_utc
        , DECODE(order_id, '', NULL, order_id) as order_id
        , {{ dbt_date.convert_timezone("_fivetran_synced", "America/Los_Angeles", "UTC") }} as date_load_utc
    FROM src_events
    )

SELECT * FROM renamed_casted