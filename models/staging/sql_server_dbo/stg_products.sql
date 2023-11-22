
{{
  config(
    materialized='view'
  )
}}

WITH src_products_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
    ),

renamed_casted AS (
    SELECT
          product_id
        , price
        , name
        , inventory
        , {{ dbt_date.convert_timezone("_fivetran_synced", "America/Los_Angeles", "UTC") }} AS date_load_utc
    FROM src_products_orders
    )

SELECT * FROM renamed_casted