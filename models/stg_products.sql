
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
        , _fivetran_synced AS date_load
    FROM src_products_orders
    )

SELECT * FROM renamed_casted