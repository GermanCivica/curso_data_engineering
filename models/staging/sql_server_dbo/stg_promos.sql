
{{
  config(
    materialized='view'
  )
}}

WITH src_promos_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

renamed_casted AS (
    SELECT
          promo_id
        , discount
        , status
        , _fivetran_synced AS date_load
    FROM src_promos_orders
    )

SELECT * FROM renamed_casted