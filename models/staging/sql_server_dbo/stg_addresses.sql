
{{
  config(
    materialized='view'
  )
}}

WITH src_addresses_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

renamed_casted AS (
    SELECT
          address_id
        , zipcode
        , country
        , address
        , state
        , _fivetran_synced AS date_load
    FROM src_addresses_orders
    )

SELECT * FROM renamed_casted