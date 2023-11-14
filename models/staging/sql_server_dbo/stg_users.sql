
{{
  config(
    materialized='view'
  )
}}

WITH src_users_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

renamed_casted AS (
    SELECT
          user_id
        , updated_at as updated_at_utc
        , address_id
        , last_name
        , created_at as created_at_utc
        , phone_number
        , total_orders
        , first_name
        , email
        , _fivetran_synced AS date_load
    FROM src_users_orders
    )

SELECT * FROM renamed_casted