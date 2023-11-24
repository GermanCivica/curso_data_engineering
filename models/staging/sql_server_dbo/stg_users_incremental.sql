
{{
  config(
    materialized='incremental',
    unique_key='user_id',
    on_schema_change='fail'
  )
}}

WITH src_users_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    --{% if is_incremental() %}
--
	--  where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
--
    --{% endif %}
    ),

renamed_casted AS (
    SELECT
          user_id
        , address_id
        , last_name
        , REPLACE(phone_number, '-', '')::int as phone_number
        , first_name
        , _fivetran_synced AS f_carga
    FROM src_users_orders
    )

SELECT * FROM renamed_casted