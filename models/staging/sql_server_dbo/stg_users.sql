
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
{% if is_incremental() %}

	  where _fivetran_synced > (select max(date_load_utc) from {{ this }})

{% endif %}
),

renamed_casted AS (
    SELECT
          user_id
        , {{ dbt_date.convert_timezone("updated_at", "America/Los_Angeles", "UTC") }} as updated_at_utc
        , address_id
        , last_name
        , {{ dbt_date.convert_timezone("created_at", "America/Los_Angeles", "UTC") }} as created_at_utc
        , phone_number
        , total_orders
        , first_name
        , email
        , {{ dbt_date.convert_timezone("_fivetran_synced", "America/Los_Angeles", "UTC") }} AS date_load_utc
    FROM src_users_orders
    )

SELECT * FROM renamed_casted