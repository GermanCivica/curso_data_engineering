
{{
    config(
        materialized='incremental',
        unique_key='address_id',
        on_schema_change='fail'
    )
}}

WITH src_addresses_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
{% if is_incremental() %}

	  where _fivetran_synced > (select max(date_load_utc) from {{ this }})

{% endif %}
),

renamed_casted AS (
    SELECT
          address_id
        , zipcode
        , country
        , address
        , state
        , {{ dbt_date.convert_timezone("_fivetran_synced", "America/Los_Angeles", "UTC") }} AS date_load_utc
    FROM src_addresses_orders
    )

SELECT * FROM renamed_casted