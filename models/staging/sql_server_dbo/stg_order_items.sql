 
{{
    config(
        materialized='incremental',
    )
}}

WITH src_order_items AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'order_items') }}
{% if is_incremental() %}

	  where _fivetran_synced > (select max(date_load_utc) from {{ this }})

{% endif %}
),

renamed_casted AS (
    SELECT
          order_id
        , product_id
        , quantity
        , {{ dbt_date.convert_timezone("_fivetran_synced", "America/Los_Angeles", "UTC") }} AS date_load_utc
    FROM src_order_items
    )

SELECT * FROM renamed_casted