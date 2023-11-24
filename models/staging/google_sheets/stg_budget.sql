
{{
    config(
        materialized='incremental',
        unique_key='product_budget_id',
        on_schema_change='fail'
    )
}}

WITH stg_budget_products AS (
    SELECT * 
    FROM {{ source('google_sheets','budget') }}
{% if is_incremental() %}

	  where _fivetran_synced > (select max(date_load_utc) from {{ this }})

{% endif %}
    ),

renamed_casted AS (
    SELECT
          {{dbt_utils.generate_surrogate_key(['_row'])}} as product_budget_id
        , product_id
        , quantity 
        , month
        , _fivetran_synced as date_load_utc
    FROM stg_budget_products
    )

SELECT * FROM renamed_casted