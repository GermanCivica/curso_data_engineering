{{
    config(
        materialized='incremental',
        unique_key='_row',
        on_schema_change='fail'
    )
}}


WITH stg_budget_products AS (
    SELECT * 
    FROM {{ source('google_sheets','budget') }}
--{% if is_incremental() %}
--
--	  where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
--
--{% endif %}
    ),

renamed_casted AS (
    SELECT
          _row
        , product_id
        , quantity
        , month 
        , _fivetran_synced
    FROM stg_budget_products
    )

SELECT * FROM renamed_casted