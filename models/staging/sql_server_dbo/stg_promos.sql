
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
          {{dbt_utils.generate_surrogate_key(['promo_id'])}} as promo_id
        , promo_id as promo_name
        , discount
        , status
        , {{ dbt_date.convert_timezone("_fivetran_synced", "America/Los_Angeles", "UTC") }} AS date_load_utc
    FROM src_promos_orders
    )

SELECT * FROM renamed_casted
UNION ALL
SELECT
      {{dbt_utils.generate_surrogate_key(["'No Promo'"])}} as promo_id
    , 'No Promo' as promo_name
    , 0 as discount
    , 'active' as status 
    , {{dbt_date.now("America/Los_Angeles")}} as date_load