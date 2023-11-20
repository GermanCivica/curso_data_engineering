
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
        , _fivetran_synced AS date_load
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