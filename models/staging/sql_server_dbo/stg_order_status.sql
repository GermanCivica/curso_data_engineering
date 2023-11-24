
{{
    config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT DISTINCT(status)
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

renamed_casted AS (
    SELECT 
          {{dbt_utils.generate_surrogate_key(['status'])}} AS status_order_id
        , status as status_order_descr
    FROM src_orders
)

SELECT * FROM renamed_casted