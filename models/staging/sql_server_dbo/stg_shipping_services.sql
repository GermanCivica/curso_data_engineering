
{{
    config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT DISTINCT(shipping_service)
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

renamed_casted AS (
    SELECT 
           {{dbt_utils.generate_surrogate_key(['shipping_service'])}} AS shipping_service_id
         , DECODE(shipping_service, '', 'Not Shipped', shipping_service) as shipping_service_name
    FROM src_orders
)

SELECT * FROM renamed_casted
