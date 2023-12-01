{{
  config(
    materialized='table'
  )
}}

WITH int_user_orders AS (
    SELECT * 
    FROM {{ ref('int_user_orders') }}
),

stg_orders AS (
    
)