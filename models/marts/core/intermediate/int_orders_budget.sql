{{
    config (
      materialized='view'
    )
}}

WITH stg_order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
),

stg_budget AS (
    SELECT *
    FROM {{ ref('stg_budget') }}
),

total_products_ordered AS (
    SELECT *
    FROM (SELECT
                product_id
              , SUM(quantity) OVER (PARTITION BY product_id) AS total_quantity_ordered
         FROM stg_order_items)
    GROUP BY product_id, total_quantity_ordered
),

renamed_casted AS (
    SELECT
          {{dbt_utils.generate_surrogate_key(['stg_budget.product_budget_id', 'total_products_ordered.product_id'])}} as orders_budget_id
        , {{ dbt_date.date_part("month", "stg_budget.month") }} AS month
        , {{ dbt_date.date_part("year", "stg_budget.month") }} AS year
        , stg_budget.product_id
        , total_products_ordered.total_quantity_ordered
        , stg_budget.quantity
        , CASE
              WHEN total_products_ordered.total_quantity_ordered > stg_budget.quantity THEN True
              ELSE False 
              END AS over_budget
    FROM stg_budget
    JOIN total_products_ordered ON stg_budget.product_id = total_products_ordered.product_id
)

SELECT * FROM renamed_casted