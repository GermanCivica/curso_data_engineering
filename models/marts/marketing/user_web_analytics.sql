
{{
  config(
    materialized='table'
  )
}}

WITH int_user_events AS (
    SELECT *
    FROM {{ ref('int_user_events') }}
),

stg_events AS (
    SELECT *
    FROM {{ ref('stg_events') }}
),

int_user_orders AS (
    SELECT *
    FROM {{ ref('int_user_orders' )}}
),

user_sessions AS (
    SELECT *
    FROM (SELECT 
              user_id
            , COUNT(session_id) OVER (PARTITION BY user_id) AS total_user_sessions
        FROM (SELECT DISTINCT user_id, session_id
              FROM stg_events))
    GROUP BY user_id, total_user_sessions
),

user_visited_products AS (
    SELECT *
    FROM (SELECT 
              user_id
            , session_id
            , SUM(CASE WHEN event_type='page_view' THEN 1 END) OVER (PARTITION BY session_id, user_id) AS page_viewed_per_session
            , SUM(CASE WHEN event_type='add_to_cart' THEN 1 END) OVER (PARTITION BY session_id, user_id) AS product_purch_per_session
        FROM stg_events)
    GROUP BY session_id, user_id, page_viewed_per_session, product_purch_per_session
),

user_web_analytics AS (
    SELECT
          user_sessions.user_id
        , total_user_sessions
        , CAST(checkout_amount/total_user_sessions*100 AS DECIMAL(18,2)) AS perc_session_with_purchases
        , CAST(page_view_amount/total_user_sessions AS DECIMAL(18,2)) AS mean_products_viewed_per_session
        , CAST(add_to_cart_amount/page_view_amount*100 AS DECIMAL(18,2)) AS perc_products_purchased_after_visit
        , CAST(user_total_expenditure_usd/total_user_sessions AS DECIMAL(18,2)) AS mean_expenditure_per_session_usd
    FROM user_sessions
    JOIN int_user_events ON user_sessions.user_id = int_user_events.user_id
    JOIN int_user_orders ON user_sessions.user_id = int_user_orders.user_id
)

SELECT * FROM user_web_analytics