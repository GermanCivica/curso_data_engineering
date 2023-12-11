{{
  config(
    materialized='table'
  )
}}

WITH stg_events AS (
    SELECT *
    FROM {{ ref('stg_events') }}
),

stg_users AS (
    SELECT *
    FROM {{ ref('stg_users') }}
),

order_events AS (
    SELECT 
         session_id
        , event_id
        , event_type
        , user_id
        , product_id
        , order_id
        , created_at_utc
        , first_event
        , last_event
    FROM (SELECT
                  session_id
                , event_id
                , event_type
                , user_id
                , product_id
                , order_id
                , created_at_utc
                , FIRST_VALUE(created_at_utc) OVER (PARTITION BY session_id ORDER BY created_at_utc) AS first_event
                , LAST_VALUE(created_at_utc) OVER (PARTITION BY session_id ORDER BY created_at_utc) AS last_event
          FROM {{ ref('stg_events') }}
          )
),

order_events_user AS (
    SELECT 
          session_id
        , event_type
        , order_events.user_id
        , stg_users.first_name
        , stg_users.last_name
        , stg_users.phone_number
        , stg_users.email
        , product_id
        , order_events.first_event
        , order_events.last_event
    FROM order_events
    JOIN stg_users ON stg_users.user_id = order_events.user_id
),

renamed_casted AS (
    SELECT
          session_id
        , user_id
        , first_name
        , email
        , first_event as first_event_time_utc
        , last_event as last_event_time_utc
        , DATEDIFF(minute, first_event, last_event) as session_length_minutes
        , SUM(case event_type when 'page_view' then 1 else 0 end) as page_view_events
        , SUM(case event_type when 'add_to_cart' then 1 else 0 end) as add_to_cart_events
        , SUM(case event_type when 'checkout' then 1 else 0 end) as checkout_events
        , SUM(case event_type when 'package_shipped' then 1 else 0 end) as package_shipped_events
    FROM order_events_user
    GROUP BY session_id, user_id, first_name, last_name, email, first_event_time_utc, last_event_time_utc
)

SELECT * FROM renamed_casted