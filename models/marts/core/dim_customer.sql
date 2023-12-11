
{{
  config(
    materialized='incremental',
    unique_key='user_id',
    on_schema_change='fail'
  )
}}

WITH stg_users AS (
    SELECT * 
    FROM {{ ref('stg_users') }}
{% if is_incremental() %}

	  where date_load_utc > (select max(date_load_utc) from {{ this }})

{% endif %}
    ),

renamed_casted AS (
    SELECT
        user_id
        , first_name
        , last_name
        , email
        , phone_number
        , created_at_utc
        , updated_at_utc
        , address_id
        , date_load_utc
    FROM stg_users
    )

SELECT * FROM renamed_casted