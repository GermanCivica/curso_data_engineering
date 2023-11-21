{% snapshot users_incremental_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_id',
      strategy='check',
      check_cols=['first_name', 'last_name', 'address_id', 'phone_number'],
        )
}}

select * from {{ ref('stg_users_incremental') }}

{% endsnapshot %}