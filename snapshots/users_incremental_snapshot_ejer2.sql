{% snapshot users_incremental_snapshot_ejer2 %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_id',
      strategy='check',
      check_cols=['first_name', 'last_name', 'address_id', 'phone_number'],
        )
}}

select * from {{ ref('stg_users_incremental_ejer2') }}
where f_carga = (select max(f_carga) from {{ ref('stg_users_incremental_ejer2') }})

{% endsnapshot %}