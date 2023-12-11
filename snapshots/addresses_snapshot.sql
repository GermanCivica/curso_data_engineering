{% snapshot addresses_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='address_id',
      strategy='timestamp',
      updated_at='_fivetran_synced',
    )
}}

select * from {{ source('sql_server_dbo', 'addresses') }}

{% endsnapshot %}