{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key='customer_id',
      check_cols=[
          'natural_key',
          'first_name',
          'last_name',
          'email',
          'phone',
          'address_line1',
          'address_line2',
          'city',
          'state_region',
          'postcode',
          'country_code',
          'latitude',
          'longitude',
          'is_vip',
          'gdpr_consent'
      ],
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}