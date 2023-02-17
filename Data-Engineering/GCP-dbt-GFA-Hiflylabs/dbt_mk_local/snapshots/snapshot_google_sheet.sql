{% snapshot google_sheet_snapshot_timestamp_local %}

    {{
        config(
          target_database='gcp-project-2023',
          target_schema='snapshots',
          strategy='check',
          unique_key='Organization',
          check_cols=['Organization','Repository_name','Repository_account','Tags','L1_type','L2_type','L3_type','Open_source_available'],
          invalidate_hard_deletes=True,
        )
    }}

    select *
    from {{ source('google_sheet','companies_data') }}

{% endsnapshot %}