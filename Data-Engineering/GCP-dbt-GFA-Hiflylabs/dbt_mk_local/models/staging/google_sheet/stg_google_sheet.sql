with

source as (
    select *
    from {{ source('google_sheet','companies_data') }}
),

final as (
    select
    
        sha256(lower(Organization)) as _pk,
        Tags as tags,
        split(Tags, ',') as tags_array,
        L1_type as l1_type,
        L2_type as l2_type,
        L3_type as l3_type,
        Organization as organization,
        Repository_name as repository_name,
        Repository_account as repository_account,
        _airbyte_emitted_at as creationtime_at_datetime_utc,
            case
                when open_source_available='Yes' then true
                    else false
            end as is_open_source_available

    from source
)

select *
from final