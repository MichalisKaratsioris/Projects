{{
    config(
        materialized='table'
    )
}}

WITH

source AS (
    SELECT *
    FROM {{ source('google_sheet_snapshot','google_snapshot') }}, UNNEST(split(tags,',')) AS tags_element
),

final AS (
    SELECT
    
        sha256(lower(Organization)) AS _pk,
        Tags AS tags,
        split(Tags, ',') AS tags_array,
        tags_element,
        L1_type AS l1_type,
        L2_type AS l2_type,
        L3_type AS l3_type,
        Organization AS organization,
        Repository_name AS repository_name,
        Repository_account AS repository_account,
        _airbyte_ab_id AS airbyte_ab_id,
        _airbyte_emitted_at AS creationtime_at_datetime_utc,
        _airbyte_normalized_at AS airbyte_normalized_at,
        _airbyte_airbyte_hashid AS airbyte_hashid,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to,
            CASE
                WHEN open_source_available='Yes' THEN true
                    ELSE false
            END AS is_open_source_available

    FROM source
)

SELECT *
FROM final