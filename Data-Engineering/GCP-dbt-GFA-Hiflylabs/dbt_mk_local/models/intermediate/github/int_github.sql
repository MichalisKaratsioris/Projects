{{
    config(
        materialized='incremental'
    )
}}

WITH

source_stg_github AS (
    SELECT
        _pk,
        split(repo_name,'/')[offset(0)] as repository_account,
        split(repo_name,'/')[offset(1)] as repository_name,
        type,
        actor_id AS user_id,
        created_at_datetime_utc,
        id AS event_id
    FROM {{ ref('stg_github') }}

    {% if is_incremental() %}
        where created_at_datetime_utc >= (select max(created_at_datetime_utc) from {{ this }})
    {% endif %}
),

source_int_google_sheet AS (
    SELECT
        repository_account,
        repository_name,
        organization AS organization_name
    FROM {{ ref('int_google_sheet') }}
    WHERE dbt_valid_to IS NULL
    
),

final AS (
    SELECT
        DISTINCT a._pk,
        b.repository_name,
        b.repository_account,
        b.organization_name,
        a.type,
        a.user_id,
        a.created_at_datetime_utc,
        a.event_id,
    FROM source_stg_github a
    RIGHT JOIN
    source_int_google_sheet b
    ON a.repository_account=b.repository_account AND (b.repository_name = a.repository_name or b.repository_name is null)
    WHERE b.repository_account IS NOT NULL and _pk IS NOT NULL
)

SELECT *
FROM final