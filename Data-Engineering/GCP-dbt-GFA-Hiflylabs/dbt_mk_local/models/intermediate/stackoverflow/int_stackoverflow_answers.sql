{{
    config(
        materialized='incremental'
    )
}}

WITH

source_stg_stackoverflow_answers AS (
    SELECT
        a._pk,
        a.id,
        a.title,
        a.accepted_answer_id,
        a.answer_count,
        a.comment_count,
        a.community_owned_datetime_utc,
        a.creation_datetime_utc,
        a.favorite_count,
        a.last_activity_datetime_utc,
        a.last_edit_datetime_utc,
        a.last_editor_display_name,
        a.last_editor_user_id,
        a.owner_display_name,
        a.owner_user_id,
        a.parent_id,
        a.post_type_id,
        a.score,
        a.tags,
        a.tags_array,
        a.view_count,
        b.tags_element
    FROM {{ ref('stg_stackoverflow_posts_answers') }} a
    LEFT JOIN
    {{ ref('int_stackoverflow_questions') }} b
    ON a.parent_id=b.id

    {% if is_incremental() %}
        where a.creation_datetime_utc >= (select max(a.creation_datetime_utc) from {{ this }})
    {% endif %}
),

source_int_google_sheet AS (
    SELECT
        repository_account,
        repository_name,
        organization AS organization_name,
        tags_element
    FROM {{ ref('int_google_sheet') }}
    WHERE dbt_valid_to IS NULL
),

final AS (
    SELECT
        a._pk,
        b.tags_element,
        b.repository_name,
        b.repository_account,
        b.organization_name,
        a.id,
        a.title,
        a.accepted_answer_id,
        a.answer_count,
        a.comment_count,
        a.community_owned_datetime_utc,
        a.creation_datetime_utc,
        a.favorite_count,
        a.last_activity_datetime_utc,
        a.last_edit_datetime_utc,
        a.last_editor_display_name,
        a.last_editor_user_id,
        a.owner_display_name,
        a.owner_user_id,
        a.parent_id,
        a.post_type_id,
        a.score,
        a.tags,
        a.tags_array,
        a.view_count
    FROM source_stg_stackoverflow_answers a
    RIGHT JOIN
    source_int_google_sheet b
    ON a.tags_element=b.tags_element
    WHERE a.tags_element=b.tags_element
)

SELECT *
FROM final