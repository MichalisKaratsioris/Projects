{{
    config(
        materialized='table'
    )
}}

WITH 

source_int_stackoverflow_questions_daily AS (
    SELECT 
        DATE(EXTRACT(YEAR FROM a.creation_datetime_utc), EXTRACT(MONTH FROM a.creation_datetime_utc), EXTRACT(DAY FROM a.creation_datetime_utc)) as first_day_of_period,
        EXTRACT(MONTH FROM a.creation_datetime_utc) as month,
        {{ var('year_of_interest') }} AS year,
        a.organization_name,
        count(a.id) as post_count,
        sum(a.answer_count) as answer_count,
        ROUND(sum(a.answer_count)/count(a.answer_count),3) as avg_answer_count,
        sum(a.comment_count) as comment_count,
        ROUND(sum(a.comment_count)/count(a.comment_count),3) as avg_comment_count,
        sum(a.favorite_count) as favorite_count,
        ROUND(sum(a.favorite_count)/count(a.favorite_count),3) as avg_favorite_count,
        sum(a.view_count) as view_count,
        ROUND(sum(a.view_count)/count(a.view_count),3) as avg_view_count,
        ROUND(sum(case when a.accepted_answer_id is not null then 1 end)/count(a.id)) as accepted_answer_count,
        sum(case
            when a.answer_count = 0 then 1
        end) as no_answer_count,
        ROUND(sum(case when a.answer_count = 0 then 1 end)/count(a.answer_count)) as avg_no_answer_count,
        sum(a.score)/count(a.id) as score,
        max(DATE(EXTRACT(YEAR FROM a.last_activity_datetime_utc), EXTRACT(MONTH FROM a.last_activity_datetime_utc), EXTRACT(DAY FROM a.last_activity_datetime_utc))) as last_activity_datetime_utc,
        max(DATE(EXTRACT(YEAR FROM a.last_edit_datetime_utc), EXTRACT(MONTH FROM a.last_edit_datetime_utc), EXTRACT(DAY FROM a.last_edit_datetime_utc))) as last_edit_datetime_utc
  
    FROM {{ ref('int_stackoverflow_questions') }} a
    LEFT JOIN
    {{ ref('int_google_sheet') }} b
    ON a.organization_name=b.organization
    GROUP BY DATE(EXTRACT(YEAR FROM creation_datetime_utc), EXTRACT(MONTH FROM creation_datetime_utc), EXTRACT(DAY FROM creation_datetime_utc)), EXTRACT(MONTH FROM creation_datetime_utc), organization_name
),

stackoverflow_tags AS (
    SELECT organization_name, tag
    FROM {{ ref('int_stackoverflow_questions') }}, UNNEST(SPLIT(tags, '|')) as tag
),

company_tags AS (
    SELECT organization, tag
    FROM {{ ref('int_google_sheet') }}, UNNEST(SPLIT(tags, ',')) as tag
),

tags_final as (
    SELECT stackoverflow_tags.organization_name,
    COUNT(stackoverflow_tags.tag) - COUNT(company_tags.tag) as tags_difference
    FROM stackoverflow_tags
    LEFT JOIN company_tags
    ON stackoverflow_tags.organization_name = company_tags.organization
    AND stackoverflow_tags.tag = company_tags.tag
    GROUP BY stackoverflow_tags.organization_name
),

final as (
    SELECT
        md5(concat (CAST(FLOOR(1000*RAND()) AS string), cast(a.first_day_of_period as string), cast(a.month as string), a.organization_name, cast(a.post_count as string), cast(a.answer_count as string), cast(a.comment_count as string), cast(b.tags_difference as string))) as _pk,
        a.first_day_of_period,
        a.month,
         EXTRACT(QUARTER FROM a.first_day_of_period) as quarter,
        a.year,
        a.organization_name,
        a.post_count,
        a.answer_count,
        a.avg_answer_count,
        a.comment_count,
        a.avg_comment_count,
        a.favorite_count,
        a.avg_favorite_count,
        a.view_count,
        a.avg_view_count,
        a.accepted_answer_count,
        a.no_answer_count,
        a.avg_no_answer_count,
        a.score,
        b.tags_difference as tags_count,
        a.last_activity_datetime_utc,
        a.last_edit_datetime_utc
    FROM source_int_stackoverflow_questions_daily a
    LEFT JOIN tags_final b
    ON a.organization_name=b.organization_name
    WHERE a.organization_name=b.organization_name
)

SELECT *
FROM final
