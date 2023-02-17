{{
    config(
        materialized='table'
    )
}}

WITH

source_int_stackoverflow_questions_quarterly AS (
    SELECT 
        case
            when quarter = 1 then TIMESTAMP(DATE(concat(CAST(EXTRACT(YEAR FROM first_day_of_period) as string),'-','01','-','01')))
            when quarter = 2 then TIMESTAMP(DATE(concat(CAST(EXTRACT(YEAR FROM first_day_of_period) as string),'-','04','-','01')))
            when quarter = 3 then TIMESTAMP(DATE(concat(CAST(EXTRACT(YEAR FROM first_day_of_period) as string),'-','07','-','01')))
            else TIMESTAMP(DATE(concat(CAST(EXTRACT(YEAR FROM first_day_of_period) as string),'-','10','-','01')))
        end as first_day_of_period,
        quarter,
        organization_name,
        count(post_count) as post_count,
        sum(answer_count) as answer_count,
        ROUND(sum(answer_count)/count(answer_count),3) as avg_answer_count,
        sum(comment_count) as comment_count,
        ROUND(sum(comment_count)/count(comment_count),3) as avg_comment_count,
        sum(favorite_count) as favorite_count,
        ROUND(sum(favorite_count)/count(favorite_count),3) as avg_favorite_count,
        sum(view_count) as view_count,
        ROUND(sum(view_count)/count(view_count),3) as avg_view_count,
        sum(accepted_answer_count) as accepted_answer_count,
        sum(no_answer_count) as no_answer_count,
        ROUND(sum(no_answer_count)/count(no_answer_count),3) as avg_no_answer_count,
        ROUND(sum(score)/count(score)) as score,
        sum(tags_count) as tags_count,
        max(last_activity_datetime_utc) as last_activity_datetime_utc,
        max(last_edit_datetime_utc) as last_edit_datetime_utc

    FROM {{ ref('mart_stackoverflow_questions_monthly') }} 
    GROUP BY quarter, organization_name, first_day_of_period
),

final as (
    SELECT
        md5(concat (CAST(FLOOR(1000*RAND()) AS string), cast(first_day_of_period as string), cast(quarter as string), organization_name, cast(post_count as string), cast(answer_count as string), cast(comment_count as string), cast(tags_count as string))) as _pk,
        first_day_of_period,
        EXTRACT(MONTH FROM first_day_of_period) as month,
        quarter,
        {{ var('year_of_interest') }} as year,
        organization_name,
        post_count,
        answer_count,
        avg_answer_count,
        comment_count,
        avg_comment_count,
        favorite_count,
        avg_favorite_count,
        view_count,
        avg_view_count,
        accepted_answer_count,
        no_answer_count,
        avg_no_answer_count,
        score,
        tags_count,
        last_activity_datetime_utc,
        last_edit_datetime_utc
    FROM source_int_stackoverflow_questions_quarterly
)

SELECT *
FROM final