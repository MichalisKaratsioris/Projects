{{
    config(
        materialized='incremental'
    )
}}

WITH

source_int_github_quarterly AS (
    SELECT
        -- month,
        quarter,
        organization_name,
        repository_account,
        repository_name,
        sum(event_count) as event_count,
        sum(user_count) as user_count,
        sum(issues_count) as issues_count,
        sum(watch_count) as watch_count,
        sum(fork_count) as fork_count,
        sum(push_count) as push_count,
        sum(pr_count) as pr_count,
        sum(delete_count) as delete_count,
        sum(public_count) as public_count,
        sum(create_count) as create_count,
        sum(gollum_count) as gollum_count,
        sum(member_count) as member_count,
        sum(commit_comment_count) as commit_comment_count,
        min(first_day_of_period) as first_day_of_period,
    FROM {{ ref('mart_github_monthly') }}
    GROUP BY quarter, repository_account, repository_name, organization_name
),

final as (
    SELECT 
        md5 ( concat (CAST(FLOOR(1000*RAND()) AS string), cast(quarter as string), organization_name, repository_account, repository_name, cast(user_count as string), cast(event_count as string), cast(issues_count as string), cast(watch_count as string), cast(fork_count as string), cast(push_count as string), cast(pr_count as string), cast(delete_count as string), cast(public_count as string), cast(create_count as string),cast(gollum_count as string), cast(member_count as string),cast(commit_comment_count as string) ) ) as _pk,
        first_day_of_period,
        quarter,
        {{ var('year_of_interest') }} AS year,
        organization_name,
        repository_account,
        repository_name,
        event_count,
        user_count,
        issues_count,
        watch_count,
        fork_count,
        push_count,
        pr_count,
        delete_count,
        public_count,
        create_count,
        gollum_count,
        member_count,
        commit_comment_count,
        (issues_count+ watch_count+ fork_count+ push_count+ pr_count+ delete_count+ public_count+ 
        create_count+ gollum_count+ member_count+ commit_comment_count) AS total_event_count,
    FROM source_int_github_quarterly
)

SELECT *
FROM final