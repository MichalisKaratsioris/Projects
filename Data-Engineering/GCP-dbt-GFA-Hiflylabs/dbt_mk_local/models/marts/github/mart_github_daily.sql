{{
    config(
        materialized='table'
    )
}}

{% set type_events = {
    "issues": "IssuesEvent",
    "watch": "WatchEvent",
    "fork": "ForkEvent",
    "push": "PushEvent",
    "pr": "PushRequestEvent",
    "delete": "DeleteEvent",
    "public": "PublicEvent",
    "create": "CreateEvent",
    "gollum": "GollumEvent",
    "member": "MemberEvent",
    "commit_comment": "CommitCommentEvent",
    }
%}

WITH

source_int_github_daily AS (
    SELECT 
        repository_account,
        repository_name,
        organization_name,
        DATE(EXTRACT(YEAR FROM created_at_datetime_utc), EXTRACT(MONTH FROM created_at_datetime_utc), EXTRACT(DAY FROM created_at_datetime_utc)) as first_day_of_period,
        EXTRACT(MONTH FROM created_at_datetime_utc) as month,
        {% for key, value in type_events.items() -%}
        sum(case when type = "{{ value }}" then 1 else 0 end) as {{ key }}_count,
        {% endfor %}    
        {{ var('year_of_interest') }} AS year,
        count(DISTINCT user_id) as user_count,
        count(DISTINCT event_id) as event_count,
    FROM {{ ref('int_github') }}
    GROUP BY repository_account, repository_name, DATE(EXTRACT(YEAR FROM created_at_datetime_utc), EXTRACT(MONTH FROM created_at_datetime_utc), EXTRACT(DAY FROM created_at_datetime_utc)), EXTRACT(MONTH FROM created_at_datetime_utc), organization_name
),

final as (
    SELECT
        md5 ( concat (CAST(FLOOR(1000*RAND()) AS string), cast(first_day_of_period as string), cast(month as string), organization_name, repository_account, repository_name, cast(user_count as string), cast(event_count as string), cast(issues_count as string), cast(watch_count as string), cast(fork_count as string), cast(push_count as string), cast(pr_count as string), cast(delete_count as string), cast(public_count as string), cast(create_count as string),cast(gollum_count as string), cast(member_count as string),cast(commit_comment_count as string) ) ) as _pk,
        first_day_of_period,
        month,
        case
            when month < 4 then 1
            when month < 7 then 2
            when month < 10 then 3
            else 4
        end as quarter,
        year,
        organization_name,
        repository_account,
        case
            when repository_name is null then repository_account
            else repository_name
        end as repository_name,
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

    FROM source_int_github_daily
)

SELECT *
FROM final
ORDER BY first_day_of_period, month, organization_name, repository_account, repository_name