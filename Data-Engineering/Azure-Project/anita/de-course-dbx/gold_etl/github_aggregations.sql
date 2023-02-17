-- Databricks notebook source
select type
from silver_db.s_github
group by type;

-- COMMAND ----------

--Name of the repository. It should be the repository account if the repository name is empty in Company Details
--Count of all the events. Sum of all the above events



--As for repositories, the level of aggregation is the Company Details table’s organization name

-- COMMAND ----------

--create table gold_db.g_github_daily as 
select comp._pk,
    CAST(date_trunc('day', git.created_at_datetime_utc) AS DATE) as first_day_of_period,
    MONTH(date_trunc('day', git.created_at_datetime_utc)) as month,
    QUARTER(date_trunc('day', git.created_at_datetime_utc)) as quarter,
    YEAR(date_trunc('day', git.created_at_datetime_utc)) as year,
    comp.Organization as organization_name,
    git.repository_account as repository_account,
    (case when (git.repository_name = '') or (git.repository_name is null) then git.repository_account else git.repository_name end) as repository_name,
    COUNT(DISTINCT git.event_id) as event_count,
    COUNT(DISTINCT git.user_id) as user_count,
    SUM(case when git.type = 'IssueCommentEvent' then 1 else 0 end) as issues_count,
    SUM(case when git.type = 'WatchEvent' then 1 else 0 end) as watch_count,
    SUM(case when git.type = 'ForkEvent' then 1 else 0 end) as fork_count,
    SUM(case when git.type = 'PushEvent' then 1 else 0 end) as push_count,
    SUM(case when git.type = 'PullRequestEvent' then 1 else 0 end) as pr_count,
    SUM(case when git.type = 'DeleteEvent' then 1 else 0 end) as delete_count,
    SUM(case when git.type = 'PublicEvent' then 1 else 0 end) as public_count,
    SUM(case when git.type = 'CreateEvent' then 1 else 0 end) as create_count,
    SUM(case when git.type = 'GollumEvent' then 1 else 0 end) as gollum_count,
    SUM(case when git.type = 'MemberEvent' then 1 else 0 end) as member_count,
    SUM(case when git.type = 'CommitCommentEvent' then 1 else 0 end) as commit_comment_count,
    (SUM(case when git.type = 'IssueCommentEvent' then 1 else 0 end) + SUM(case when git.type = 'WatchEvent' then 1 else 0 end) + 
      SUM(case when git.type = 'ForkEvent' then 1 else 0 end)+SUM(case when git.type = 'PushEvent' then 1 else 0 end)
      +SUM(case when git.type = 'PullRequestEvent' then 1 else 0 end)+
      SUM(case when git.type = 'DeleteEvent' then 1 else 0 end)+SUM(case when git.type = 'PublicEvent' then 1 else 0 end)
      +SUM(case when git.type = 'CreateEvent' then 1 else 0 end)+SUM(case when git.type = 'GollumEvent' then 1 else 0 end)
      +SUM(case when git.type = 'MemberEvent' then 1 else 0 end)+SUM(case when git.type = 'CommitCommentEvent' then 1 else 0 end)) as total_event_count
from silver_db.s_github AS git
INNER JOIN silver_db.s_company_detail AS comp
            ON (git.repository_account = comp.Repository_account AND git.repository_name = comp.Repository_name and comp.is_current = True)
            or (git.repository_account = comp.Repository_account AND comp.Repository_name = "" and comp.is_current = True)
group by date_trunc('day', git.created_at_datetime_utc), git.repository_account, git.repository_name, comp._pk, comp.Organization
order by first_day_of_period;

-- COMMAND ----------

--create table gold_db.g_github_monthly as 
select comp._pk,
    CAST(date_trunc('month', git.created_at_datetime_utc) AS DATE) as first_day_of_period,
    MONTH(date_trunc('month', git.created_at_datetime_utc)) as month,
    QUARTER(date_trunc('month', git.created_at_datetime_utc)) as quarter,
    YEAR(date_trunc('month', git.created_at_datetime_utc)) as year,
    comp.Organization,
    git.repository_account,
    (case when (git.repository_name = '') or (git.repository_name is null) then git.repository_account else git.repository_name end) as repository_name,
    COUNT(DISTINCT git.event_id) as event_count,
    COUNT(DISTINCT git.user_id) as user_count,
    COUNT(case when git.type = 'IssueCommentEvent' then 1 else null end) as issues_count,
    COUNT(case when git.type = 'WatchEvent' then 1 else null end) as watch_count,
    COUNT(case when git.type = 'ForkEvent' then 1 else null end) as fork_count,
    COUNT(case when git.type = 'PushEvent' then 1 else null end) as push_count,
    COUNT(case when git.type = 'PullRequestEvent' then 1 else null end) as pr_count,
    COUNT(case when git.type = 'DeleteEvent' then 1 else null end) as delete_count,
    COUNT(case when git.type = 'PublicEvent' then 1 else null end) as public_count,
    COUNT(case when git.type = 'CreateEvent' then 1 else null end) as create_count,
    COUNT(case when git.type = 'GollumEvent' then 1 else null end) as gollum_count,
    COUNT(case when git.type = 'MemberEvent' then 1 else null end) as member_count,
    COUNT(case when git.type = 'CommitCommentEvent' then 1 else null end) as commit_comment_count,
    (SUM(case when git.type = 'IssueCommentEvent' then 1 else 0 end) + SUM(case when git.type = 'WatchEvent' then 1 else 0 end) + 
      SUM(case when git.type = 'ForkEvent' then 1 else 0 end)+SUM(case when git.type = 'PushEvent' then 1 else 0 end)
      +SUM(case when git.type = 'PullRequestEvent' then 1 else 0 end)+
      SUM(case when git.type = 'DeleteEvent' then 1 else 0 end)+SUM(case when git.type = 'PublicEvent' then 1 else 0 end)
      +SUM(case when git.type = 'CreateEvent' then 1 else 0 end)+SUM(case when git.type = 'GollumEvent' then 1 else 0 end)
      +SUM(case when git.type = 'MemberEvent' then 1 else 0 end)+SUM(case when git.type = 'CommitCommentEvent' then 1 else 0 end)) as total_event_count
from silver_db.s_github AS git
INNER JOIN silver_db.s_company_detail AS comp
           ON (git.repository_account = comp.Repository_account AND git.repository_name = comp.Repository_name and comp.is_current = True)
            or (git.repository_account = comp.Repository_account AND comp.Repository_name = "" and comp.is_current = True)
group by date_trunc('month', git.created_at_datetime_utc), git.repository_account, git.repository_name, comp._pk, comp.Organization;

-- COMMAND ----------

--create table gold_db.g_github_quarterly as 
select comp._pk,
    CAST(date_trunc('quarter', git.created_at_datetime_utc) AS DATE) as first_day_of_period,
    MONTH(date_trunc('quarter', git.created_at_datetime_utc)) as month,
    QUARTER(date_trunc('quarter', git.created_at_datetime_utc)) as quarter,
    YEAR(date_trunc('quarter', git.created_at_datetime_utc)) as year,
    comp.Organization,
    git.repository_account,
    (case when (git.repository_name = '') or (git.repository_name is null) then git.repository_account else git.repository_name end) as repository_name,
    COUNT(DISTINCT git.event_id) as event_count,
    COUNT(DISTINCT git.user_id) as user_count,
    COUNT(case git.type when 'IssueCommentEvent' then 1 else null end) as issues_count,
    COUNT(case git.type when 'WatchEvent' then 1 else null end) as watch_count,
    COUNT(case git.type when 'ForkEvent' then 1 else null end) as fork_count,
    COUNT(case git.type when 'PushEvent' then 1 else null end) as push_count,
    COUNT(case git.type when 'PullRequestEvent' then 1 else null end) as pr_count,
    COUNT(case git.type when 'DeleteEvent' then 1 else null end) as delete_count,
    COUNT(case git.type when 'PublicEvent' then 1 else null end) as public_count,
    COUNT(case git.type when 'CreateEvent' then 1 else null end) as create_count,
    COUNT(case git.type when 'GollumEvent' then 1 else null end) as gollum_count,
    COUNT(case git.type when 'MemberEvent' then 1 else null end) as member_count,
    COUNT(case git.type when 'CommitCommentEvent' then 1 else null end) as commit_comment_count,
    (SUM(case when git.type = 'IssueCommentEvent' then 1 else 0 end) + SUM(case when git.type = 'WatchEvent' then 1 else 0 end) + 
      SUM(case when git.type = 'ForkEvent' then 1 else 0 end)+SUM(case when git.type = 'PushEvent' then 1 else 0 end)
      +SUM(case when git.type = 'PullRequestEvent' then 1 else 0 end)+
      SUM(case when git.type = 'DeleteEvent' then 1 else 0 end)+SUM(case when git.type = 'PublicEvent' then 1 else 0 end)
      +SUM(case when git.type = 'CreateEvent' then 1 else 0 end)+SUM(case when git.type = 'GollumEvent' then 1 else 0 end)
      +SUM(case when git.type = 'MemberEvent' then 1 else 0 end)+SUM(case when git.type = 'CommitCommentEvent' then 1 else 0 end)) as total_event_count
from silver_db.s_github AS git
INNER JOIN silver_db.s_company_detail AS comp
            ON (git.repository_account = comp.Repository_account AND git.repository_name = comp.Repository_name and comp.is_current = True)
            or (git.repository_account = comp.Repository_account AND comp.Repository_name = "" and comp.is_current = True)
group by date_trunc('quarter', git.created_at_datetime_utc), git.repository_account, git.repository_name, comp._pk, comp.Organization;

-- COMMAND ----------

---test

select comp._pk,
    CAST(date_trunc('quarter', git.created_at_datetime_utc) AS DATE) as first_day_of_period,
    MONTH(date_trunc('quarter', git.created_at_datetime_utc)) as month,
    QUARTER(date_trunc('quarter', git.created_at_datetime_utc)) as quarter,
    YEAR(date_trunc('quarter', git.created_at_datetime_utc)) as year,
    comp.Organization,
    git.repository_account,
    (case when (git.repository_name = '') or (git.repository_name is null) then git.repository_account else git.repository_name end) as repository_name,
    COUNT(DISTINCT git.event_id) as event_count,
    COUNT(DISTINCT git.user_id) as user_count,
    COUNT(case git.type when 'IssueCommentEvent' then 1 else null end) as issues_count,
    COUNT(case git.type when 'WatchEvent' then 1 else null end) as watch_count,
    COUNT(case git.type when 'ForkEvent' then 1 else null end) as fork_count,
    COUNT(case git.type when 'PushEvent' then 1 else null end) as push_count,
    COUNT(case git.type when 'PullRequestEvent' then 1 else null end) as pr_count,
    COUNT(case git.type when 'DeleteEvent' then 1 else null end) as delete_count,
    COUNT(case git.type when 'PublicEvent' then 1 else null end) as public_count,
    COUNT(case git.type when 'CreateEvent' then 1 else null end) as create_count,
    COUNT(case git.type when 'GollumEvent' then 1 else null end) as gollum_count,
    COUNT(case git.type when 'MemberEvent' then 1 else null end) as member_count,
    COUNT(case git.type when 'CommitCommentEvent' then 1 else null end) as commit_comment_count,
    (SUM(case when git.type = 'IssueCommentEvent' then 1 else 0 end) + SUM(case when git.type = 'WatchEvent' then 1 else 0 end) + 
      SUM(case when git.type = 'ForkEvent' then 1 else 0 end)+SUM(case when git.type = 'PushEvent' then 1 else 0 end)
      +SUM(case when git.type = 'PullRequestEvent' then 1 else 0 end)+
      SUM(case when git.type = 'DeleteEvent' then 1 else 0 end)+SUM(case when git.type = 'PublicEvent' then 1 else 0 end)
      +SUM(case when git.type = 'CreateEvent' then 1 else 0 end)+SUM(case when git.type = 'GollumEvent' then 1 else 0 end)
      +SUM(case when git.type = 'MemberEvent' then 1 else 0 end)+SUM(case when git.type = 'CommitCommentEvent' then 1 else 0 end)) as total_event_count
from silver_db.s_github AS git
INNER JOIN silver_db.s_company_detail AS comp
            ON (git.repository_account = comp.Repository_account AND git.repository_name = comp.Repository_name and comp.is_current = True)
            or (git.repository_account = comp.Repository_account AND comp.Repository_name = "" and comp.is_current = True)
group by date_trunc('quarter', git.created_at_datetime_utc), git.repository_account, git.repository_name, comp._pk, comp.Organization
having COUNT(case git.type when 'CreateEvent' then 1 else null end) != sum(case git.type when 'CreateEvent' then 1 else 0 end);
