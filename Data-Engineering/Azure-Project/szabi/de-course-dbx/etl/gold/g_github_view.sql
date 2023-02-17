-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

SHOW TABLES
  IN silver_db

-- COMMAND ----------

SELECT *
FROM silver_db.s_github
LIMIT 10

-- COMMAND ----------

SELECT *
FROM silver_db.s_company_detail
LIMIT 10

-- COMMAND ----------

-- -- Rows number with this method: 786 352 pcs -- --

CREATE OR REPLACE VIEW gold_db.g_github_view
AS
SELECT sg.*, sc.organization
FROM silver_db.s_github sg
JOIN silver_db.s_company_detail sc
ON sg.repository_account = sc.repository_account
WHERE (sc.repository_name = sg.repository_name AND sc.is_current = True)
  OR  (sc.repository_name = "" AND sc.is_current = True);

-- COMMAND ----------

SELECT COUNT(*)
FROM gold_db.g_github_view
--LIMIT 10
--WHERE event_id = 22785653106
-- WHERE user_id = 39689341

-- COMMAND ----------

-- -- Checking the different type of EVENTs -- --
SELECT type, count(type)
FROM gold_db.g_github_view
GROUP BY type
ORDER BY type ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Github aggregations:

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_db.g_github_daily
AS
SELECT  Md5(to_date(created_at_datetime_utc)||organization||repository_account||repository_name) AS _pk,
        to_date(created_at_datetime_utc) AS first_day_of_period,
        MONTH(created_at_datetime_utc) AS month,
        QUARTER(created_at_datetime_utc) AS quarter,
        YEAR(created_at_datetime_utc) AS year,
        organization AS organization_name,
        repository_account,
        CASE
          WHEN NOT null THEN repository_name
          ELSE repository_account
        END AS repository_name,
        COUNT(DISTINCT event_id) AS event_count,
        COUNT(DISTINCT user_id) AS user_count,
        SUM(CASE WHEN type = "IssuesEvent" then 1 END) AS issues_count, -- -- -- https://stackoverflow.com/questions/17194145/sql-count-based-on-column-value
        SUM(CASE WHEN type = "WatchEvent" then 1 END) AS watch_count,
        SUM(CASE WHEN type = "ForkEvent" then 1 END) AS fork_count,
        SUM(CASE WHEN type = "PushEvent" then 1 END) AS push_count,
        SUM(CASE WHEN type = "PullRequestEvent" then 1 END) AS pr_count,
        SUM(CASE WHEN type = "PublicEvent" then 1 END) AS public_count,
        SUM(CASE WHEN type = "CreateEvent" then 1 END) AS create_count,
        SUM(CASE WHEN type = "GollumEvent" then 1 END) AS gollum_count,
        SUM(CASE WHEN type = "MemberEvent" then 1 END) AS member_count,
        SUM(CASE WHEN type = "CommitCommentEvent" then 1 END) AS commit_comment_count,
--        COUNT(CASE WHEN type = "IssuesEvent" then 1 END) +         -- -- -- this "count" solution also works
--          COUNT(CASE WHEN type = "WatchEvent" then 1 END) +
--          COUNT(CASE WHEN type = "ForkEvent" then 1 END) +
--          COUNT(CASE WHEN type = "PushEvent" then 1 END) +
--          COUNT(CASE WHEN type = "PullRequestEvent" then 1 END) + 
--          COUNT(CASE WHEN type = "PublicEvent" then 1 END) + 
--          COUNT(CASE WHEN type = "CreateEvent" then 1 END) +
--          COUNT(CASE WHEN type = "GollumEvent" then 1 END) + 
--          COUNT(CASE WHEN type = "MemberEvent" then 1 END) +
--          COUNT(CASE WHEN type = "CommitCommentEvent" then 1 END) AS total_event_count
        SUM(CASE WHEN type in ("IssuesEvent","WatchEvent","ForkEvent", "PushEvent", "PullRequestEvent", "PublicEvent", "CreateEvent", "GollumEvent", "MemberEvent",
            "CommitCommentEvent") THEN 1 END) AS total_event_count        
FROM gold_db.g_github_view
GROUP BY Md5(to_date(created_at_datetime_utc)||organization||repository_account||repository_name),
         to_date(created_at_datetime_utc),
         MONTH(created_at_datetime_utc),
         QUARTER(created_at_datetime_utc),
         YEAR(created_at_datetime_utc),
         organization_name,
         repository_account,
         repository_name
ORDER BY first_day_of_period

-- COMMAND ----------

SELECT *
FROM gold_db.g_github_daily
-- LIMIT 100
-- WHERE user_count is null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Testing our results:
-- MAGIC ![github_daily_test_results](files/tables/github_daily.png)

-- COMMAND ----------

SELECT month, COUNT(*) AS row_count, SUM(event_count) AS event_count, SUM(user_count) AS user_count, 
       SUM(issues_count) AS issues_count, SUM(pr_count) AS pr_count, SUM(total_event_count) AS total_event_count
FROM gold_db.g_github_daily
GROUP BY month
ORDER BY month ASC

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_db.g_github_monthly
AS
SELECT  Md5(trunc(created_at_datetime_utc, "MM")||organization||repository_account||repository_name) AS _pk,
        trunc(created_at_datetime_utc, "MM") AS first_day_of_period,
        MONTH(created_at_datetime_utc) AS month,
        QUARTER(created_at_datetime_utc) AS quarter,
        YEAR(created_at_datetime_utc) AS year,
        organization AS organization_name,
        repository_account,
        CASE
          WHEN NOT null THEN repository_name
          ELSE repository_account
        END AS repository_name,
        COUNT(DISTINCT event_id) AS event_count,
        COUNT(DISTINCT user_id) AS user_count,
        SUM(CASE WHEN type = "IssuesEvent" then 1 END) AS issues_count, -- -- -- https://stackoverflow.com/questions/17194145/sql-count-based-on-column-value
        SUM(CASE WHEN type = "WatchEvent" then 1 END) AS watch_count,
        SUM(CASE WHEN type = "ForkEvent" then 1 END) AS fork_count,
        SUM(CASE WHEN type = "PushEvent" then 1 END) AS push_count,
        SUM(CASE WHEN type = "PullRequestEvent" then 1 END) AS pr_count,
        SUM(CASE WHEN type = "PublicEvent" then 1 END) AS public_count,
        SUM(CASE WHEN type = "CreateEvent" then 1 END) AS create_count,
        SUM(CASE WHEN type = "GollumEvent" then 1 END) AS gollum_count,
        SUM(CASE WHEN type = "MemberEvent" then 1 END) AS member_count,
        SUM(CASE WHEN type = "CommitCommentEvent" then 1 END) AS commit_comment_count,
        COUNT(CASE WHEN type = "IssuesEvent" then 1 END) + 
          COUNT(CASE WHEN type = "WatchEvent" then 1 END) +
          COUNT(CASE WHEN type = "ForkEvent" then 1 END) +
          COUNT(CASE WHEN type = "PushEvent" then 1 END) +
          COUNT(CASE WHEN type = "PullRequestEvent" then 1 END) + 
          COUNT(CASE WHEN type = "PublicEvent" then 1 END) + 
          COUNT(CASE WHEN type = "CreateEvent" then 1 END) +
          COUNT(CASE WHEN type = "GollumEvent" then 1 END) + 
          COUNT(CASE WHEN type = "MemberEvent" then 1 END) +
          COUNT(CASE WHEN type = "CommitCommentEvent" then 1 END) AS total_event_count        
FROM gold_db.g_github_view
GROUP BY Md5(trunc(created_at_datetime_utc, "MM")||organization||repository_account||repository_name),
         trunc(created_at_datetime_utc, "MM"),
         MONTH(created_at_datetime_utc),
         QUARTER(created_at_datetime_utc),
         YEAR(created_at_datetime_utc),
         organization_name,
         repository_account,
         repository_name
ORDER BY first_day_of_period

-- COMMAND ----------

SELECT *
FROM gold_db.g_github_monthly
-- LIMIT 100
-- WHERE pr_count > 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Testing our results:
-- MAGIC ![github_daily_test_results](files/tables/github_monthly.png)

-- COMMAND ----------

SELECT month, COUNT(*) AS row_count, SUM(event_count) AS event_count, SUM(user_count) AS user_count, 
       SUM(issues_count) AS issues_count, SUM(pr_count) AS pr_count, SUM(total_event_count) AS total_event_count
FROM gold_db.g_github_monthly
GROUP BY month
ORDER BY month ASC

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_db.g_github_quarterly
AS
SELECT  Md5(trunc(created_at_datetime_utc, "quarter")||organization||repository_account||repository_name) AS _pk,
        trunc(created_at_datetime_utc, "quarter") AS first_day_of_period,
        MONTH(created_at_datetime_utc) AS month,
        QUARTER(created_at_datetime_utc) AS quarter,
        YEAR(created_at_datetime_utc) AS year,
        organization AS organization_name,
        repository_account,
        CASE
          WHEN NOT null THEN repository_name
          ELSE repository_account
        END AS repository_name,
        COUNT(event_id) AS event_count,
        COUNT(user_id) AS user_count,
        SUM(CASE WHEN type = "IssuesEvent" then 1 END) AS issues_count, -- -- -- https://stackoverflow.com/questions/17194145/sql-count-based-on-column-value
        SUM(CASE WHEN type = "WatchEvent" then 1 END) AS watch_count,
        SUM(CASE WHEN type = "ForkEvent" then 1 END) AS fork_count,
        SUM(CASE WHEN type = "PushEvent" then 1 END) AS push_count,
        SUM(CASE WHEN type = "PullRequestEvent" then 1 END) AS pr_count,
        SUM(CASE WHEN type = "PublicEvent" then 1 END) AS public_count,
        SUM(CASE WHEN type = "CreateEvent" then 1 END) AS create_count,
        SUM(CASE WHEN type = "GollumEvent" then 1 END) AS gollum_count,
        SUM(CASE WHEN type = "MemberEvent" then 1 END) AS member_count,
        SUM(CASE WHEN type = "CommitCommentEvent" then 1 END) AS commit_comment_count,
        SUM((CASE WHEN type = "IssuesEvent" then 1 ELSE 0 END) + 
            (CASE WHEN type = "WatchEvent" then 1 ELSE 0 END) +
            (CASE WHEN type = "ForkEvent" then 1 ELSE 0 END) +
            (CASE WHEN type = "PushEvent" then 1 ELSE 0 END) +
            (CASE WHEN type = "PullRequestEvent" then 1 ELSE 0 END) + 
            (CASE WHEN type = "PublicEvent" then 1 ELSE 0 END) + 
            (CASE WHEN type = "CreateEvent" then 1 ELSE 0 END) +
            (CASE WHEN type = "GollumEvent" then 1 ELSE 0 END) + 
            (CASE WHEN type = "MemberEvent" then 1 ELSE 0 END) +
            (CASE WHEN type = "CommitCommentEvent" then 1 ELSE 0 END)) AS total_event_count
FROM gold_db.g_github_view
GROUP BY Md5(trunc(created_at_datetime_utc, "quarter")||organization||repository_account||repository_name),
         trunc(created_at_datetime_utc, "quarter"),
         MONTH(created_at_datetime_utc),
         QUARTER(created_at_datetime_utc),
         YEAR(created_at_datetime_utc),
         organization_name,
         repository_account,
         repository_name
ORDER BY first_day_of_period

-- COMMAND ----------

SELECT *
FROM gold_db.g_github_quarterly
-- LIMIT 100
-- WHERE pr_count > 1

-- COMMAND ----------


