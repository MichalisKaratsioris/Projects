-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Filtered detail data

-- COMMAND ----------

-- SELECT *
-- FROM silver_db.s_company
-- ORDER BY organization_name ASC, valid_from_date ASC;

-- COMMAND ----------

-- SELECT *
-- FROM silver_db.s_company
-- WHERE is_current = true
-- ORDER BY organization_name ASC;

-- COMMAND ----------

-- SELECT *
-- FROM silver_db.s_github;

-- COMMAND ----------

-- SELECT COUNT(*)
-- FROM silver_db.s_github;

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_db.g_github_view
AS
SELECT git._pk,
       com.organization_name,
       com.repository_account AS com_repository_account,
       com.repository_name AS com_repository_name,
       git.repository_account,
       git.repository_name,
       git.user_id,
       git.event_id,
       git.type,
       git.created_at_datetime_utc,
       git.valid_date,
       git.dbx_created_at_datetime_utc
FROM silver_db.s_company AS com
JOIN silver_db.s_github AS git
ON com.repository_account = git.repository_account AND (com.repository_name = git.repository_name OR com.repository_name = '')
WHERE com.is_current = true;

-- COMMAND ----------

SELECT *
FROM gold_db.g_github_view;

-- COMMAND ----------

SELECT COUNT(*)
FROM gold_db.g_github_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Test "airbytehq"

-- COMMAND ----------

-- SELECT *
-- FROM silver_db.s_company
-- WHERE is_current = true AND repository_account = 'airbytehq';

-- COMMAND ----------

-- SELECT repository_name,
--        COUNT(*)
-- FROM silver_db.s_github
-- WHERE repository_account = 'airbytehq'
-- GROUP BY repository_name
-- ORDER BY repository_name ASC;

-- COMMAND ----------

-- SELECT repository_name,
--        COUNT(*)
-- FROM gold_db.g_github_view
-- WHERE repository_account = 'airbytehq'
-- GROUP BY repository_name
-- ORDER BY repository_name ASC;

-- COMMAND ----------

-- SELECT *
-- FROM gold_db.g_github_view
-- WHERE repository_account = 'airbytehq';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Test "astronomer"

-- COMMAND ----------

-- SELECT *
-- FROM silver_db.s_company
-- WHERE is_current = true AND repository_account = 'astronomer';

-- COMMAND ----------

-- SELECT repository_name,
--        COUNT(*)
-- FROM silver_db.s_github
-- WHERE repository_account = 'astronomer'
-- GROUP BY repository_name
-- ORDER BY repository_name ASC;

-- COMMAND ----------

-- SELECT repository_name,
--        COUNT(*)
-- FROM gold_db.g_github_view
-- WHERE repository_account = 'astronomer'
-- GROUP BY repository_name
-- ORDER BY repository_name ASC;

-- COMMAND ----------

-- SELECT *
-- FROM gold_db.g_github_view
-- WHERE repository_account = 'astronomer';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Test _pk duplicates

-- COMMAND ----------

-- SELECT COUNT(DISTINCT _pk)
-- FROM gold_db.g_github_view;

-- COMMAND ----------

-- SELECT _pk,
--        COUNT(*)
-- FROM gold_db.g_github_view
-- GROUP BY _pk
-- HAVING COUNT(*) > 1
-- ORDER BY _pk ASC;

-- COMMAND ----------

-- SELECT *
-- FROM gold_db.g_github_view
-- WHERE _pk = 19790627537;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Aggregated report data

-- COMMAND ----------

-- SELECT type
-- FROM gold_db.g_github_view
-- GROUP BY type
-- ORDER BY type ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Daily

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_db.g_github_daily
AS
SELECT hash(to_date(created_at_datetime_utc), organization_name) AS _pk,
       to_date(created_at_datetime_utc) AS first_day_of_period,
       month(created_at_datetime_utc) AS month,
       quarter(created_at_datetime_utc) AS quarter,
       year(created_at_datetime_utc) AS year,
       organization_name,
       repository_account,
       CASE WHEN com_repository_name = '' THEN repository_account ELSE repository_name END AS repository_name,
       COUNT(DISTINCT event_id) AS event_count,
       COUNT(DISTINCT user_id) AS user_count,
       SUM(CASE WHEN type = 'IssuesEvent' THEN 1 ELSE 0 END) AS issues_count,
       SUM(CASE WHEN type = 'WatchEvent' THEN 1 ELSE 0 END) AS watch_count,
       SUM(CASE WHEN type = 'ForkEvent' THEN 1 ELSE 0 END) AS fork_count,
       SUM(CASE WHEN type = 'PushEvent' THEN 1 ELSE 0 END) AS push_count,
       SUM(CASE WHEN type = 'PullRequestEvent' THEN 1 ELSE 0 END) AS pr_count,
       SUM(CASE WHEN type = 'DeleteEvent' THEN 1 ELSE 0 END) AS delete_count,
       SUM(CASE WHEN type = 'PublicEvent' THEN 1 ELSE 0 END) AS public_count,
       SUM(CASE WHEN type = 'CreateEvent' THEN 1 ELSE 0 END) AS create_count,
       SUM(CASE WHEN type = 'GollumEvent' THEN 1 ELSE 0 END) AS gollum_count,
       SUM(CASE WHEN type = 'MemberEvent' THEN 1 ELSE 0 END) AS member_count,
       SUM(CASE WHEN type = 'CommitCommentEvent' THEN 1 ELSE 0 END) AS commit_comment_count,
       SUM(CASE WHEN type IN ('IssuesEvent', 'WatchEvent', 'ForkEvent', 'PushEvent', 'PullRequestEvent', 'DeleteEvent', 'PublicEvent', 'CreateEvent', 'GollumEvent', 'MemberEvent', 'CommitCommentEvent') THEN 1 ELSE 0 END) AS total_event_count
FROM gold_db.g_github_view
GROUP BY hash(to_date(created_at_datetime_utc), organization_name),
         to_date(created_at_datetime_utc),
         month(created_at_datetime_utc),
         quarter(created_at_datetime_utc),
         year(created_at_datetime_utc),
         organization_name,
         repository_account,
         CASE WHEN com_repository_name = '' THEN repository_account ELSE repository_name END
ORDER BY first_day_of_period ASC, organization_name ASC;

-- COMMAND ----------

SELECT *
FROM gold_db.g_github_daily;

-- COMMAND ----------

SELECT COUNT(*)
FROM gold_db.g_github_daily;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Test Daily

-- COMMAND ----------

SELECT month,
       COUNT(*) AS row_count,
       SUM(event_count) AS event_count,
       SUM(user_count) AS user_count,
       SUM(issues_count) AS issues_count,
       SUM(pr_count) AS pr_count,
       SUM(total_event_count) AS total_event_count
FROM gold_db.g_github_daily
GROUP BY month
ORDER BY month ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Monthly

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_db.g_github_monthly
AS
SELECT hash(trunc(created_at_datetime_utc, 'MM'), organization_name) AS _pk,
       trunc(created_at_datetime_utc, 'MM') AS first_day_of_period,
       month(created_at_datetime_utc) AS month,
       quarter(created_at_datetime_utc) AS quarter,
       year(created_at_datetime_utc) AS year,
       organization_name,
       repository_account,
       CASE WHEN com_repository_name = '' THEN repository_account ELSE repository_name END AS repository_name,
       COUNT(DISTINCT event_id) AS event_count,
       COUNT(DISTINCT user_id) AS user_count,
       SUM(CASE WHEN type = 'IssuesEvent' THEN 1 ELSE 0 END) AS issues_count,
       SUM(CASE WHEN type = 'WatchEvent' THEN 1 ELSE 0 END) AS watch_count,
       SUM(CASE WHEN type = 'ForkEvent' THEN 1 ELSE 0 END) AS fork_count,
       SUM(CASE WHEN type = 'PushEvent' THEN 1 ELSE 0 END) AS push_count,
       SUM(CASE WHEN type = 'PullRequestEvent' THEN 1 ELSE 0 END) AS pr_count,
       SUM(CASE WHEN type = 'DeleteEvent' THEN 1 ELSE 0 END) AS delete_count,
       SUM(CASE WHEN type = 'PublicEvent' THEN 1 ELSE 0 END) AS public_count,
       SUM(CASE WHEN type = 'CreateEvent' THEN 1 ELSE 0 END) AS create_count,
       SUM(CASE WHEN type = 'GollumEvent' THEN 1 ELSE 0 END) AS gollum_count,
       SUM(CASE WHEN type = 'MemberEvent' THEN 1 ELSE 0 END) AS member_count,
       SUM(CASE WHEN type = 'CommitCommentEvent' THEN 1 ELSE 0 END) AS commit_comment_count,
       SUM(CASE WHEN type IN ('IssuesEvent', 'WatchEvent', 'ForkEvent', 'PushEvent', 'PullRequestEvent', 'DeleteEvent', 'PublicEvent', 'CreateEvent', 'GollumEvent', 'MemberEvent', 'CommitCommentEvent') THEN 1 ELSE 0 END) AS total_event_count
FROM gold_db.g_github_view
GROUP BY hash(trunc(created_at_datetime_utc, 'MM'), organization_name),
         trunc(created_at_datetime_utc, 'MM'),
         month(created_at_datetime_utc),
         quarter(created_at_datetime_utc),
         year(created_at_datetime_utc),
         organization_name,
         repository_account,
         CASE WHEN com_repository_name = '' THEN repository_account ELSE repository_name END
ORDER BY first_day_of_period ASC, organization_name ASC;

-- COMMAND ----------

SELECT *
FROM gold_db.g_github_monthly;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Test Monthly

-- COMMAND ----------

SELECT month,
       COUNT(*) AS row_count,
       SUM(event_count) AS event_count,
       SUM(user_count) AS user_count,
       SUM(issues_count) AS issues_count,
       SUM(pr_count) AS pr_count,
       SUM(total_event_count) AS total_event_count
FROM gold_db.g_github_monthly
GROUP BY month
ORDER BY month ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Quarterly

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_db.g_github_quarterly
AS
SELECT hash(trunc(created_at_datetime_utc, 'quarter'), month(created_at_datetime_utc), organization_name) AS _pk,
       trunc(created_at_datetime_utc, 'quarter') AS first_day_of_period,
       month(created_at_datetime_utc) AS month,
       quarter(created_at_datetime_utc) AS quarter,
       year(created_at_datetime_utc) AS year,
       organization_name,
       repository_account,
       CASE WHEN com_repository_name = '' THEN repository_account ELSE repository_name END AS repository_name,
       COUNT(DISTINCT event_id) AS event_count,
       COUNT(DISTINCT user_id) AS user_count,
       SUM(CASE WHEN type = 'IssuesEvent' THEN 1 ELSE 0 END) AS issues_count,
       SUM(CASE WHEN type = 'WatchEvent' THEN 1 ELSE 0 END) AS watch_count,
       SUM(CASE WHEN type = 'ForkEvent' THEN 1 ELSE 0 END) AS fork_count,
       SUM(CASE WHEN type = 'PushEvent' THEN 1 ELSE 0 END) AS push_count,
       SUM(CASE WHEN type = 'PullRequestEvent' THEN 1 ELSE 0 END) AS pr_count,
       SUM(CASE WHEN type = 'DeleteEvent' THEN 1 ELSE 0 END) AS delete_count,
       SUM(CASE WHEN type = 'PublicEvent' THEN 1 ELSE 0 END) AS public_count,
       SUM(CASE WHEN type = 'CreateEvent' THEN 1 ELSE 0 END) AS create_count,
       SUM(CASE WHEN type = 'GollumEvent' THEN 1 ELSE 0 END) AS gollum_count,
       SUM(CASE WHEN type = 'MemberEvent' THEN 1 ELSE 0 END) AS member_count,
       SUM(CASE WHEN type = 'CommitCommentEvent' THEN 1 ELSE 0 END) AS commit_comment_count,
       SUM(CASE WHEN type IN ('IssuesEvent', 'WatchEvent', 'ForkEvent', 'PushEvent', 'PullRequestEvent', 'DeleteEvent', 'PublicEvent', 'CreateEvent', 'GollumEvent', 'MemberEvent', 'CommitCommentEvent') THEN 1 ELSE 0 END) AS total_event_count
FROM gold_db.g_github_view
GROUP BY hash(trunc(created_at_datetime_utc, 'quarter'), month(created_at_datetime_utc), organization_name),
         trunc(created_at_datetime_utc, 'quarter'),
         month(created_at_datetime_utc),
         quarter(created_at_datetime_utc),
         year(created_at_datetime_utc),
         organization_name,
         repository_account,
         CASE WHEN com_repository_name = '' THEN repository_account ELSE repository_name END
ORDER BY first_day_of_period ASC, month ASC, organization_name ASC;

-- COMMAND ----------

SELECT *
FROM gold_db.g_github_quarterly;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Test Quarterly

-- COMMAND ----------

SELECT quarter,
       COUNT(*) AS row_count,
       SUM(event_count) AS event_count,
       SUM(user_count) AS user_count,
       SUM(issues_count) AS issues_count,
       SUM(pr_count) AS pr_count,
       SUM(total_event_count) AS total_event_count
FROM gold_db.g_github_quarterly
GROUP BY quarter
ORDER BY quarter ASC;
