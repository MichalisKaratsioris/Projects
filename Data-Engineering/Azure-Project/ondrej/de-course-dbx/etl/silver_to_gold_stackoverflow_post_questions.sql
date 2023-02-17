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

-- SELECT explode(tags_array) AS tags, *
-- FROM silver_db.s_company
-- WHERE is_current = true;

-- COMMAND ----------

-- SELECT *
-- FROM silver_db.s_stackoverflow_post_questions;

-- COMMAND ----------

-- SELECT COUNT(*)
-- FROM silver_db.s_stackoverflow_post_questions;

-- COMMAND ----------

-- SELECT COUNT(DISTINCT id)
-- FROM silver_db.s_stackoverflow_post_questions;

-- COMMAND ----------

-- SELECT explode(tags_array) AS tags, *
-- FROM silver_db.s_stackoverflow_post_questions;

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_db.g_stackoverflow_post_questions_view
AS
SELECT que.id,
       que.title,
       que.accepted_answer_id,
       que.answer_count,
       que.comment_count,
       que.community_owned_datetime_utc,
       que.created_at_datetime_utc,
       que.favorite_count,
       que.last_activity_datetime_utc,
       que.last_edit_datetime_utc,
       que.last_editor_display_name,
       que.last_editor_user_id,
       que.owner_display_name,
       que.owner_user_id,
       que.parent_id,
       que.post_type_id,
       que.score,
       que.view_count,
       que.tags_array AS que_tags_array,
       que.tags AS que_tags,
       com.organization_name,
       com.tags_array AS com_tags_array,
       com.tags AS com_tags,
       que._pk,
       que.valid_date,
       que.dbx_created_at_datetime_utc,
       que.dbx_updated_at_datetime_utc
FROM (
    SELECT explode(tags_array) AS tags, *
    FROM silver_db.s_company
    WHERE is_current = true
) AS com
JOIN (
    SELECT explode(tags_array) AS tags, *
    FROM silver_db.s_stackoverflow_post_questions
) AS que
ON com.tags = que.tags;

-- COMMAND ----------

SELECT *
FROM gold_db.g_stackoverflow_post_questions_view;

-- COMMAND ----------

SELECT COUNT(*)
FROM gold_db.g_stackoverflow_post_questions_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Aggregated report data

-- COMMAND ----------

-- SELECT COUNT(DISTINCT id)
-- FROM gold_db.g_stackoverflow_post_questions_view;

-- COMMAND ----------

-- SELECT COUNT(DISTINCT id, organization_name)
-- FROM gold_db.g_stackoverflow_post_questions_view;

-- COMMAND ----------

-- SELECT *
-- FROM gold_db.g_stackoverflow_post_questions_view
-- WHERE answer_count IS null;

-- COMMAND ----------

-- SELECT *
-- FROM gold_db.g_stackoverflow_post_questions_view
-- WHERE comment_count IS null;

-- COMMAND ----------

-- SELECT *
-- FROM gold_db.g_stackoverflow_post_questions_view
-- WHERE favorite_count IS null;

-- COMMAND ----------

-- SELECT *
-- FROM gold_db.g_stackoverflow_post_questions_view
-- WHERE view_count IS null;

-- COMMAND ----------

-- SELECT *, array_size(array_except(que_tags_array, com_tags_array)) AS tags_count
-- FROM gold_db.g_stackoverflow_post_questions_view
-- WHERE organization_name = 'Firebase' AND to_date(created_at_datetime_utc) = '2022-01-01';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Daily

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_db.g_stackoverflow_post_questions_daily
AS
SELECT hash(to_date(created_at_datetime_utc), organization_name) AS _pk,
       to_date(created_at_datetime_utc) AS first_day_of_period,
       month(created_at_datetime_utc) AS month,
       quarter(created_at_datetime_utc) AS quarter,
       year(created_at_datetime_utc) AS year,
       organization_name,
       COUNT(id) AS post_count,
       SUM(answer_count) AS answer_count,
       round(AVG(answer_count), 3) AS avg_answer_count,
       SUM(comment_count) AS comment_count,
       round(AVG(comment_count), 3) AS avg_comment_count,
       SUM(favorite_count) AS favorite_count,
       round(AVG(coalesce(favorite_count, 0)), 3) AS avg_favorite_count,
       SUM(view_count) AS view_count,
       round(AVG(view_count), 3) AS avg_view_count,
       COUNT(accepted_answer_id) AS accepted_answer_count,
       round(COUNT(accepted_answer_id)/COUNT(id), 3) AS avg_accepted_answer_count,
       SUM(CASE WHEN answer_count = 0 THEN 1 ELSE 0 END) AS no_answer_count,
       round(SUM(CASE WHEN answer_count = 0 THEN 1 ELSE 0 END)/COUNT(id), 3) AS avg_no_answer_count,
       round(SUM(score)/COUNT(id), 3) AS score,
       SUM(array_size(array_except(que_tags_array, com_tags_array))) AS tags_count,
       MAX(last_activity_datetime_utc) AS last_activity_datetime_utc,
       MAX(last_edit_datetime_utc) AS last_edit_datetime_utc
FROM gold_db.g_stackoverflow_post_questions_view
GROUP BY hash(to_date(created_at_datetime_utc), organization_name),
         to_date(created_at_datetime_utc),
         month(created_at_datetime_utc),
         quarter(created_at_datetime_utc),
         year(created_at_datetime_utc),
         organization_name
ORDER BY first_day_of_period ASC, organization_name ASC;

-- COMMAND ----------

SELECT *
FROM gold_db.g_stackoverflow_post_questions_daily;

-- COMMAND ----------

SELECT COUNT(*)
FROM gold_db.g_stackoverflow_post_questions_daily;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Test Daily

-- COMMAND ----------

SELECT month,
       COUNT(*) AS row_count,
       SUM(post_count) AS post_count,
       SUM(answer_count) AS answer_count,
       round(AVG(avg_answer_count), 3) AS avg_answer_count,
       round(AVG(score), 3) AS avg_of_score,
       SUM(tags_count) AS tags_count
FROM gold_db.g_stackoverflow_post_questions_daily
GROUP BY month
ORDER BY month ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Monthly

-- COMMAND ----------


