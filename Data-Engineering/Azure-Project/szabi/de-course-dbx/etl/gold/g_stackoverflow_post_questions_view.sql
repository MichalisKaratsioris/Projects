-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

SHOW TABLES
  IN silver_db

-- COMMAND ----------

SELECT *
FROM silver_db.s_stackoverflow_post_questions
LIMIT 10

-- COMMAND ----------

SELECT *
FROM silver_db.s_company_detail
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking functionality of SPLIT and EXPLODE:

-- COMMAND ----------

SELECT organization, EXPLODE(tags_array) AS tags
FROM silver_db.s_company_detail

-- COMMAND ----------

SELECT id, EXPLODE(SPLIT(tags, "\\|"))
FROM silver_db.s_stackoverflow_post_questions

-- COMMAND ----------

-- -- Rows number with this method: 21 912 pcs -- --

CREATE OR REPLACE VIEW gold_db.g_stackoverflow_post_questions_view
AS
SELECT ss.*, sc.organization, sc.tags_array
FROM 
  (SELECT *, EXPLODE(SPLIT(tags, '\\|')) stack_tags 
   FROM silver_db.s_stackoverflow_post_questions) ss
JOIN
  (SELECT *, EXPLODE(tags_array) company_tags 
   FROM silver_db.s_company_detail) sc
   ON ss.stack_tags = sc.company_tags
WHERE sc.is_current = True

-- COMMAND ----------

SELECT id, EXPLODE(SPLIT(tags, "\\|"))
FROM gold_db.g_stackoverflow_post_questions_view

-- COMMAND ----------

-- -- Rows number with this method: 237 pcs -- --

-- CREATE OR REPLACE VIEW gold_db.g_stackoverflow_post_questions_view
-- AS
-- SELECT ss.*, sc.organization
-- FROM silver_db.s_stackoverflow_post_questions ss
-- INNER JOIN silver_db.s_company_detail sc
--  ON SPLIT(ss.tags, "\\|") = sc.tags_array
--  AND sc.is_current = True

-- https://learn.microsoft.com/en-us/u-sql/statements-and-expressions/select/from/cross-apply/explode
-- WHERE EXPLODE(FROM ss_tag in ss.tags.Split("\\|") select ss_tag) = EXPLODE(from sc_tag in sc.tags_array select sc_tag)

-- COMMAND ----------

SELECT *
FROM gold_db.g_stackoverflow_post_questions_view
-- GROUP BY id
-- ORDER BY id
-- LIMIT 10
-- WHERE answer_count is null

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_db.g_stackoverflow_post_questions_daily
AS
SELECT  Md5(to_date(creation_date_datetime_utc)||organization||MONTH(creation_date_datetime_utc)||QUARTER(creation_date_datetime_utc)) AS _pk,
        to_date(creation_date_datetime_utc) AS first_day_of_period,
        MONTH(creation_date_datetime_utc) AS month,
        QUARTER(creation_date_datetime_utc) AS quarter,
        YEAR(creation_date_datetime_utc) AS year,
        organization AS organization_name,
        COUNT(DISTINCT id) AS post_count,  -- -- --nr. of id: 21912, nr of distinct id: 21846
        SUM(answer_count) AS answer_count,
        ROUND(AVG(answer_count), 3) AS avg_answer_count,   -- -- -- round to 3. decimal
        SUM(comment_count) AS comment_count,
        ROUND(AVG(comment_count), 3) AS avg_comment_count,
        SUM(COALESCE(favorite_count, 0)) AS favorite_count,   -- -- -- https://stackoverflow.com/questions/16840522/replacing-null-with-0-in-a-sql-server-query
        ROUND(AVG(COALESCE(favorite_count, 0)), 3) AS avg_favorite_count,
        SUM(view_count) AS view_count,
        ROUND(AVG(view_count), 3) AS avg_view_countcomment_count,
        COUNT(accepted_answer_id) AS accepeted_answer_count,
        ROUND(COUNT(accepted_answer_id)/COUNT(id), 3) AS avg_accepted_answer_count,
        COUNT(CASE WHEN answer_count = 0 THEN 1 ELSE null END) AS no_answer_count,   -- -- -- COUNT only counts not null values
        ROUND(COUNT(CASE WHEN answer_count = 0 THEN 1 ELSE null END)/COUNT(id), 3) AS avg_no_answer_count,
        ROUND(SUM(score)/COUNT(id), 3) AS score,
        SUM(SIZE(ARRAY_EXCEPT(SPLIT(tags, '\\|'), tags_array))) AS tags_count,
        MAX(last_activity_date_datetime_utc) AS last_activity_datetime_utc,
        MAX(last_edit_date_datetime_utc) AS last_edit_datetime_utc                     
FROM gold_db.g_stackoverflow_post_questions_view
GROUP BY Md5(to_date(creation_date_datetime_utc)||organization||MONTH(creation_date_datetime_utc)||QUARTER(creation_date_datetime_utc)),
         to_date(creation_date_datetime_utc),
         MONTH(creation_date_datetime_utc), 
         QUARTER(creation_date_datetime_utc),
         YEAR(creation_date_datetime_utc),
         organization_name
ORDER BY first_day_of_period

-- COMMAND ----------

SELECT *
FROM gold_db.g_stackoverflow_post_questions_daily

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Testing our results:
-- MAGIC ![stackoverflow_daily_test_results](files/tables/stackoverflow_test.png)

-- COMMAND ----------

SELECT month, COUNT(_pk) AS row_count, SUM(post_count) AS post_count, SUM(answer_count) AS answer_count, 
       ROUND(AVG(avg_answer_count), 3) AS avg_answer_count, ROUND(AVG(score), 3) AS avg_of_score, SUM(tags_count) AS tags_count
FROM gold_db.g_stackoverflow_post_questions_daily
GROUP BY month
ORDER BY month ASC

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_db.g_stackoverflow_post_questions_monthly
AS
SELECT  Md5(trunc(creation_date_datetime_utc, "MM")||organization||MONTH(creation_date_datetime_utc)||QUARTER(creation_date_datetime_utc)) AS _pk,
        trunc(creation_date_datetime_utc, "MM") AS first_day_of_period,
        MONTH(creation_date_datetime_utc) AS month,
        QUARTER(creation_date_datetime_utc) AS quarter,
        YEAR(creation_date_datetime_utc) AS year,
        organization AS organization_name,
        COUNT(DISTINCT id) AS post_count,  -- -- --nr. of id: 21912, nr of distinct id: 21846
        SUM(answer_count) AS answer_count,
        ROUND(AVG(answer_count), 3) AS avg_answer_count,   -- -- -- round to 3. decimal
        SUM(comment_count) AS comment_count,
        ROUND(AVG(comment_count), 3) AS avg_comment_count,
        SUM(COALESCE(favorite_count, 0)) AS favorite_count,   -- -- -- https://stackoverflow.com/questions/16840522/replacing-null-with-0-in-a-sql-server-query
        ROUND(AVG(COALESCE(favorite_count, 0)), 3) AS avg_favorite_count,
        SUM(view_count) AS view_count,
        ROUND(AVG(view_count), 3) AS avg_view_countcomment_count,
        COUNT(accepted_answer_id) AS accepeted_answer_count,
        ROUND(COUNT(accepted_answer_id)/COUNT(id), 3) AS avg_accepted_answer_count,
        COUNT(CASE WHEN answer_count = 0 THEN 1 ELSE null END) AS no_answer_count,   -- -- -- COUNT only counts not null values
        ROUND(COUNT(CASE WHEN answer_count = 0 THEN 1 ELSE null END)/COUNT(id), 3) AS avg_no_answer_count,
        ROUND(SUM(score)/COUNT(id), 3) AS score,
        SUM(SIZE(ARRAY_EXCEPT(SPLIT(tags, '\\|'), tags_array))) AS tags_count,
        MAX(last_activity_date_datetime_utc) AS last_activity_datetime_utc,
        MAX(last_edit_date_datetime_utc) AS last_edit_datetime_utc                     
FROM gold_db.g_stackoverflow_post_questions_view
GROUP BY Md5(trunc(creation_date_datetime_utc, "MM")||organization||MONTH(creation_date_datetime_utc)||QUARTER(creation_date_datetime_utc)),
         trunc(creation_date_datetime_utc, "MM"),
         MONTH(creation_date_datetime_utc), 
         QUARTER(creation_date_datetime_utc),
         YEAR(creation_date_datetime_utc),
         organization_name
ORDER BY first_day_of_period

-- COMMAND ----------

SELECT *
FROM gold_db.g_stackoverflow_post_questions_monthly

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Testing our results:
-- MAGIC ![stakcoverflow_monthly_test_results](files/tables/stackoverflow_test_montly.png)

-- COMMAND ----------

SELECT month, COUNT(_pk) AS row_count, SUM(post_count) AS post_count, SUM(answer_count) AS answer_count, 
       ROUND(AVG(avg_answer_count), 3) AS avg_answer_count, ROUND(AVG(score), 3) AS avg_of_score, SUM(tags_count) AS tags_count
FROM gold_db.g_stackoverflow_post_questions_monthly
GROUP BY month
ORDER BY month ASC

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_db.g_stackoverflow_post_questions_quarterly
AS
SELECT  Md5(trunc(creation_date_datetime_utc, "quarter")||organization||MONTH(creation_date_datetime_utc)||QUARTER(creation_date_datetime_utc)) AS _pk,
        trunc(creation_date_datetime_utc, "quarter") AS first_day_of_period,
        MONTH(creation_date_datetime_utc) AS month,
        QUARTER(creation_date_datetime_utc) AS quarter,
        YEAR(creation_date_datetime_utc) AS year,
        organization AS organization_name,
        COUNT(DISTINCT id) AS post_count,  -- -- --nr. of id: 21912, nr of distinct id: 21846
        SUM(answer_count) AS answer_count,
        ROUND(AVG(answer_count), 3) AS avg_answer_count,   -- -- -- round to 3. decimal
        SUM(comment_count) AS comment_count,
        ROUND(AVG(comment_count), 3) AS avg_comment_count,
        SUM(COALESCE(favorite_count, 0)) AS favorite_count,   -- -- -- https://stackoverflow.com/questions/16840522/replacing-null-with-0-in-a-sql-server-query
        ROUND(AVG(COALESCE(favorite_count, 0)), 3) AS avg_favorite_count,
        SUM(view_count) AS view_count,
        ROUND(AVG(view_count), 3) AS avg_view_countcomment_count,
        COUNT(accepted_answer_id) AS accepeted_answer_count,
        ROUND(COUNT(accepted_answer_id)/COUNT(id), 3) AS avg_accepted_answer_count,
        COUNT(CASE WHEN answer_count = 0 THEN 1 ELSE null END) AS no_answer_count,   -- -- -- COUNT only counts not null values
        ROUND(COUNT(CASE WHEN answer_count = 0 THEN 1 ELSE null END)/COUNT(id), 3) AS avg_no_answer_count,
        ROUND(SUM(score)/COUNT(id), 3) AS score,
        SUM(SIZE(ARRAY_EXCEPT(SPLIT(tags, '\\|'), tags_array))) AS tags_count,
        MAX(last_activity_date_datetime_utc) AS last_activity_datetime_utc,
        MAX(last_edit_date_datetime_utc) AS last_edit_datetime_utc                     
FROM gold_db.g_stackoverflow_post_questions_view
GROUP BY Md5(trunc(creation_date_datetime_utc, "quarter")||organization||MONTH(creation_date_datetime_utc)||QUARTER(creation_date_datetime_utc)),
         trunc(creation_date_datetime_utc, "quarter"),
         MONTH(creation_date_datetime_utc), 
         QUARTER(creation_date_datetime_utc),
         YEAR(creation_date_datetime_utc),
         organization_name
ORDER BY first_day_of_period

-- COMMAND ----------

SELECT *
FROM gold_db.g_stackoverflow_post_questions_quarterly

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("This notebook successfully run and finished!")
