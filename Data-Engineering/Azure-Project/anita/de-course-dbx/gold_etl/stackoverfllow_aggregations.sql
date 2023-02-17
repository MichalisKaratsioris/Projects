-- Databricks notebook source
--create table g_stackoverflow_post_questions_daily as
select comp._pk,
    CAST(date_trunc('day', q.creation_date_datetime_utc) AS DATE) as first_day_of_period,
    MONTH(date_trunc('day', q.creation_date_datetime_utc)) as month,
    QUARTER(date_trunc('day', q.creation_date_datetime_utc)) as quarter,
    YEAR(date_trunc('day', q.creation_date_datetime_utc)) as year,
    comp.Organization as organization_name,
    COUNT(q.id) as post_count,
    SUM(q.answer_count) as answer_count,
    AVG(q.answer_count) as avg_answer_count,
    SUM(q.comment_count) as comment_count, 
    AVG(q.comment_count) as avg_comment_count,
    SUM(COALESCE(q.favorite_count, 0)) as favorite_count,
    AVG(COALESCE(q.favorite_count, 0)) as avg_favorite_count,
    SUM(q.view_count) as view_count,
    AVG(q.view_count) as avg_view_count,
    COUNT(q.accepted_answer_id) as accepted_answer_count,
    (COUNT(q.accepted_answer_id)/COUNT(q.id)) as avg_accepted_answer_count,
    COUNT(case when q.answer_count = 0 then 1 else null end) as no_answer_count,
    ((COUNT(case when q.answer_count = 0 then 1 else null end))/COUNT(q.id)) as avg_no_answer_count,
    (sum(q.score)/count(q.id) ) as score,
    array_size(array_except(split(q.tags, '\\|'),comp.Tags_array)) as tags_count,
    MAX(last_activity_date_datetime_utc) as last_activity_datetime_utc,
    MAX(last_edit_date_datetime_utc) as last_edit_datetime_utc
from (select *, explode(split(tags, '\\|')) as splitted from silver_db.s_stackoverflow_questions) as q
    inner join (select *, explode(Tags_array) as tags 
                      from silver_db.s_company_detail 
                      where is_current = True) as comp
            on q.splitted = ltrim(comp.tags)
group by date_trunc('day', q.creation_date_datetime_utc), comp._pk, comp.Organization, q.tags, comp.Tags_array
order by first_day_of_period, organization_name;

-- COMMAND ----------

--create table g_stackoverflow_post_questions_monthly as
select comp._pk,
    CAST(date_trunc('month', q.creation_date_datetime_utc) AS DATE) as first_day_of_period,
    MONTH(date_trunc('month', q.creation_date_datetime_utc)) as month,
    QUARTER(date_trunc('month', q.creation_date_datetime_utc)) as quarter,
    YEAR(date_trunc('month', q.creation_date_datetime_utc)) as year,
    comp.Organization as organization_name,
    COUNT(q.id) as post_count,
    SUM(q.answer_count) as answer_count,
    AVG(q.answer_count) as avg_answer_count,
    SUM(q.comment_count) as comment_count, 
    AVG(q.comment_count) as avg_comment_count,
    SUM(COALESCE(q.favorite_count, 0)) as favorite_count,
    AVG(COALESCE(q.favorite_count, 0)) as avg_favorite_count,
    SUM(q.view_count) as view_count,
    AVG(q.view_count) as avg_view_count,
    COUNT(q.accepted_answer_id) as accepted_answer_count,
    (COUNT(q.accepted_answer_id)/COUNT(q.id)) as avg_accepted_answer_count,
    COUNT(case when q.answer_count = 0 then 1 else null end) as no_answer_count,
    ((COUNT(case when q.answer_count = 0 then 1 else null end))/COUNT(q.id)) as avg_no_answer_count,
    (sum(q.score)/count(q.id) ) as score,
    array_size(array_except(split(q.tags, '\\|'),comp.Tags_array)) as tags_count,
    MAX(last_activity_date_datetime_utc) as last_activity_datetime_utc,
    MAX(last_edit_date_datetime_utc) as last_edit_datetime_utc
from (select *, explode(split(tags, '\\|')) as splitted from silver_db.s_stackoverflow_questions) as q
    inner join (select *, explode(Tags_array) as tags 
                      from silver_db.s_company_detail 
                      where is_current = True) as comp
            on q.splitted = ltrim(comp.tags)
group by date_trunc('month', q.creation_date_datetime_utc), comp._pk, comp.Organization, q.tags, comp.Tags_array
order by first_day_of_period, organization_name;

-- COMMAND ----------

--create table g_stackoverflow_post_questions_quarterly as
select comp._pk,
    CAST(date_trunc('quarter', q.creation_date_datetime_utc) AS DATE) as first_day_of_period,
    MONTH(date_trunc('quarter', q.creation_date_datetime_utc)) as month,
    QUARTER(date_trunc('quarter', q.creation_date_datetime_utc)) as quarter,
    YEAR(date_trunc('quarter', q.creation_date_datetime_utc)) as year,
    comp.Organization as organization_name,
    COUNT(q.id) as post_count,
    SUM(q.answer_count) as answer_count,
    AVG(q.answer_count) as avg_answer_count,
    SUM(q.comment_count) as comment_count, 
    AVG(q.comment_count) as avg_comment_count,
    SUM(COALESCE(q.favorite_count, 0)) as favorite_count,
    AVG(COALESCE(q.favorite_count, 0)) as avg_favorite_count,
    SUM(q.view_count) as view_count,
    AVG(q.view_count) as avg_view_count,
    COUNT(q.accepted_answer_id) as accepted_answer_count,
    (COUNT(q.accepted_answer_id)/COUNT(q.id)) as avg_accepted_answer_count,
    COUNT(case when q.answer_count = 0 then 1 else null end) as no_answer_count,
    ((COUNT(case when q.answer_count = 0 then 1 else null end))/COUNT(q.id)) as avg_no_answer_count,
    ( sum(q.score)/count(q.id) ) as score,
    array_size(array_except(split(q.tags, '\\|'),comp.Tags_array)) as tags_count,
    MAX(last_activity_date_datetime_utc) as last_activity_datetime_utc,
    MAX(last_edit_date_datetime_utc) as last_edit_datetime_utc
from (select *, explode(split(tags, '\\|')) as splitted from silver_db.s_stackoverflow_questions) as q
    inner join (select *, explode(Tags_array) as tags 
                      from silver_db.s_company_detail 
                      where is_current = True) as comp
            on q.splitted = ltrim(comp.tags)
group by date_trunc('quarter', q.creation_date_datetime_utc), comp._pk, comp.Organization, q.tags, comp.Tags_array
order by first_day_of_period, organization_name;

-- COMMAND ----------

select favorite_count, score
from silver_db.s_stackoverflow_questions
where score < 0
order by creation_date_datetime_utc;

-- COMMAND ----------

select max(last_edit_date_datetime_utc)
from silver_db.s_stackoverflow_questions
where score < 0;
