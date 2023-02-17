-- Databricks notebook source

CREATE OR REPLACE VIEW gold_db.g_github_view
    AS select comp.Organization, q.tags, q._pk, q.title, comp.tags as comptag, comp.Tags_array
    from (select *, explode(split(tags, '\\|')) as splitted from silver_db.s_stackoverflow_questions) as q
    inner join (select *, explode(Tags_array) as tags 
                      from silver_db.s_company_detail 
                      where is_current = True) as comp
            on q.splitted = ltrim(comp.tags);

-- COMMAND ----------

select count(*) from gold_db.g_github_view;


-- COMMAND ----------

select comp.Organization, q.tags, q._pk, q.title, comp.Tags_array, split(q.tags, '\\|') as splitted
    from silver_db.s_stackoverflow_questions as q
    inner join (select * 
                      from silver_db.s_company_detail 
                      where is_current = True) as comp
            on (arrays_overlap(split(q.tags, '\\|'),comp.Tags_array))
where array_contains(comp.Tags_array, " powerbi-desktop")
order by q._pk;

-- COMMAND ----------

select (case when array_size(split(tags, '\\|')) - array_size(Tags_array) < 0 then 0 else array_size(split(tags, '\\|')) - array_size(Tags_array) end)
from (select comp.Organization, q.tags, q._pk, q.title, comp.Tags_array
    from silver_db.s_stackoverflow_questions as q
    inner join (select * 
                      from silver_db.s_company_detail 
                      where is_current = True) as comp
            on arrays_overlap(split(q.tags, '\\|'),comp.Tags_array)
order by q._pk);

-- COMMAND ----------

 select array_size(array_except(split(tags, '\\|'),Tags_array))
from (select comp.Organization, q.tags, q._pk, q.title, comp.Tags_array
    from silver_db.s_stackoverflow_questions as q
    inner join (select * 
                      from silver_db.s_company_detail 
                      where is_current = True) as comp
            on arrays_overlap(split(q.tags, '\\|'),comp.Tags_array)
order by q._pk);


-- COMMAND ----------

--select count(*) from silver_db.s_stackoverflow_questions where tags = "powerbi-desktop";

select comp.Organization, q.tags, q._pk, q.title, comp.tags as comptag, comp.Tags_array
    from (select *, explode(split(tags, '\\|')) as splitted from silver_db.s_stackoverflow_questions) as q
    inner join (select *, explode(Tags_array) as tags 
                      from silver_db.s_company_detail 
                      where is_current = True) as comp
            on (q.splitted = ltrim(comp.tags))
where q.tags = "powerbi-desktop";

-- COMMAND ----------

select *
    from silver_db.s_stackoverflow_questions as q
where array_contains(split(q.tags, '\\|'), "powerbi-desktop")
order by q._pk;

-- COMMAND ----------

select *, array_contains(Tags_array, tags) as contains from gold_db.g_github_view where array_contains(Tags_array, tags)
