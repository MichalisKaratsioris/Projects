-- Databricks notebook source
CREATE OR REPLACE VIEW gold_db.g_github_view
    AS SELECT git._pk, comp.Organization, comp.Repository_account, comp.Repository_name, comp.is_current
         FROM silver_db.s_github AS git
         INNER JOIN silver_db.s_company_detail AS comp
            ON (git.repository_account = comp.Repository_account AND git.repository_name = comp.Repository_name and comp.is_current = True)
            or (git.repository_account = comp.Repository_account AND comp.Repository_name = "" and comp.is_current = True);

-- COMMAND ----------

--if the repository name is empty in the Company Details table, it means that all the available repositories should be used for that GitHub account.

-- COMMAND ----------

SELECT count(*)
         FROM silver_db.s_github AS git
         INNER JOIN silver_db.s_company_detail AS comp
            ON (git.repository_account = comp.Repository_account AND git.repository_name = comp.Repository_name and comp.is_current = True)
            or (git.repository_account = comp.Repository_account AND comp.Repository_name = "" and comp.is_current = True);

-- COMMAND ----------

select count(*) as zeros
from silver_db.s_company_detail as s
where s.Repository_name is null;

select count(*) as blanks
from silver_db.s_company_detail as s
where s.Repository_name = "";

-- COMMAND ----------

select count(*) from gold_db.g_github_view;

-- COMMAND ----------

select * from gold_db.g_github_view where Repository_name = "";
