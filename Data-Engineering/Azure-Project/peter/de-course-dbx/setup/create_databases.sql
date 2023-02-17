-- Databricks notebook source
-- MAGIC %md
-- MAGIC # **MAKING NEW DATABASE**
-- MAGIC - Plus some basic commands

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS bronze_db
LOCATION "dbfs:/mnt/gfaproject/bronze";

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS silver_db
LOCATION "dbfs:/mnt/gfaproject/silver";

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESC DATABASE EXTENDED bronze_db;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES IN bronze_db;

-- COMMAND ----------

USE bronze_db;

-- COMMAND ----------

DROP TABLE bronze_db.b_company;

-- COMMAND ----------



-- COMMAND ----------


