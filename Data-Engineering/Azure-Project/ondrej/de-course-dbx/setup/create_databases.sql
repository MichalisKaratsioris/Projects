-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create bronze_db

-- COMMAND ----------

-- CREATE DATABASE IF NOT EXISTS bronze_db
-- LOCATION 'dbfs:/mnt/decoursesa/bronze';

-- COMMAND ----------

SHOW TABLES IN bronze_db;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create silver_db

-- COMMAND ----------

-- CREATE DATABASE IF NOT EXISTS silver_db
-- LOCATION 'dbfs:/mnt/decoursesa/silver';

-- COMMAND ----------

SHOW TABLES IN silver_db;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create gold_db

-- COMMAND ----------

-- CREATE DATABASE IF NOT EXISTS gold_db
-- LOCATION 'dbfs:/mnt/decoursesa/gold';

-- COMMAND ----------

SHOW TABLES IN gold_db;
