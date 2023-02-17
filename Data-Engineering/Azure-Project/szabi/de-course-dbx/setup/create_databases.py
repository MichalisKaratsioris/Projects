# Databricks notebook source
# MAGIC %md
# MAGIC ### Creating database in SQL and in Python:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE bronze_db
# MAGIC LOCATION 'dbfs:/mnt/bronze/';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP database bronze_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE ...
# MAGIC DESC database extended bronze_db;

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("""
# MAGIC           CREATE DATABASE IF NOT EXISTS bronze_db
# MAGIC           LOCATION 'dbfs:/mnt/bronze/';
# MAGIC           """)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE ...
# MAGIC DESC database extended bronze_db;

# COMMAND ----------

spark.sql("""
          CREATE DATABASE IF NOT EXISTS silver_db
          LOCATION 'dbfs:/mnt/silver/';
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED silver_db;

# COMMAND ----------

spark.sql("""
          CREATE DATABASE IF NOT EXISTS gold_db
          LOCATION 'dbfs:/mnt/gold/';
          """)

# COMMAND ----------


