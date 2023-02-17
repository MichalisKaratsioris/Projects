# Databricks notebook source
from pyspark.sql.functions import split, col, current_timestamp, to_utc_timestamp

# COMMAND ----------

# MAGIC %run "../../setup/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

# COMMAND ----------

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/bronze

# COMMAND ----------

silver_df = spark.read.format("delta").load(f"{bronze_folder_path}/b_github/")

# COMMAND ----------

display(silver_df)

# COMMAND ----------

silver_df = silver_df \
    .withColumn("repository_account", split(silver_df.repo_name, "/").getItem(0)) \
    .withColumn("repository_name", split(silver_df.repo_name, "/").getItem(1))

# COMMAND ----------

display(silver_df)

# COMMAND ----------

silver_df = silver_df \
    .select("_pk", \
            col("actor_id").alias("user_id"), \
            col("id").alias("event_id"), \
            "repository_account", \
            "repository_name", \
            "type", \
            "created_at_datetime_utc", \
            "valid_date") \
    .withColumn("dbx_created_at_datetime_utc", to_utc_timestamp(current_timestamp(),'UTC'))

# COMMAND ----------

display(silver_df)

# COMMAND ----------

# https://learn.microsoft.com/en-us/azure/databricks/delta/selective-overwrite

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("valid_date") \
    .save(f"{silver_folder_path}/s_github")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver_db.s_github
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/silver/s_github'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS silver_db.s_github

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM silver_db.s_github
# MAGIC WHERE valid_date = '2022-07-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT valid_date, COUNT(*)
# MAGIC FROM silver_db.s_github
# MAGIC GROUP BY valid_date

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT last_day(created_at_datetime_utc) AS creation_month, COUNT(*)
# MAGIC  FROM silver_db.s_github
# MAGIC  GROUP BY creation_month
# MAGIC  ORDER BY creation_month ASC

# COMMAND ----------

dbutils.notebook.exit("This notebook successfully run and finished!")
