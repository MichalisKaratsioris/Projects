# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, DateType, TimestampType, LongType
from pyspark.sql.functions import current_timestamp, col, to_timestamp, lit, split

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read bronze table

# COMMAND ----------

#%sql
#SELECT * FROM bronze_db.b_github
#WHERE valid_date = "20220731"

# COMMAND ----------

silver_github_df = spark.read.format("delta").load(f"/mnt/gfaproject/bronze/b_github/")

# COMMAND ----------

silver_github_df = silver_github_df \
    .withColumn("repository_account", split(silver_github_df.repo_name, "/").getItem(0)) \
    .withColumn("repository_name", split(silver_github_df.repo_name, "/").getItem(1)) \
    .withColumnRenamed("actor_id", "user_id") \
    .withColumnRenamed("id", "event_id")

silver_github_df = add_ingestion_date(silver_github_df)
silver_github_df = silver_github_df \
    .withColumnRenamed("loaded_at_datetime_utc", "dbx_created_at_datetime_utc")

silver_github_df = silver_github_df \
    .select("_pk", \
            "repository_account", \
            "repository_name", \
            "user_id", \
            "event_id", \
            "type", \
            "created_at_datetime_utc", \
            "valid_date",
            "dbx_created_at_datetime_utc")

# COMMAND ----------

display(silver_github_df)
silver_github_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to DELTA silver

# COMMAND ----------

silver_github_df.write.mode("overwrite").format("delta").option("overwriteSchema", True).option("partitionOverwriteMode", "dynamic").partitionBy("valid_date").saveAsTable("silver_db.s_github")

# COMMAND ----------

#%sql
#SHOW TABLES IN silver_db;

# COMMAND ----------

#%sql
#SHOW databases;

# COMMAND ----------

#%sql
#SELECT * FROM silver_db.s_github
#WHERE _pk = "20483608706"

# COMMAND ----------

#%sql
#SELECT * FROM silver_db.s_github
#WHERE valid_date = "20220930"
