# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
from pyspark.sql.functions import current_timestamp, to_utc_timestamp
from pyspark.sql.functions import col, lit, when, flatten, split
from delta.tables import *

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

# COMMAND ----------

file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

Sampledata = spark.read.format("delta").load(f"dbfs:/mnt/bronze/b_github")

# COMMAND ----------

deltainstance = DeltaTable.forPath(spark, f"dbfs:/mnt/bronze/b_github");

# COMMAND ----------

display(deltainstance.toDF())

# COMMAND ----------

df = deltainstance.toDF()

# COMMAND ----------

df1 = df.withColumn('repository_account', split(df['repo_name'], '/').getItem(0)) 
      

# COMMAND ----------

df1 = df1.withColumn('repository_name', split(df['repo_name'], '/').getItem(1))

# COMMAND ----------

df2 = df1.select(col("_pk"),
          col("repository_account"),
          col("repository_name"),
          col("actor_id").alias("user_id"),
          col("id").alias("event_id"),
          col("type"),
          col("load_date_datetime_utc").alias("created_at_datetime_utc"),
          col("valid_date"))
          

# COMMAND ----------

df2 = df2.withColumn("dbx_created_at_datetime_utc", to_utc_timestamp(current_timestamp(),'UTC'))

# COMMAND ----------

display(df2)

# COMMAND ----------

df2.write \
    .format("delta") \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("valid_date") \
    .save(f"dbfs:/mnt/silver/s_github")
