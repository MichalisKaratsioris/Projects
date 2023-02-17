# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
from pyspark.sql.functions import current_timestamp, to_utc_timestamp
from pyspark.sql.functions import col, lit, when, flatten, split, monotonically_increasing_id
from delta.tables import *

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

# COMMAND ----------

file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

Sampledata = spark.read.format("delta").load(f"dbfs:/mnt/bronze/b_stackoverflowquestions")

# COMMAND ----------

deltainstance = DeltaTable.forPath(spark, f"dbfs:/mnt/bronze/bronze_db.b_stackoverflowquestions");

# COMMAND ----------

display(deltainstance.toDF())

# COMMAND ----------

df = deltainstance.toDF()

# COMMAND ----------

df1 = df.withColumn('auto_increment', monotonically_increasing_id()) 

# COMMAND ----------

df3 = df1.withColumn('score',when(df1.auto_increment <= 999,"80085").otherwise(df1.score))

# COMMAND ----------

display(df3)

# COMMAND ----------

if(file_date == 20220731):
    df = df3
else:
    df = df
