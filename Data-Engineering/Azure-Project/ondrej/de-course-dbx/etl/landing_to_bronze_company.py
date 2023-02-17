# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql.functions import col, split, current_timestamp, lit, to_date, hash

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create a widget for the file date parameter

# COMMAND ----------

# dbutils.widgets.text("p_file_date", "")

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read the JSON file using the spark dataframe reader

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# display(dbutils.fs.ls(f"/mnt/decoursesa/landing/company_detail/{v_file_date[:4]}/{v_file_date[4:6]}/{v_file_date[6:]}"))

# COMMAND ----------

# df = spark.read.json(f"/mnt/decoursesa/landing/company_detail/{v_file_date[:4]}/{v_file_date[4:6]}/{v_file_date[6:]}/company_detail_{v_file_date}.json")

# COMMAND ----------

# display(df)

# COMMAND ----------

company_schema = StructType(fields=[
    StructField("Organization", StringType(), False),
    StructField("Repository account", StringType(), True),
    StructField("Repository name", StringType(), True),
    StructField("L1 type", StringType(), True),
    StructField("L2 type", StringType(), True),
    StructField("L3 type", StringType(), True),
    StructField("Tags", StringType(), True),
    StructField("Open source available", StringType(), True)
])

# COMMAND ----------

df = spark.read \
.schema(company_schema) \
.json(f"/mnt/decoursesa/landing/company_detail/{v_file_date[:4]}/{v_file_date[4:6]}/{v_file_date[6:]}/company_detail_{v_file_date}.json")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Convert string to array column

# COMMAND ----------

array_df = df.withColumn("tags_array", split(col("Tags"), ", "))

# COMMAND ----------

display(array_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cast string type to boolean type

# COMMAND ----------

bool_df = array_df.withColumn("is_open_source_available",col("Open source available").cast(BooleanType()))

# COMMAND ----------

display(bool_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop the columns

# COMMAND ----------

dropped_df = bool_df.drop("Tags", "Open source available")

# COMMAND ----------

display(dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rename the columns

# COMMAND ----------

renamed_df = dropped_df \
.withColumnRenamed("Organization", "organization_name") \
.withColumnRenamed("Repository account", "repository_account")\
.withColumnRenamed("Repository name", "repository_name")\
.withColumnRenamed("L1 type", "l1_type")\
.withColumnRenamed("L2 type", "l2_type")\
.withColumnRenamed("L3 type", "l3_type")

# COMMAND ----------

display(renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add new columns

# COMMAND ----------

added_df = renamed_df \
.withColumn("_pk", hash("organization_name")) \
.withColumn("loaded_at_datetime_utc", current_timestamp()) \
.withColumn("valid_date", to_date(lit(v_file_date), "yyyyMMdd"))

# COMMAND ----------

display(added_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write data to delta table

# COMMAND ----------

added_df.write \
.option("overwriteSchema", True) \
.mode("overwrite") \
.format("delta") \
.saveAsTable("bronze_db.b_company")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED bronze_db.b_company;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze_db.b_company
# MAGIC LIMIT 10;

# COMMAND ----------

dbutils.notebook.exit(f"Success for file date parameter {v_file_date}")
