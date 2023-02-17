# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import col, current_timestamp, lit, to_date

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

# display(dbutils.fs.ls(f"/mnt/decoursesa/landing/github/{v_file_date[:4]}/{v_file_date[4:6]}/{v_file_date[6:]}"))

# COMMAND ----------

# df = spark.read.json(f"/mnt/decoursesa/landing/github/{v_file_date[:4]}/{v_file_date[4:6]}/{v_file_date[6:]}/github_{v_file_date}.json")

# COMMAND ----------

# display(df)

# COMMAND ----------

repo_schema = StructType(fields=[
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

actor_schema = StructType(fields=[
    StructField("id", StringType(), True),
    StructField("login", StringType(), True),
    StructField("gravatar_id", StringType(), True),
    StructField("avatar_url", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

github_schema = StructType(fields=[
    StructField("id", StringType(), False),
    StructField("type", StringType(), True),
    StructField("public", BooleanType(), True),
    StructField("repo", repo_schema),
    StructField("actor", actor_schema),
    StructField("org", actor_schema),
    StructField("created_at", TimestampType(), True),
    StructField("other", StringType(), True)
])

# COMMAND ----------

df = spark.read \
.schema(github_schema) \
.json(f"/mnt/decoursesa/landing/github/{v_file_date[:4]}/{v_file_date[4:6]}/{v_file_date[6:]}/github_{v_file_date}.json")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cast string type to long type

# COMMAND ----------

long_df = df.withColumn("id",col("id").cast(LongType()))

# COMMAND ----------

display(long_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read the nested fields

# COMMAND ----------

nested_df = long_df \
.withColumn("repo_id", col("repo.id").cast(IntegerType())) \
.withColumn("repo_name", col("repo.name")) \
.withColumn("repo_url", col("repo.url")) \
.withColumn("actor_id", col("actor.id").cast(IntegerType())) \
.withColumn("actor_login", col("actor.login")) \
.withColumn("actor_gravatar_id", col("actor.gravatar_id")) \
.withColumn("actor_avatar_url", col("actor.avatar_url")) \
.withColumn("actor_url", col("actor.url")) \
.withColumn("org_id", col("org.id").cast(IntegerType())) \
.withColumn("org_login", col("org.login")) \
.withColumn("org_gravatar_id", col("org.gravatar_id")) \
.withColumn("org_avatar_url", col("org.avatar_url")) \
.withColumn("org_url", col("org.url"))

# COMMAND ----------

display(nested_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop the columns

# COMMAND ----------

dropped_df = nested_df.drop("repo", "actor", "org")

# COMMAND ----------

display(dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rename the columns

# COMMAND ----------

renamed_df = dropped_df \
.withColumnRenamed("public", "is_public") \
.withColumnRenamed("created_at", "created_at_datetime_utc")

# COMMAND ----------

display(renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add new columns

# COMMAND ----------

added_df = renamed_df \
.withColumn("_pk", col("id")) \
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
.saveAsTable("bronze_db.b_github")

# COMMAND ----------

# %sql
# DESCRIBE EXTENDED bronze_db.b_github;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze_db.b_github
# MAGIC LIMIT 10;

# COMMAND ----------

dbutils.notebook.exit(f"Success for file date parameter {v_file_date}")
