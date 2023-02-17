# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, split, current_timestamp, lit, to_date

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
# MAGIC ###Read the CSV file using the spark dataframe reader

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# display(dbutils.fs.ls("/mnt/decoursesa/landing/stackoverflow"))

# COMMAND ----------

# df = spark.read \
# .option("header", True) \
# .csv("/mnt/decoursesa/landing/stackoverflow/stackoverflow_post_questions.csv")

# COMMAND ----------

# display(df)

# COMMAND ----------

questions_schema = StructType(fields=[
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("accepted_answer_id", IntegerType(), True),
    StructField("answer_count", IntegerType(), True),
    StructField("comment_count", IntegerType(), True),
    StructField("community_owned_date", TimestampType(), True),
    StructField("creation_date", TimestampType(), True),
    StructField("favorite_count", IntegerType(), True),
    StructField("last_activity_date", TimestampType(), True),
    StructField("last_edit_date", TimestampType(), True),
    StructField("last_editor_display_name", StringType(), True),
    StructField("last_editor_user_id", IntegerType(), True),
    StructField("owner_display_name", StringType(), True),
    StructField("owner_user_id", IntegerType(), True),
    StructField("parent_id", IntegerType(), True),
    StructField("post_type_id", IntegerType(), True),
    StructField("score", IntegerType(), True),
    StructField("tags", StringType(), True),
    StructField("view_count", IntegerType(), True)
])

# COMMAND ----------

df = spark.read \
.option("header", True) \
.schema(questions_schema) \
.csv("/mnt/decoursesa/landing/stackoverflow/stackoverflow_post_questions.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Convert string to array column

# COMMAND ----------

array_df = df.withColumn("tags_array", split(col("tags"), "\|"))

# COMMAND ----------

display(array_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop the column

# COMMAND ----------

dropped_df = array_df.drop("tags")

# COMMAND ----------

display(dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rename the columns

# COMMAND ----------

renamed_df = dropped_df \
.withColumnRenamed("community_owned_date", "community_owned_datetime_utc") \
.withColumnRenamed("creation_date", "created_at_datetime_utc") \
.withColumnRenamed("last_activity_date", "last_activity_datetime_utc") \
.withColumnRenamed("last_edit_date", "last_edit_datetime_utc")

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
# MAGIC ###Filter the source data

# COMMAND ----------

filtered_df = added_df.filter("created_at_datetime_utc <= valid_date")

# COMMAND ----------

display(filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write data to delta table

# COMMAND ----------

filtered_df.write \
.option("overwriteSchema", True) \
.mode("overwrite") \
.format("delta") \
.saveAsTable("bronze_db.b_stackoverflow_post_questions")

# COMMAND ----------

# %sql
# DESCRIBE EXTENDED bronze_db.b_stackoverflow_post_questions;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze_db.b_stackoverflow_post_questions
# MAGIC LIMIT 10;

# COMMAND ----------

dbutils.notebook.exit(f"Success for file date parameter {v_file_date}")
