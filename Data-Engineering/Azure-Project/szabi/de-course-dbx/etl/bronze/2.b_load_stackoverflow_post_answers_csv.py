# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, current_timestamp, to_utc_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC Using child notebook:

# COMMAND ----------

# MAGIC %run "../../setup/configuration"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/landing/stackoverflow

# COMMAND ----------

df_stackoverflow_post_answers = spark.read.csv(f"{landing_folder_path}/stackoverflow/stackoverflow_post_answers.csv")

# COMMAND ----------

display(df_stackoverflow_post_answers)

# COMMAND ----------

# MAGIC %md
# MAGIC Infering the input schema automatically:

# COMMAND ----------

df_stackoverflow_post_answers = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.csv(f"{landing_folder_path}/stackoverflow/stackoverflow_post_answers.csv")

# option - inferSchema: for big dataset not a good solution ( needs to go through the whole dataset) -> we need to specify the schema what dataframe will use later on!

# COMMAND ----------

display(df_stackoverflow_post_answers)

# COMMAND ----------

# MAGIC %md
# MAGIC Whenever we are not sure about data type, we can check min,max,mean etc. value of the columns:

# COMMAND ----------

# display(df_stackoverflow_post_answers.describe().show())

# COMMAND ----------

# MAGIC %md
# MAGIC Specifying schema for the dataframe:

# COMMAND ----------

stackoverflow_post_answers_schema = StructType(fields=[StructField("id", IntegerType(), False), # as it is a primary key: nullable=False
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

df_stackoverflow_post_answers = spark.read \
.option("header", True) \
.schema(stackoverflow_post_answers_schema) \
.csv(f"{landing_folder_path}/stackoverflow/stackoverflow_post_answers.csv")

# COMMAND ----------

display(df_stackoverflow_post_answers)

# COMMAND ----------

df_stackoverflow_post_answers.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC - Adding
# MAGIC     - UTC timestamp column
# MAGIC     - "_pk" column (cloned from "id" column)
# MAGIC - Renaming date columns

# COMMAND ----------

df_stackoverflow_post_answers = df_stackoverflow_post_answers \
.withColumn("load_date_datetime_utc", to_utc_timestamp(current_timestamp(),'UTC')) \
.withColumn("_pk", col("id")) \
.withColumnRenamed("community_owned_date", "community_owned_date_datetime_utc") \
.withColumnRenamed("creation_date", "creation_date_datetime_utc") \
.withColumnRenamed("last_activity_date", "last_activity_date_datetime_utc") \
.withColumnRenamed("last_edit_date", "last_edit_date_datetime_utc")

# COMMAND ----------

display(df_stackoverflow_post_answers)

# COMMAND ----------

df_stackoverflow_post_answers.write.format("delta").mode("overwrite").save(f"{bronze_folder_path}/b_stackoverflow_post_answers")

# as "df_stackoverflow_post_answers" dataframe has different schemas than the created empty delta table
# I had to use: ".option("overwriteSchema", "true")"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS bronze_db.b_stackoverflow_post_answers
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/bronze/b_stackoverflow_post_answers'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- ALTER TABLE bronze_db.b_stackoverflow_post_answers SET TBLPROPERTIES (
# MAGIC --                               'delta.columnMapping.mode' = 'name',
# MAGIC --                               'delta.minReaderVersion' = '2',
# MAGIC --                               'delta.minWriterVersion' = '5')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM bronze_db.b_stackoverflow_post_answers

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC  -- DROP TABLE IF EXISTS bronze_db.b_stackoverflow_post_answers

# COMMAND ----------

dbutils.notebook.exit("This notebook successfully run and finished!")
