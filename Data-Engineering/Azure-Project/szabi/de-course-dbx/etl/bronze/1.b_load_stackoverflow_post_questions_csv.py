# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, current_timestamp, to_utc_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC Using child notebook:

# COMMAND ----------

# MAGIC %run "../../setup/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC Testing whether child notebook folder-path works:

# COMMAND ----------

landing_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC Cmd 6-8: Figuring out the right path to read file:

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/landing

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/landing/stackoverflow

# COMMAND ----------

df_stackoverflow_post_questions = spark.read.csv(f"{landing_folder_path}/stackoverflow/stackoverflow_post_questions.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Checking type of red file:

# COMMAND ----------

type(df_stackoverflow_post_questions)

# COMMAND ----------

# MAGIC %md
# MAGIC Showing the first 20 records:

# COMMAND ----------

df_stackoverflow_post_questions.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Better solution than using "show" function:

# COMMAND ----------

display(df_stackoverflow_post_questions)

# COMMAND ----------

# MAGIC %md
# MAGIC Checking column/data types:

# COMMAND ----------

df_stackoverflow_post_questions.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Whenever we are not sure about data type, we can check min,max,mean etc. value of the columns:

# COMMAND ----------

# df_stackoverflow_post_questions.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Infering the input schema automatically:

# COMMAND ----------

df_stackoverflow_post_questions = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.csv(f"{landing_folder_path}/stackoverflow/stackoverflow_post_questions.csv")

# option - inferSchema: for big dataset not a good solution ( needs to go through the whole dataset) -> we need to specify the schema what dataframe will use later on!

# COMMAND ----------

display(df_stackoverflow_post_questions)

# COMMAND ----------

# MAGIC %md
# MAGIC Specifying schema for the dataframe:

# COMMAND ----------

stackoverflow_post_questions_schema = StructType(fields=[StructField("id", IntegerType(), False), # as it is a primary key: nullable=False
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

df_stackoverflow_post_questions = spark.read \
.option("header", True) \
.schema(stackoverflow_post_questions_schema) \
.csv(f"{landing_folder_path}/stackoverflow/stackoverflow_post_questions.csv")

# COMMAND ----------

display(df_stackoverflow_post_questions)

# COMMAND ----------

df_stackoverflow_post_questions.printSchema()

# COMMAND ----------

display(df_stackoverflow_post_questions)

# COMMAND ----------

# MAGIC %md
# MAGIC - Adding
# MAGIC     - UTC timestamp column
# MAGIC     - "_pk" column (cloned from "id" column)
# MAGIC - Renaming date columns

# COMMAND ----------

df_stackoverflow_post_questions = df_stackoverflow_post_questions \
.withColumn("load_date_datetime_utc", to_utc_timestamp(current_timestamp(),'UTC')) \
.withColumn("_pk", col("id")) \
.withColumnRenamed("community_owned_date", "community_owned_date_datetime_utc") \
.withColumnRenamed("creation_date", "creation_date_datetime_utc") \
.withColumnRenamed("last_activity_date", "last_activity_date_datetime_utc") \
.withColumnRenamed("last_edit_date", "last_edit_date_datetime_utc")

# df_stackoverflow_post_questions = df_stackoverflow_post_questions.withColumn("load_datetime", current_timestamp())

# COMMAND ----------

display(df_stackoverflow_post_questions)

# COMMAND ----------

# Wanted to run "Create a delta table" command:
df_stackoverflow_post_questions.write.format("delta").mode("overwrite").save(f"{bronze_folder_path}/b_stackoverflow_post_questions")
# Got an error and a description how to solve it
# Followed that description in the next 4 cmd's

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS bronze_db.b_stackoverflow_post_questions
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/bronze/b_stackoverflow_post_questions'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS bronze_db.b_stackoverflow_post_questions

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC -- ALTER TABLE bronze_db.b_stackoverflow_post_questions SET TBLPROPERTIES (
# MAGIC --                               'delta.columnMapping.mode' = 'name',
# MAGIC --                               'delta.minReaderVersion' = '2',
# MAGIC --                               'delta.minWriterVersion' = '5')

# COMMAND ----------

# df_stackoverflow_post_questions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{bronze_folder_path}/b_stackoverflow_post_questions")

# as "df_stackoverflow_post_questions" dataframe has different schemas than the created empty delta table
# I had to use: ".option("overwriteSchema", "true")"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM bronze_db.b_stackoverflow_post_questions

# COMMAND ----------

dbutils.notebook.exit("This notebook successfully run and finished!")
