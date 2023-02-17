# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, DateType, TimestampType
from pyspark.sql.functions import current_timestamp, col, to_date, lit

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - read CSV file

# COMMAND ----------

answers_df = spark.read \
.option("header", True) \
.csv('dbfs:/mnt/gfaproject/landing/stackoverflow/stackoverflow_post_answers.csv')

# COMMAND ----------

display(answers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - schema processing

# COMMAND ----------

answers_schema = StructType(fields=[StructField("id", IntegerType(), False),
                                      StructField("title", StringType(), True),
                                      StructField("accepted_answer_id", IntegerType(), True),
                                      StructField("answer_count", IntegerType(), True),
                                      StructField("comment_count", IntegerType(), True),
                                      StructField("community_owned_date", DateType(), True),
                                      StructField("creation_date", TimestampType(), True),
                                      StructField("favorite_count", IntegerType(), True),
                                      StructField("last_activity_date", DateType(), True),
                                      StructField("last_edit_date", DateType(), True),
                                      StructField("last_editor_display_name", StringType(), True),
                                      StructField("last_editor_user_id", IntegerType(), True),
                                      StructField("owner_display_name", StringType(), True),
                                      StructField("owner_user_id", IntegerType(), True),
                                      StructField("parent_id", IntegerType(), True),
                                      StructField("post_type_id", IntegerType(), True),
                                      StructField("score", IntegerType(), True),
                                      StructField("tags", StringType(), True),
                                      StructField("view_count", IntegerType(), True),
])

# COMMAND ----------

answers_df = spark.read \
.option("header", True) \
.schema(answers_schema) \
.csv('dbfs:/mnt/gfaproject/landing/stackoverflow/stackoverflow_post_answers.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename columns

# COMMAND ----------

answers_df_renamed = answers_df.withColumn("_pk", col("id")) \
.withColumnRenamed("creation_date", "created_at_datetime_utc") \
.withColumnRenamed("last_activity_date", "last_activity_date_datetime_utc") \
.withColumnRenamed("last_edit_date", "last_edit_date_datetime_utc") \
.withColumnRenamed("community_owned_date", "community_owned_date_datetime_utc")

# COMMAND ----------

display(answers_df_renamed)

# COMMAND ----------

answers_final_df = answers_df_renamed.withColumn("valid_date_datetime_utc", to_date(lit(v_file_date), "yyyyMMdd"))

# COMMAND ----------

display(answers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - write CSV file to Delta

# COMMAND ----------

answers_final_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("bronze_db.b_stackoverflow_answers")

# COMMAND ----------

#filter the datasets: where last_activity_datetime_utc <= p_load_date
#▪ pick a column and change its value on 1000 rows when it is running with the first two load date parameters (the last, ‘2022-09-30’ dataset should contain the original data in it)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE bronze_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CURRENT_DATABASE();

# COMMAND ----------

#%sql
#UPDATE bronze_db.b_stackoverflow_answers
#SET last_activity_datetime_utc = '2022-09-30'
#WHERE in (SELECT TOP 100 * FROM bronze_db.b_stackoverflow_answers);

# COMMAND ----------

#%fs
#ls dbfs:/mnt/gfaproject/bronze/stackoverflow/answers/

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_db.b_stackoverflow_answers
