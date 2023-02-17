# Databricks notebook source
# MAGIC %md
# MAGIC General data modeling tips:
# MAGIC - Create a primary key in every table, called _pk.
# MAGIC - Make sure that there is a timestamp column which indicates the creation or load datetime of each record.
# MAGIC - Make sure that every datetime is in UTC format.
# MAGIC - Add _datetime_utc postfix to every datetime column name.
# MAGIC - Add is_ prefix to every boolean type (‘Yes’/’No’, 1/0) column name and convert it to boolean (true/false).
# MAGIC - Add a valid_date column to every table which contains the load date information.
# MAGIC - prefix the table names with the first letter of the table’s layer (for exam-ple “b_github” for bronze github table)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, DateType, TimestampType
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - read CSV file

# COMMAND ----------

questions_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.csv('dbfs:/mnt/gfaproject/landing/stackoverflow/stackoverflow_post_questions.csv')

# COMMAND ----------

#display(dbutils.fs.mounts())

# COMMAND ----------

#%fs
#ls dbfs:/mnt/gfaproject/landing/stackoverflow/

# COMMAND ----------

#display(questions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - schema processing

# COMMAND ----------

questions_schema = StructType(fields=[StructField("id", IntegerType(), False),
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

questions_df = spark.read \
.option("header", True) \
.schema(questions_schema) \
.csv('dbfs:/mnt/gfaproject/landing/stackoverflow/stackoverflow_post_questions.csv')

# COMMAND ----------

#questions_df.printSchema()
#questions_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename columns

# COMMAND ----------

questions_df_renamed = questions_df.withColumn("_pk", col("id")) \
.withColumnRenamed("creation_date", "creation_date_datetime_utc") \
.withColumnRenamed("last_activity_date", "last_activity_date_datetime_utc") \
.withColumnRenamed("last_edit_date", "last_edit_date_datetime_utc") \
.withColumnRenamed("community_owned_date", "community_owned_date_datetime_utc")

# COMMAND ----------

#display(questions_df_renamed)

# COMMAND ----------

questions_final_df = questions_df_renamed.withColumn("valid_date_datetime_utc", current_timestamp())

# COMMAND ----------

#display(questions_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - write CSV file to DELTA

# COMMAND ----------

questions_final_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("bronze_db.b_stackoverflow_questions")

# COMMAND ----------

#%fs
#ls
#ls dbfs:/mnt/gfaproject/bronze/stackoverflow/questions/

# COMMAND ----------

#new_df = spark.read.parquet("dbfs:/mnt/gfaproject/bronze/b_stackoverflow_questions")

# COMMAND ----------

#display(new_df)

# COMMAND ----------

#%sql
#SELECT * FROM bronze_db.b_stackoverflow_questions
