# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
from pyspark.sql.functions import current_timestamp, to_utc_timestamp
from pyspark.sql.functions import col, lit, when, flatten, split
from delta.tables import *

# COMMAND ----------

#display(dbutils.fs.mounts())

# COMMAND ----------

#%fs
#ls /mnt/landing/stackoverflow


# COMMAND ----------


#questions_df = spark.read \
#.option("header", True) \
#.option("inferSchema", True) \
#.csv(f"dbfs:/mnt/landing/stackoverflow/stackoverflow_post_questions.csv")

# COMMAND ----------

#display(questions_df)

# COMMAND ----------

schema = StructType(fields=[StructField("id", IntegerType(), False), 
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

questions_df = spark.read \
.option("header", True) \
.schema(schema) \
.csv(f"dbfs:/mnt/landing/stackoverflow/stackoverflow_post_questions.csv")

# COMMAND ----------

df_stackoverflow_post_questions = questions_df \
.withColumn("load_date_datetime_utc", to_utc_timestamp(current_timestamp(),'UTC')) \
.withColumn("_pk", col("id")) \
.withColumnRenamed("community_owned_date", "community_owned_date_datetime_utc") \
.withColumnRenamed("creation_date", "creation_date_datetime_utc") \
.withColumnRenamed("last_activity_date", "last_activity_date_datetime_utc") \
.withColumnRenamed("last_edit_date", "last_edit_date_datetime_utc")

# COMMAND ----------

display(df_stackoverflow_post_questions)

# COMMAND ----------

df_stackoverflow_post_questions.write.format("delta").mode("overwrite").save(f"dbfs:/mnt/bronze/bronze_db.b_stackoverflowquestions")
