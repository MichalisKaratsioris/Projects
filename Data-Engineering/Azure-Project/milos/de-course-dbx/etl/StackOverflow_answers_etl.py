# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import col

# COMMAND ----------

#answers_df = spark.read \
#.option("header", True) \
#.option("inferSchema", True) \
#.csv(f"dbfs:/mnt/landing/stackoverflow/stackoverflow_post_questions.csv")

# COMMAND ----------

#display(answers_df)

# COMMAND ----------

#answers_df.printSchema()

# COMMAND ----------

schema = StructType(fields= [StructField("id", IntegerType(), False), 
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

answers_df = spark.read \
.option("header", True) \
.schema(schema) \
.csv(f"dbfs:/mnt/landing/stackoverflow/stackoverflow_post_questions.csv")

# COMMAND ----------

answers_df.printSchema()

# COMMAND ----------

df = answers_df.withColumnRenamed("id","_pk")

# COMMAND ----------

df_w_timestamp = df.withColumn("loaded_datetime_utc", current_timestamp())

# COMMAND ----------

df_transform = df_w_timestamp.withColumnRenamed("community_owned_date","community_owned_date_datetime_utc") \
                           .withColumnRenamed("creation_date","creation_date_datetime_utc") \
                           .withColumnRenamed("last_activity_date","last_activity_date_datetime_utc")\
                           .withColumnRenamed("last_edit_date","last_edit_date_datetime_utc")

# COMMAND ----------

#display(df_transform)

# COMMAND ----------

df_final = df_transform.select(
    col("_pk"),
    col("title"),
    col("accepted_answer_id"),
    col("answer_count"),
    col("comment_count"),
    col("community_owned_date_datetime_utc"),
    col("creation_date_datetime_utc"),
    col("favorite_count"),
    col("last_activity_date_datetime_utc"),
    col("last_edit_date_datetime_utc"),
    col("last_editor_display_name"),
    col("last_editor_user_id"),
    col("owner_display_name"),
    col("owner_user_id"),
    col("parent_id"),
    col("post_type_id"),
    col("score"),
    col("tags"),
    col("view_count"),
    col("loaded_datetime_utc"),
)

# COMMAND ----------

df_final.write.mode("overwrite").format("delta").saveAsTable("bronze_db.b_stackoverflowAnswers")
