# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import col, lit, when, flatten, to_utc_timestamp, to_date

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

# COMMAND ----------

file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

peopleDF = spark.read.json(f"dbfs:/mnt/landing/github/githubarchiveday_{file_date}.json")

# COMMAND ----------

#display(peopleDF)

# COMMAND ----------

peopleDF.printSchema()

# COMMAND ----------

#actor_schema = StructType(fields= [StructField("avatar_url", StringType(), True), 
                             #StructField("gravatar_id", IntegerType(), True),
                             #StructField("id", IntegerType(), False),
                             #StructField("answer_count", IntegerType(), True),
                             #StructField("comment_count", IntegerType(), True),
                            #])

# COMMAND ----------

inner_avatar_schema = StructType(fields=[StructField("avatar_url", StringType(), True),
                                        StructField("gravatar_id", StringType(), True),
                                        StructField("id", StringType(), True),
                                        StructField("login", StringType(), True),
                                        StructField("url", StringType(), True)
                                        ])

# COMMAND ----------

inner_id_schema = StructType(fields=[StructField("id", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("url", StringType(), True)
                                    ])

# COMMAND ----------

github_schema = StructType(fields=[StructField("actor", inner_avatar_schema),
                                   StructField("created_at", TimestampType(), True),
                                   StructField("id", StringType(), True),
                                   StructField("org", inner_avatar_schema),
                                   StructField("other", StringType(), True),
                                   StructField("public", BooleanType(), True),
                                   StructField("repo", inner_id_schema),
                                   StructField("type", StringType(), True)
                                   ])

# COMMAND ----------

df_github_schema = spark.read \
.schema(github_schema) \
.json(f"dbfs:/mnt/landing/github/githubarchiveday_{file_date}.json")


# COMMAND ----------

df_github_schema = df_github_schema \
.withColumnRenamed("created_at","created_at_datetime_utc") \
.withColumnRenamed("public","is_public") \
    .withColumn("load_date_datetime_utc", to_utc_timestamp(current_timestamp(),'UTC')) \
    .withColumn("valid_date", to_date(lit(file_date), "yyyyMMdd")) \
    .withColumn("_pk", col("id")) \
.withColumn("actor_avatar_url", col("actor.avatar_url")) \
.withColumn("actor_gravatar_id", col("actor.gravatar_id")) \
.withColumn("actor_id", col("actor.id").cast(IntegerType())) \
.withColumn("actor_login", col("actor.login")) \
.withColumn("actor_url", col("actor.url")) \
.withColumn("id", col("id").cast(LongType())) \
    .withColumn("org_avatar_url", col("org.avatar_url")) \
    .withColumn("org_gravatar_id", col("org.gravatar_id")) \
    .withColumn("org_id", col("org.id").cast(IntegerType())) \
    .withColumn("org_login", col("org.login")) \
    .withColumn("org_url", col("org.url")) \
.withColumn("repo_id", col("repo.id").cast(IntegerType())) \
.withColumn("repo_name", col("repo.name")) \
.withColumn("repo_url", col("repo.url")) \
    .drop("repo", "actor", "org")

# COMMAND ----------

display(df_github_schema)


# COMMAND ----------

df_github_schema.write.format("delta").mode("overwrite").save(f"dbfs:/mnt/bronze/b_github")
