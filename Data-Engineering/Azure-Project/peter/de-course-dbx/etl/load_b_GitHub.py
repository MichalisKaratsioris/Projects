# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, DateType, TimestampType, LongType
from pyspark.sql.functions import current_timestamp, col, to_timestamp, lit, to_date

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
    
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../setup/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ↑github_df = add_ingestion_date(github_df)↑
# MAGIC #### Step 1 - read JSON file

# COMMAND ----------

github_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.json(f"/mnt/gfaproject/landing/GitHub/githubarchive_day_{v_file_date}.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - schema processing

# COMMAND ----------

GitHub_schema = StructType(fields=[
    StructField('actor', StructType([
                            StructField("avatar_url", StringType(), True),
                            StructField("gravatar_id", StringType(), True),
                            StructField("id", StringType(), True),
                            StructField("login", StringType(), True),
                            StructField("url", StringType(), True)
                        ])),
    
                          StructField("created_at", TimestampType(), True),
                          StructField("id", StringType(), True),

                            StructField("org", StructType([
                                StructField("avatar_url", StringType(), True),
                                StructField("gravatar_id", StringType(), True),
                                StructField("id", StringType(), True),
                                StructField("login", StringType(), True),
                                StructField("url", StringType(), True)
                        ])),

                          StructField("other", StringType(), True),
                          StructField("public", BooleanType(), True),

                            StructField("repo", StructType([
                                StructField("id", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("url", StringType(), True)
                        ])),

                          StructField("type", StringType(), True),
])

# COMMAND ----------

github_df = spark.read \
.option("header", True) \
.schema(GitHub_schema) \
.json(f"/mnt/gfaproject/landing/GitHub/githubarchive_day_{v_file_date}.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename columns + timestamp load

# COMMAND ----------

github_df = github_df.withColumnRenamed("created_at", "created_at_datetime_utc")
#github_df = github_df.withColumn("_pk", col("id"))
github_df = github_df.withColumnRenamed("public", "is_public")
github_df = add_ingestion_date(github_df)
#github_df = github_df.withColumn("loaded_at_datetime_utc", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Extract and flat the columns + put valid date + change type to long + make _pk column

# COMMAND ----------

github_df_flat = github_df \
.withColumn("actor_id", col("actor.id").cast(IntegerType())) \
.withColumn("actor_login", col("actor.login")) \
.withColumn("actor_gravatar_id", col("actor.gravatar_id")) \
.withColumn("actor_avatar_url", col("actor.avatar_url")) \
.withColumn("actor_url", col("actor.url")) \
.withColumn("org_id", col("org.id").cast(IntegerType())) \
.withColumn("org_login", col("org.login")) \
.withColumn("org_gravatar_id", col("org.gravatar_id")) \
.withColumn("org_avatar_url", col("org.avatar_url")) \
.withColumn("org_url", col("org.url")) \
.withColumn("repo_id", col("repo.id").cast(IntegerType())) \
.withColumn("repo_name", col("repo.name")) \
.withColumn("repo_url", col("repo.url")) \
.withColumn("valid_date", to_date(lit(v_file_date), "yyyyMMdd")) \
.withColumn("id", col("id").cast(LongType())) \
.withColumn("_pk", col("id").cast(LongType())) \
.drop("repo", "actor", "org")

#github_df = github_df.withColumn("_pk", col("id"))

# COMMAND ----------



# COMMAND ----------

display(github_df_flat)
github_df_flat.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - write CSV file to DELTA

# COMMAND ----------

#%sql

#CREATE TABLE IF NOT EXISTS bronze_db.b_github
#USING DELTA
#LOCATION "/mnt/gfaproject/bronze/b_github";

# COMMAND ----------

#%sql
#DROP TABLE bronze_db.b_github;

# COMMAND ----------

github_df_flat.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("bronze_db.b_github")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_db.b_github

# COMMAND ----------

#%sql
#SELECT * FROM bronze_db.b_github
#WHERE _pk = "20483608706"

# COMMAND ----------


