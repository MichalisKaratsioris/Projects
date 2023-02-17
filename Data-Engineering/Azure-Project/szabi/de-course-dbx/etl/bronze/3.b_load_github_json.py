# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import current_timestamp, to_utc_timestamp, to_date, col, lit

# COMMAND ----------

# MAGIC %md
# MAGIC Setting up a widget:

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating widget name, with empty value:

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

# COMMAND ----------

# MAGIC %md
# MAGIC Taking this parameter into a variable:

# COMMAND ----------

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Testing the variable:

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../../setup/configuration"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/landing/github

# COMMAND ----------

df_github = spark.read.json(f"{landing_folder_path}/github/githubarchiveday_{v_file_date}.json")

# COMMAND ----------

display(df_github)

# COMMAND ----------

# df_github.describe().show()

# COMMAND ----------

inner_avatar_schema = StructType(fields=[StructField("avatar_url", StringType(), True),
                                        StructField("gravatar_id", StringType(), True),
                                        StructField("id", StringType(), True),
                                        StructField("login", StringType(), True),
                                        StructField("url", StringType(), True)
                                        ])

# COMMAND ----------

# inner_actor_schema = StructType(fields=[StructField("display_login", StringType(), True)])

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
.json(f"{landing_folder_path}/github/githubarchiveday_{v_file_date}.json")

# COMMAND ----------

display(df_github_schema)


# COMMAND ----------

# df_github_new_schema = df_github_schema.withColumn("id", col("id").cast(IntegerType()))

# COMMAND ----------

# display(df_github_new_schema)

# COMMAND ----------

df_github_schema = df_github_schema \
.withColumnRenamed("created_at","created_at_datetime_utc") \
.withColumnRenamed("public","is_public") \
    .withColumn("load_date_datetime_utc", to_utc_timestamp(current_timestamp(),'UTC')) \
    .withColumn("valid_date", to_date(lit(v_file_date), "yyyyMMdd")) \
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



# with "lit" function we convert text type variable into column type

# COMMAND ----------

display(df_github_schema)

# COMMAND ----------

# Wanted to run "Create a delta table" command:
df_github_schema.write.format("delta").mode("overwrite").save(f"{bronze_folder_path}/b_github")
# Got an error and a description how to solve it
# Followed that description in the next 2 cmd's

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS bronze_db.b_github
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/bronze/b_github'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- ALTER TABLE bronze_db.b_github SET TBLPROPERTIES (
# MAGIC --                               'delta.columnMapping.mode' = 'name',
# MAGIC --                               'delta.minReaderVersion' = '2',
# MAGIC --                               'delta.minWriterVersion' = '5')

# COMMAND ----------

# df_github_schema.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{bronze_folder_path}/b_github")

# as "df_stackoverflow_post_questions" dataframe has different schemas than the created empty delta table
# I had to use: ".option("overwriteSchema", "true")"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM bronze_db.b_github

# COMMAND ----------

dbutils.notebook.exit("This notebook successfully run and finished!")
