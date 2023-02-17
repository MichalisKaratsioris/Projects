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
from pyspark.sql.functions import current_timestamp, col, to_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - read JSON file

# COMMAND ----------

#display(dbutils.fs.mounts())
#display(dbutils.fs.ls("/mnt/gfaproject/landing/GitHub/"))

# COMMAND ----------

github_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.json("/mnt/gfaproject/landing/GitHub/githubarchive_day_20220731.json")

# COMMAND ----------

display(github_df)
github_df.printSchema()

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
                          StructField("id", IntegerType(), True),

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
.json("/mnt/gfaproject/landing/GitHub/githubarchive_day_20220731.json")

# COMMAND ----------

display(github_df)
github_df.printSchema()

# COMMAND ----------

#github_df = github_df.withColumn("created_at", to_timestamp("created_at"))
#github_df = github_df.withColumn("id", lit("id").cast("Int"))

#github_df = github_df.withColumn("id", github_df.id.cast(IntegerType))
#github_df = github_df.withColumn("id", col("id").cast("Int"))



# COMMAND ----------

github_df = github_df.withColumnRenamed("created_at", ("created_at_datetime_utc"))
github_df = github_df.withColumn("_pk", col("id"))



# COMMAND ----------

display(github_df)
github_df.printSchema()

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

questions_df.printSchema()

# COMMAND ----------

questions_df.describe().show()

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

display(questions_df_renamed)

# COMMAND ----------

questions_final_df = questions_df_renamed.withColumn("valid_date_datetime_utc", current_timestamp())

# COMMAND ----------

display(questions_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - write CSV file to DELTA

# COMMAND ----------

questions_final_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("bronze_db.b_stackoverflow_questions")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls
# MAGIC #ls dbfs:/mnt/gfaproject/bronze/stackoverflow/questions/

# COMMAND ----------

#new_df = spark.read.parquet("dbfs:/mnt/gfaproject/bronze/b_stackoverflow_questions")

# COMMAND ----------

#display(new_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_db.b_stackoverflow_questions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import col, lit, when, flatten

peopleDF = spark.read.json("/mnt/gfaproject/landing/GitHub/githubarchive_day_20220731.json")

changedTypedf = peopleDF.withColumn("actor.avatar_url", peopleDF["actor.avatar_url"].cast(StringType()))
changedTypedf = peopleDF.withColumn("actor.gravatar_id", changedTypedf["actor.gravatar_id"].cast(IntegerType()))
changedTypedf = peopleDF.withColumn("actor.id", changedTypedf["actor.id"].cast(IntegerType()))
changedTypedf = peopleDF.withColumn("actor.login", changedTypedf["actor.login"].cast(StringType()))
changedTypedf = peopleDF.withColumn("actor.url", changedTypedf["actor.url"].cast(StringType()))
changedTypedf = peopleDF.withColumn("created_at", changedTypedf["created_at"].cast(TimestampType()))
changedTypedf = peopleDF.withColumn("id", changedTypedf["id"].cast(IntegerType()))
changedTypedf = peopleDF.withColumn("org.avatar_url", changedTypedf["org.avatar_url"].cast(StringType()))
changedTypedf = peopleDF.withColumn("org.gravatar_id", changedTypedf["org.gravatar_id"].cast(IntegerType()))
changedTypedf = peopleDF.withColumn("org.id", changedTypedf["org.id"].cast(IntegerType()))
changedTypedf = peopleDF.withColumn("org.login", changedTypedf["org.login"].cast(StringType()))
changedTypedf = peopleDF.withColumn("org.url", changedTypedf["org.url"].cast(StringType()))
changedTypedf = peopleDF.withColumn("other", changedTypedf["other"].cast(StringType()))
changedTypedf = peopleDF.withColumn("public", changedTypedf["public"].cast(StringType()))
changedTypedf = peopleDF.withColumn("repo.id", changedTypedf["repo.id"].cast(IntegerType()))
changedTypedf = peopleDF.withColumn("repo.name", changedTypedf["repo.name"].cast(StringType()))
changedTypedf = peopleDF.withColumn("repo.url", changedTypedf["repo.url"].cast(StringType()))


# COMMAND ----------

display(changedTypedf)

# COMMAND ----------

changedTypedf.printSchema()

# COMMAND ----------

tranformation_df = changedTypedf \
.withColumn("load_datetime_utc", current_timestamp()) \
.withColumn("valid_date", lit("20220731")) \
.withColumnRenamed("created_at","created_at_datetime_utc")

# COMMAND ----------

display(tranformation_df)

# COMMAND ----------

field = "public"
conditions_all = (when(col(field) == "Yes", True)
    .when(col(field) == "No", False)
    .when(col(field) == "1", True)
    .when(col(field) == "0", False)
    .otherwise(col(field))
)

# COMMAND ----------

sector_df = tranformation_df.withColumn(field, conditions_all)

# COMMAND ----------

changedTypedf = changedTypedf.withColumnRenamed("id","_pk")
df_timestamp = changedTypedf.withColumn("loaded_datetime_utc", current_timestamp())

# COMMAND ----------

df_final = changedTypedf.select(
    col("actor.avatar_url").alias("actor_avatar_url"),
    col("actor.gravatar_id").alias("actor_gravatar_id"),
    col("actor.id").alias("actor_id"),
    col("actor.login").alias("actor_login"),
    col("actor.url").alias("actor_url"),
    col("created_at"),
    col("_pk"),
    col("org.avatar_url").alias("org_avatar_url"),
    col("org.gravatar_id").alias("org_gravatar_id"),
    col("other"),
    col("public"),
    col("repo.id").alias("repo_id"),
    col("repo.name").alias("repo_login"),
    col("repo.url").alias("repo_url"),
)

# COMMAND ----------

display(df_final)
df_final.printSchema()

# COMMAND ----------


