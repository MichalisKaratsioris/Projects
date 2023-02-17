# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
from pyspark.sql.functions import col, lit, split, current_timestamp, to_date, to_utc_timestamp

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
# MAGIC ls /mnt/landing/company_detail

# COMMAND ----------

df_company_detail = spark.read.json(f"{landing_folder_path}/company_detail/company_detail_{v_file_date}.json")

# COMMAND ----------

display(df_company_detail)

# COMMAND ----------

df_company_detail.describe().show()

# COMMAND ----------

company_detail_schema = StructType(fields=[StructField("L1 type", StringType(), True),
                                   StructField("L2 type", StringType(), True),
                                   StructField("L3 type", StringType(), True),
                                   StructField("Open source available", StringType(), True),
                                   StructField("Organization", StringType(), True),
                                   StructField("Repository account", StringType(), True),
                                   StructField("Repository name", StringType(), True),
                                   StructField("Tags", StringType(), True)
                                   ])

# COMMAND ----------

df_company_detail_schema = spark.read \
.schema(company_detail_schema) \
.json(f"{landing_folder_path}/company_detail/company_detail_{v_file_date}.json")

# COMMAND ----------

df_company_detail_schema.printSchema()

# COMMAND ----------

display(df_company_detail_schema)

# COMMAND ----------

df_company_detail_schema = df_company_detail_schema

df_company_detail_schema = df_company_detail_schema \
.withColumn("Open source available", col("Open source available").cast(BooleanType())) \
.withColumn("load_date_datetime_utc", to_utc_timestamp(current_timestamp(),'UTC')) \
.withColumn("valid_date", to_date(lit(v_file_date), "yyyyMMdd")) \
.withColumn("_pk", col("Organization")) \
    .withColumnRenamed("Open source available","is_open_source_available") \
    .withColumnRenamed("L1 type","l1_type") \
    .withColumnRenamed("L2 type","l2_type") \
    .withColumnRenamed("L3 type","l3_type") \
    .withColumnRenamed("Organization","organization") \
    .withColumnRenamed("Repository account","repository_account") \
    .withColumnRenamed("Repository name","repository_name") \
    .withColumnRenamed("Tags","tags") \
# with "lit" function we convert text type variable into column type

# COMMAND ----------

display(df_company_detail_schema)

# COMMAND ----------

df_company_detail_schema = df_company_detail_schema \
.select("*", split(col("tags"),",") \
        .alias("tags_array")) \
        .drop("tags")

# COMMAND ----------

display(df_company_detail_schema)

# COMMAND ----------

# Wanted to run "Create a delta table" command:
df_company_detail_schema.write.format("delta").mode("overwrite").save(f"{bronze_folder_path}/b_company_detail")
# Got an error and a description how to solve it
# Followed that description in the next 2 cmd's

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS bronze_db.b_company_detail
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/bronze/b_company_detail'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- ALTER TABLE bronze_db.b_company_detail SET TBLPROPERTIES (
# MAGIC --                               'delta.columnMapping.mode' = 'name',
# MAGIC --                               'delta.minReaderVersion' = '2',
# MAGIC --                               'delta.minWriterVersion' = '5') 

# COMMAND ----------

# df_company_detail_schema.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{bronze_folder_path}/b_company_detail")

# as "df_stackoverflow_post_questions" dataframe has different schemas than the created empty delta table
# I had to use: ".option("overwriteSchema", "true")"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM bronze_db.b_company_detail
# MAGIC WHERE organization = "Airbyte"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS bronze_db.b_company_detail

# COMMAND ----------

dbutils.notebook.exit(f"This notebook successfully run and finished by {v_file_date}!")
