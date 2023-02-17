# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, DateType, TimestampType, ArrayType, LongType
from pyspark.sql.functions import current_timestamp, col, to_timestamp, lit, split, to_date
from pyspark.sql import SparkSession

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
    
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../setup/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ↑github_df = add_ingestion_date(github_df)↑
# MAGIC #### Step 1 - read JSON file

# COMMAND ----------

company_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.json(f"/mnt/gfaproject/landing/company_detail/company_detail_{v_file_date}.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - schema processing

# COMMAND ----------

company_schema = StructType(fields=[
    StructField("L1 type", StringType(), False),
    StructField("L2 type", StringType(), True),
    StructField("L3 type", StringType(), True),
    StructField("Open source available", StringType(), True),
    StructField("Organization", StringType(), True),
    StructField("Repository account", StringType(), True),
    StructField("Repository name", StringType(), True),
    StructField("Tags", StringType(), True)
])

# COMMAND ----------

company_df = spark.read \
.option("header", True) \
.schema(company_schema) \
.json(f"/mnt/gfaproject/landing/company_detail/company_detail_{v_file_date}.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename columns + change INT type + timestamp load + split Array

# COMMAND ----------

company_df = company_df.withColumnRenamed("Open source available", "Is_open_source_available")
company_df = company_df.withColumn("_pk", col("Organization"))
company_df = company_df.withColumn("Is_open_source_available", col("Is_open_source_available").cast(BooleanType()))
company_df = company_df.withColumn("valid_date", to_date(lit(v_file_date), "yyyyMMdd"))
company_df = company_df.withColumnRenamed("L1 type", "L1_type")
company_df = company_df.withColumnRenamed("L2 type", "L2_type")
company_df = company_df.withColumnRenamed("L3 type", "L3_type")
company_df = company_df.withColumnRenamed("Repository account", "Repository_account")
company_df = company_df.withColumnRenamed("Repository name", "Repository_name")
company_df = add_ingestion_date(company_df)
company_df = company_df.select("*", split(col("Tags") , ",").alias("Array_of_Tags")).drop("Tags")

# COMMAND ----------

display(company_df)
company_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - write CSV file to DELTA

# COMMAND ----------

#%sql

#CREATE TABLE IF NOT EXISTS bronze_db.b_company
#USING DELTA
#LOCATION "/mnt/gfaproject/bronze/b_company";

# COMMAND ----------

#%sql
#ALTER TABLE bronze_db.b_company SET TBLPROPERTIES (
#        'delta.columnMapping.mode' = 'name',
#        'delta.minReaderVersion' = '2',
#        'delta.minWriterVersion' = '5')

# COMMAND ----------

#%sql
#DROP TABLE bronze_db.b_company; 

# COMMAND ----------

company_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("bronze_db.b_company")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_db.b_company

# COMMAND ----------


