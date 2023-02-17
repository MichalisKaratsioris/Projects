# Databricks notebook source
from pyspark.sql.types import StructType, StructField
import datetime
from pyspark.sql.functions import lit
from pyspark.sql import *	
from pyspark.sql.functions import *	
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import IntegerType,BooleanType,DateType

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

# COMMAND ----------

file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

peopleDF = spark.read.json(f"dbfs:/mnt/landing/company_detail/company_detail_{file_date}.json")

# COMMAND ----------

#display(peopleDF)

# COMMAND ----------

#peopleDF.printSchema()

# COMMAND ----------

schema = StructType(fields=[StructField("L1 type", StringType(), True),
                                   StructField("L2 type", StringType(), True),
                                   StructField("L3 type", StringType(), True),
                                   StructField("Open source available", StringType(), True),
                                   StructField("Organization", StringType(), True),
                                   StructField("Repository account", StringType(), True),
                                   StructField("Repository name", StringType(), True),
                                   StructField("Tags", StringType(), True)
                                   ])

# COMMAND ----------

rest_df = spark.read \
.schema(schema) \
.json(f"dbfs:/mnt/landing/company_detail/company_detail_{file_date}.json")

# COMMAND ----------

df_company_detail_schema = rest_df \
.withColumn("Open source available", col("Open source available").cast(BooleanType())) \
.withColumn("load_date_datetime_utc", to_utc_timestamp(current_timestamp(),'UTC')) \
.withColumn("valid_date", to_date(lit(file_date), "yyyyMMdd")) \
.withColumn("_pk", col("Organization")) \
    .withColumnRenamed("Open source available","is_open_source_available") \
    .withColumnRenamed("L1 type","l1_type") \
    .withColumnRenamed("L2 type","l2_type") \
    .withColumnRenamed("L3 type","l3_type") \
    .withColumnRenamed("Organization","organization") \
    .withColumnRenamed("Repository account","repository_account") \
    .withColumnRenamed("Repository name","repository_name") \
    .withColumnRenamed("Tags","tags")

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

df_company_detail_schema.write.format("delta").mode("overwrite").save(f"dbfs:/mnt/bronze/b_companydetails")
