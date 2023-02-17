# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
from pyspark.sql.functions import current_timestamp, to_utc_timestamp
from pyspark.sql.functions import col, lit, when, flatten, split
from delta.tables import *

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

# COMMAND ----------

file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

Sampledata = spark.read.format("delta").load(f"dbfs:/mnt/bronze/b_companydetails")

# COMMAND ----------

display(Sampledata)

# COMMAND ----------

# %sql
CREATE TABLE IF NOT EXISTS s_companydetails (
                        l1_type STRING,
                        l2_type STRING,
                        l3_type STRING,
                        is_open_source_available BOOLEAN,
                        organization STRING,
                        repository_account STRING,
                        repository_name STRING,
                        load_date_time_utc TIMESTAMP,
                        valid_date DATE,
                        tags_array ARRAY<
                                      STRUCT<
                                         element STRING>>,
                        _pk STRING, 
                        is_current BOOLEAN,
                        valid_from_date DATE,
                        valid_to_date DATE,
                        dbx_created_at_datetime_utc TIMESTAMP,
                        dbx_updated_at_datetime_utc TIMESTAMP
)
USING DELTA
LOCATION '/mnt/silver/s_company_detail'


# COMMAND ----------

#https://stackoverflow.com/questions/73425127/how-to-add-array-column-to-spark-table-using-alter-table
