# Databricks notebook source
from pyspark.sql.types import StructType, IntegerType, LongType
import datetime
from pyspark.sql.functions import lit, col, split
from pyspark.sql import SparkSession	
from pyspark.sql.functions import *	

#df = spark.read.format("delta").load("/mnt/bronze/b_github")

dbutils.widgets.text("p_file_date",defaultValue="20220731", label="1 file_date")
v_file_date= dbutils.widgets.get("p_file_date")

date = datetime.datetime.strptime(v_file_date, '%Y%m%d')
print(date.date())

b_df = spark.read.table("bronze_db.b_github")

display(b_df)

# COMMAND ----------

b_df = b_df.withColumn('repository_account', split(b_df['repo_name'], '/', 2).getItem(0)) \
           .withColumn('repository_name', split(b_df['repo_name'], '/', 2).getItem(1))

#user_id = It is the actor_id in the source - DONE
b_df = b_df.withColumnRenamed("actor_id", "user_id")

#event_id = ID column in the source - DONE
b_df = b_df.withColumnRenamed("id", "event_id")

#dbx_created_at_datetiime_utc: current timestamp of databricks notebook run when the row is created - ?
now = datetime.datetime.utcnow()
b_df = b_df.withColumn('dbx_created_at_datetime_utc', lit(now))

    #get rid of unneccessary columns - DONE

s_df = b_df.select("_pk","repository_account","repository_name","user_id","event_id","type", "created_at_datetime_utc", "valid_date_datetime_utc", "dbx_created_at_datetime_utc")

# COMMAND ----------

#Make your code idempotent, pay attention not to duplicate data in case of a re-run:
#if you use append mode, any previously loaded data for the same loading 
#date must be deleted from the silver table
#▪ or you can use a selective overwrite logic instead of append mode 
#▪ in both cases partitions must be used


###
s_df.write \
.mode("overwrite") \
.option("overwriteSchema", True) \
.partitionBy("valid_date_datetime_utc") \
.option("partitionOverwriteMode", "dynamic") \
.format("delta") \
.saveAsTable("silver_db.s_github") 


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_db.s_github limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT valid_date_datetime_utc, COUNT(*)
# MAGIC FROM silver_db.s_github
# MAGIC GROUP BY valid_date_datetime_utc;
