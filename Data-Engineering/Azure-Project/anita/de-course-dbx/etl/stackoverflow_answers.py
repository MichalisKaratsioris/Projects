# Databricks notebook source
dbutils.widgets.text("basePath",defaultValue="/mnt/bronze", label="1 basePath")
dbutils.widgets.text("database",defaultValue="bronze_db", label="2 database")
dbutils.widgets.text("table",defaultValue="/mnt/landing/stackoverflow", label="3 table")

basePath = dbutils.widgets.get("basePath")
database = dbutils.widgets.get("database")
database = dbutils.widgets.get("table")

dbutils.widgets.text("p_file_date",defaultValue="20220731", label="1 file_date")

# COMMAND ----------

from pyspark.sql.types import IntegerType
import datetime
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession	
from pyspark.sql.functions import *	


df = spark.read.csv("/mnt/landing/stackoverflow/stackoverflow_post_answers.csv", header='true')

df.show()

for cols in df.columns:
    if cols.endswith("_date"):
        df = df.withColumn(cols, to_timestamp(cols))
        
for col in df.dtypes:
    print(col[1])
    if col[1] == "boolean":
        print(col[0])
        df = df.withColumnRenamed(col[0], "is_" + col[0])
    if col[1] == "timestamp":
        print(col[0])
        df = df.withColumnRenamed(col[0], col[0] + "_datetime_utc")
        
df = df.withColumn("id", df["id"].cast(IntegerType()))

df = df.withColumn("_pk", df["id"])

df = df.select([df.columns[-1]] + df.columns[:-1])


v_file_date= dbutils.widgets.get("p_file_date")

date = datetime.datetime.strptime(v_file_date, '%Y%m%d')
print(date.date())

df = df.withColumn("valid_date", lit(date))

display(df)

# COMMAND ----------

df.write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("bronze_db.b_stackoverflow_answers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_db.b_stackoverflow_answers LIMIT 20
