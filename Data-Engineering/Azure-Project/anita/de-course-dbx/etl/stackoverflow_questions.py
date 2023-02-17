# Databricks notebook source

dbutils.widgets.text("basePath",defaultValue="/mnt/bronze", label="1 basePath")
dbutils.widgets.text("database",defaultValue="bronze_db", label="2 database")
dbutils.widgets.text("table",defaultValue="/mnt/landing/stackoverflow", label="3 table")

basePath = dbutils.widgets.get("basePath")
database = dbutils.widgets.get("database")
database = dbutils.widgets.get("table")



# COMMAND ----------

#Create a primary key in every table, called _pk.
#▪ Make sure that there is a timestamp column which indicates the creation 
#or load datetime of each record.
#▪ Make sure that every datetime is in UTC format.
#▪ Add _datetime_utc postfix to every datetime column name.
#▪ Add is_ prefix to every boolean type (‘Yes’/’No’, 1/0) column name and 
#convert it to boolean (true/false).
#▪ Add a valid_date column to every table which contains the load date 
#information.
#▪ prefix the table names with the first letter of the table’s layer (for example “b_github” for bronze github table)

#Stackoverflow specific tips:
#▪ it is not necessary to use file date parameter for these source data 
#since there is only one source file for all of the dates but you can use 
#the file date parameter and then filter the source data based on it 
#(where creation_date <= valid_date)
#▪ Create the _pk field based on the “id” field


# COMMAND ----------

from pyspark.sql.types import IntegerType
import datetime
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession	
from pyspark.sql.functions import *	


df = spark.read.csv("/mnt/landing/stackoverflow/stackoverflow_post_questions.csv", header='true')



for cols in df.columns:
    if cols.endswith("_date"):
        df = df.withColumn(cols, to_timestamp(cols))
    if cols.endswith("_id") or cols.endswith("_count"):
        df = df.withColumn(cols, df[cols].cast(IntegerType()))
        
for col in df.dtypes:
    print(col[1])
    if col[1] == "boolean":
        df = df.withColumnRenamed(col[0], "is_" + col[0])
    if col[1] == "timestamp":
        df = df.withColumnRenamed(col[0], col[0] + "_datetime_utc")

df = df.withColumn("id", df["id"].cast(IntegerType()))
df = df.withColumn("_pk", df["id"])

df = df.select([df.columns[-1]] + df.columns[:-1])

#create valid_date column from load_date parameter
dbutils.widgets.text("p_file_date",defaultValue="20220731", label="1 file_date")
v_file_date= dbutils.widgets.get("p_file_date")

date = datetime.datetime.strptime(v_file_date, '%Y%m%d')
print(date.date())

df = df.withColumn("valid_date", lit(date))

display(df)

# COMMAND ----------

df.write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("bronze_db.b_stackoverflow_questions")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM bronze_db.b_stackoverflow_questions LIMIT 20
