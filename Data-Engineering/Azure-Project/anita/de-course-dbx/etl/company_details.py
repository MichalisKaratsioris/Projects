# Databricks notebook source
dbutils.widgets.text("p_file_date",defaultValue="20220731", label="1 file_date")
v_file_date= dbutils.widgets.get("p_file_date")


df = spark.read.json("/mnt/landing/{}/company_detail_{}.json".format(('company_detail/' + v_file_date[0:4]+'/'+ 
v_file_date[4:6]+'/'+ v_file_date[6:8]), v_file_date))
display(df)



#company_detail/2022/07/31

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

#Company detail tips:
#▪ Create the _pk field based on the “organization_name” field
#▪ Create a “tags_array” field from the source “tags” field

# COMMAND ----------

from pyspark.sql.types import StructType
import datetime
from pyspark.sql.functions import lit
from pyspark.sql import *	
from pyspark.sql.functions import *	
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import IntegerType,BooleanType,DateType


date = datetime.datetime.strptime(v_file_date, '%Y%m%d')
print(date.date())

df = df.withColumn("valid_date", lit(date))

df = df.select("*", split(col("Tags") , ",").alias("Tags_array")).drop("Tags")

display(df)
    
df = df.withColumn("Open_source_available",col("Open source available").cast(BooleanType()))
df = df.drop(col("Open source available"))

for col in df.dtypes:
    if col[1] == "boolean":
        df = df.withColumnRenamed(col[0], "is_" + col[0])
    if col[1] == "timestamp":
        df = df.withColumnRenamed(col[0], col[0] + "_datetime_utc")
        
df =(df.withColumn('order', row_number().over(Window.partitionBy(lit('1')).orderBy(lit('1'))))#Create increasing id
          .withColumn('_pk', dense_rank().over(Window.partitionBy().orderBy('Organization'))).orderBy('order')#Use window function; dense_ranl to generate new id
          .drop('order'))

df = df.select([df.columns[-1]] + df.columns[:-1])

for i in df.columns:
    k = i.replace(" ", "_")
    if k != i:
        print(k)
        df = df.withColumnRenamed(i, k)

display(df)
print(df.dtypes)

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("bronze_db.b_company_detail")
