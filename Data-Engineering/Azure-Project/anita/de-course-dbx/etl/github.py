# Databricks notebook source
dbutils.widgets.text("p_file_date",defaultValue="20220731", label="1 file_date")
v_file_date= dbutils.widgets.get("p_file_date")


df = spark.read.json("/mnt/landing/{}/githubarchive_day_{}.json".format(('github/' + v_file_date[0:4]+'/'+ 
v_file_date[4:6]+'/'+ v_file_date[6:8]), v_file_date))
display(df)

# COMMAND ----------

#Create a primary key in every table, called _pk.
#▪ Make sure that there is a timestamp column which indicates the creation 
#or load datetime of each record.
#▪ Make sure that every datetime is in UTC format.
#▪ Add _datetime_utc postfix to every datetime column name.
#▪ Add is_ prefix to every boolean type (‘Yes’/’No’, 1/0) column name and convert it to boolean (true/false).
#▪ Add a valid_date column to every table which contains the load date information.
#▪ prefix the table names with the first letter of the table’s layer (for example “b_github” for bronze github table)

#GitHub specific tips:
#▪ Create the _pk field based on the “id” field
#▪ Make sure that you read the nested fields correctly, and flatten its values 
#to separate columns in the bronze table (for example from the nested 
#repo field you should create 3 columns: repo_id, repo_name and 
#repo_url)


# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, LongType
import datetime
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession	
from pyspark.sql.functions import *	


date = datetime.datetime.strptime(v_file_date, '%Y%m%d')
print(date.date())

df = df.withColumn("valid_date", lit(date))

display(df)
df.printSchema()
print(df.dtypes)

df = df.withColumn("created_at", to_timestamp("created_at"))

df = df.withColumn("id", df["id"].cast(LongType()))
df = df.withColumn("_pk", df["id"])

df = df.withColumnRenamed("id", "id_pk")

df = df.select([df.columns[-1]] + df.columns[:-1])

for col in df.dtypes:
    print(col[1])
    if col[1] == "boolean":
        print(col[0])
        df = df.withColumnRenamed(col[0], "is_" + col[0])
    if col[1] == "timestamp":
        print(col[0])
        df = df.withColumnRenamed(col[0], col[0] + "_datetime_utc")
        
display(df)

# COMMAND ----------

from pyspark.sql import functions

actor_col = []
for element in df.select("actor.*").columns:
    actor_col.append(element)

df =df.select("actor.*", '*')

for name in actor_col:
    df =df.withColumnRenamed(name, "actor_" + name)

actor_col.clear

actor_col = []
for element in df.select("org.*").columns:
    actor_col.append(element)

df =df.select('*', "org.*")

for name in actor_col:
    df =df.withColumnRenamed(name, "org_" + name)

actor_col.clear

for element in df.select("repo.*").columns:
    actor_col.append(element)

df =df.select('*', "repo.*")

for name in actor_col:
    df =df.withColumnRenamed(name, "repo_" + name)


cols = ("actor", "org", "repo")

df = df.drop(*cols)

for names in df.columns:
    if names.endswith("id"): 
        df = df.withColumn(names, df[names].cast(IntegerType()))

df = df.withColumnRenamed("id_pk", "id")

display(df)


# COMMAND ----------

df.write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("bronze_db.b_github")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_db.b_github LIMIT 100
