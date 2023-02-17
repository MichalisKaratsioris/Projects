# Databricks notebook source
# MAGIC %python
# MAGIC 
# MAGIC # "/mnt/silver"
# MAGIC # "silver_db"
# MAGIC 
# MAGIC # "/mnt/gold"
# MAGIC # "gold_db"
# MAGIC 
# MAGIC dbutils.widgets.text("basePath",defaultValue="/mnt/bronze", label="1 basePath")
# MAGIC dbutils.widgets.text("database",defaultValue="bronze_db", label="1 database")
# MAGIC dbutils.widgets.text("database2",defaultValue="silver_db", label="2 database")
# MAGIC dbutils.widgets.text("basePath2",defaultValue="/mnt/silver", label="2 basePath")
# MAGIC dbutils.widgets.text("database3",defaultValue="gold_db", label="3 database")
# MAGIC dbutils.widgets.text("basePath3",defaultValue="/mnt/gold", label="3 basePath")

# COMMAND ----------

# MAGIC %python
# MAGIC basePath = dbutils.widgets.get("basePath")
# MAGIC database = dbutils.widgets.get("database")
# MAGIC sqlDatabasePath = "dbfs:" + basePath
# MAGIC createDbSql = "CREATE DATABASE if not exists " + database + " LOCATION '" + sqlDatabasePath + "';"
# MAGIC 
# MAGIC basePath2 = dbutils.widgets.get("basePath2")
# MAGIC database2 = dbutils.widgets.get("database2")
# MAGIC sqlDatabasePath2 = "dbfs:" + basePath2
# MAGIC createDbSql2 = "CREATE DATABASE if not exists " + database2 + " LOCATION '" + sqlDatabasePath2 + "';"
# MAGIC 
# MAGIC basePath3 = dbutils.widgets.get("basePath3")
# MAGIC database3 = dbutils.widgets.get("database3")
# MAGIC sqlDatabasePath3 = "dbfs:" + basePath3
# MAGIC createDbSql3 = "CREATE DATABASE if not exists " + database3 + " LOCATION '" + sqlDatabasePath3 + "';"
# MAGIC #try:
# MAGIC #  dbutils.fs.ls(basePath)
# MAGIC #except:
# MAGIC #  dbutils.fs.mkdirs(basePath)
# MAGIC #else:
# MAGIC #  raise Exception("The folder " + basePath + " already exists")
# MAGIC 
# MAGIC print("basePath " + basePath + " created")
# MAGIC 
# MAGIC try:
# MAGIC   spark.sql(createDbSql)
# MAGIC except:
# MAGIC   raise Exception("The database " + database + " already exists")
# MAGIC   
# MAGIC try:
# MAGIC   spark.sql(createDbSql2)
# MAGIC except:
# MAGIC   raise Exception("The database " + database2 + " already exists")
# MAGIC 
# MAGIC try:
# MAGIC   spark.sql(createDbSql3)
# MAGIC except:
# MAGIC   raise Exception("The database " + database3 + " already exists")
# MAGIC 
# MAGIC print("database " + database + " created")
# MAGIC print("database " + database2 + " created")
# MAGIC print("database " + database3 + " created")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#spark.sql("DROP DATABASE if exists " + database + " CASCADE")
#dbutils.fs.rm(basePath, recurse = True)
