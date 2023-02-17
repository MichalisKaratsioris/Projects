# Databricks notebook source
# MAGIC %md
# MAGIC Creating Delta table by pyspark:

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

DeltaTable.create(spark) \
        .tableName("bronze_db.szabi_demo") \
        .addColumn("id", "INT") \
        .addColumn("name", "STRING") \
        .addColumn("salary", "INT") \
        .addColumn("registration_date", "TIMESTAMP") \
        .property("description", "table created for demo purpose") \
        .location("mnt/bronze/szabi_demo") \
        .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Delta table by sql:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze_db.szabi_demo (
# MAGIC                                                         id INT,
# MAGIC                                                         name STRING,
# MAGIC                                                         salary INT,
# MAGIC                                                         registration_date TIMESTAMP
# MAGIC                                                         )
# MAGIC USING DELTA
# MAGIC LOCATION 'mnt/bronze/szabi_demo'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists bronze_db.szabi_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into bronze_db.szabi_demo values(1, "Szabi", 500, CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_db.szabi_demo

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bronze/"))

# COMMAND ----------

delta_instance = DeltaTable.forName(spark, "bronze_db.szabi_demo")

# COMMAND ----------

delta_instance_2 = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/mnt/bronze/szabi_demo")

# COMMAND ----------

display(delta_instance.toDF())

# COMMAND ----------

delta_instance.delete("id = 1")

# COMMAND ----------

display(delta_instance.history())

# COMMAND ----------


