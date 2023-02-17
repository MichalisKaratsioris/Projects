# Databricks notebook source
from pyspark.sql.functions import col, split, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create a widget for the file date parameter

# COMMAND ----------

# dbutils.widgets.text("p_file_date", "")

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read the data from the bronze layer using the spark dataframe reader

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# display(dbutils.fs.ls("/mnt/decoursesa/bronze/b_github"))

# COMMAND ----------

df = spark.read \
.format("delta") \
.load("/mnt/decoursesa/bronze/b_github")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Split column

# COMMAND ----------

splitted_df = df \
.withColumn("repository_account", split(col("repo_name"), "/").getItem(0)) \
.withColumn("repository_name", split(col("repo_name"), "/").getItem(1))

# COMMAND ----------

display(splitted_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select required columns

# COMMAND ----------

selected_df = splitted_df.select(col("_pk"), col("repository_account"), col("repository_name"), col("actor_id").alias("user_id"), col("id").alias("event_id"), col("type"), col("created_at_datetime_utc"), col("valid_date"))

# COMMAND ----------

display(selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add new column

# COMMAND ----------

added_df = selected_df.withColumn("dbx_created_at_datetime_utc", current_timestamp())

# COMMAND ----------

display(added_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write data to delta table using dynamic partition overwrites

# COMMAND ----------

# %sql
# DROP TABLE silver_db.s_github;

# COMMAND ----------

added_df.write \
.mode("overwrite") \
.partitionBy("valid_date") \
.format("delta") \
.option("partitionOverwriteMode", "dynamic") \
.saveAsTable("silver_db.s_github")

# COMMAND ----------

# %sql
# DESCRIBE EXTENDED silver_db.s_github;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_db.s_github
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT valid_date, COUNT(*)
# MAGIC FROM silver_db.s_github
# MAGIC GROUP BY valid_date
# MAGIC ORDER BY valid_date ASC;

# COMMAND ----------

dbutils.notebook.exit(f"Success for file date parameter {v_file_date}")
