# Databricks notebook source
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
# MAGIC ###Run bronze_to_silver_stackoverflow_post_questions

# COMMAND ----------

v_result = dbutils.notebook.run("bronze_to_silver_stackoverflow_post_questions", 0, {"p_file_date": v_file_date})
v_result

# COMMAND ----------

# MAGIC %md
# MAGIC ###Run bronze_to_silver_stackoverflow_post_answers

# COMMAND ----------

v_result = dbutils.notebook.run("bronze_to_silver_stackoverflow_post_answers", 0, {"p_file_date": v_file_date})
v_result

# COMMAND ----------

# MAGIC %md
# MAGIC ###Run bronze_to_silver_github

# COMMAND ----------

v_result = dbutils.notebook.run("bronze_to_silver_github", 0, {"p_file_date": v_file_date})
v_result

# COMMAND ----------

# MAGIC %md
# MAGIC ###Run bronze_to_silver_company

# COMMAND ----------

v_result = dbutils.notebook.run("bronze_to_silver_company", 0, {"p_file_date": v_file_date})
v_result
