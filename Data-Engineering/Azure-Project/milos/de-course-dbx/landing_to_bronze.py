# Databricks notebook source
dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# COMMAND ----------

timeoutS = 120

# COMMAND ----------

dbutils.notebook.run("./etl/GitHub_etl", timeoutS, {"p_file_date" : "20220731"})
dbutils.notebook.run("./etl/REST_etl", timeoutS, {"p_file_date" : "20220731"})
dbutils.notebook.run("./etl/StackOverflow_answers_etl", timeoutS, {"p_file_date" : "20220731"})
dbutils.notebook.run("./etl/StackOverflow_questions_etl", timeoutS, {"p_file_date" : "20220731"})
