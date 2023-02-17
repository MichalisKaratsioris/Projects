# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

dbutils.notebook.run("1.load_stackoverflow_post_questions_csv", 0)

# COMMAND ----------

dbutils.notebook.run("2.load_stackoverflow_post_answers_csv", 0)

# COMMAND ----------

dbutils.notebook.run("3.load_github_json", 0, {"p_file_date": "20220731"})

# COMMAND ----------

dbutils.notebook.run("4.load_company_detail", 0, {"p_file_date": "20220731"})
