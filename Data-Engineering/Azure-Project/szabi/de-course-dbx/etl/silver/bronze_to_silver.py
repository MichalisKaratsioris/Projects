# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

dbutils.notebook.run("1.s_stackoverflow_post_questions", 0)

# COMMAND ----------

dbutils.notebook.run("2.s_stackoverflow_post_answers", 0)

# COMMAND ----------

dbutils.notebook.run("3.s_github", 0, {"p_file_date": "20220731"})

# COMMAND ----------

dbutils.notebook.run("4.s_company_detail", 0, {"p_file_date": "20220731"})
