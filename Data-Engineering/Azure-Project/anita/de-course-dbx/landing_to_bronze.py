# Databricks notebook source
#dbutils.notebook.run(notebookpath, timeout_in_seconds, parameters)

timeout_in_seconds = 100

dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

dbutils.notebook.run("./etl/github", timeout_in_seconds, {"p_file_date" : "20220930"})
dbutils.notebook.run("./etl/company_details", timeout_in_seconds, {"p_file_date" : "20220930"})
dbutils.notebook.run("./etl/stackoverflow_answers", timeout_in_seconds, {"p_file_date" : "20220731"})
dbutils.notebook.run("./etl/stackoverflow_questions", timeout_in_seconds, {"p_file_date" : "20220930"})

#20220731
#20220831
#20220930
