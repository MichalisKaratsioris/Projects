# Databricks notebook source
timeout_in_seconds = 100


dbutils.notebook.run("./silver_etl/github", timeout_in_seconds, {"p_file_date" : "20220930"})
dbutils.notebook.run("./silver_etl/company_details", timeout_in_seconds, {"p_file_date" : "20220831"})
dbutils.notebook.run("./silver_etl/stackoverflow_answers", timeout_in_seconds, {"p_file_date" : "20220831"})
dbutils.notebook.run("./silver_etl/stackoverflow_questions", timeout_in_seconds, {"p_file_date" : "20220930"})

#20220731
#20220831
#20220930
