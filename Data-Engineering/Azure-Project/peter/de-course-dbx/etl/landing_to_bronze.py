# Databricks notebook source
# MAGIC %md
# MAGIC #### landing_to_bronze
# MAGIC ###### Take p_load_date from ADF and ingest it into each notebook.

# COMMAND ----------

dbutils.widgets.text("p_load_date", "")
v_load_date = dbutils.widgets.get("p_load_date")

# COMMAND ----------

return_value = dbutils.notebook.run("load_b_stackoverflow_questions", 0)

# COMMAND ----------

return_value = dbutils.notebook.run("load_b_stackoverflow_answers", 0)

# COMMAND ----------

return_value = dbutils.notebook.run("load_b_GitHub", 0, {"p_file_date": v_load_date})

# COMMAND ----------

return_value = dbutils.notebook.run("load_b_Company_Details", 0, {"p_file_date": v_load_date})
