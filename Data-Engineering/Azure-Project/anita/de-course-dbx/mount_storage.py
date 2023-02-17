# Databricks notebook source
def mount_storage(container_name):
    dbutils.fs.mount(
      source = "wasbs://{}@abogar2.blob.core.windows.net/".format(container_name),
      mount_point = "/mnt/{}/".format(container_name),
      extra_configs  = {"fs.azure.account.key.abogar2.blob.core.windows.net":"D8X+Hbl9L1cAI6OLKeI6oBjiUJuTJJ5Sv18I6VBMITGmUUnN+6WiIqjGl6W2aDwH9ClNDVDJrdXH+ASt7ydhVQ=="})


# COMMAND ----------

mount_storage("landing")
mount_storage("bronze")


# COMMAND ----------

mount_storage("silver")

# COMMAND ----------

mount_storage("gold")

# COMMAND ----------

display(dbutils.fs.mounts())
