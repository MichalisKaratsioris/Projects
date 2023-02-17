# Databricks notebook source
storage_account_name = "gfaproject"
client_id = "702979ad-850b-4e25-aa33-1faf916a4c38"
tenant_id = "172a6725-1f8a-457c-b743-707bb83f086c"
client_secret = "bKY8Q~ExBqPh~78P~IYnmw-yDA84OWhxu4doHajX"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls_container(container_name):
    dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

mount_adls_container("bronze")
mount_adls_container("silver")
mount_adls_container("gold")
mount_adls_container("sandbox")
mount_adls_container("landing")
mount_adls_container("system")

# COMMAND ----------

dbutils.fs.ls("/mnt/gfaproject/bronze")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


