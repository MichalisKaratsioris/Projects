# Databricks notebook source
# MAGIC %md
# MAGIC Help menu for secret scope setting:

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC Print out the list of scopes:

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

# MAGIC %md
# MAGIC Checking what is inside the scope:

# COMMAND ----------

display(dbutils.secrets.list("project_phase_scope"))

# COMMAND ----------

# MAGIC %md
# MAGIC Checking the values of keys, inside the scope:

# COMMAND ----------

dbutils.secrets.get(scope = "project_phase_scope", key = "databricks-app-client-id")

# COMMAND ----------

# MAGIC %md
# MAGIC Above result will be '[REDACTED]'  
# MAGIC But we can still check the value of that specific key:

# COMMAND ----------

for x in dbutils.secrets.get(scope = "project_phase_scope", key = "databricks-app-client-id"):
    print(x, end=" ")

# COMMAND ----------

# MAGIC %md
# MAGIC By this logic we can change hard coded values by secrets:

# COMMAND ----------

storage_account_name = "dl0projectphase"

# client_id = "d3e6e549-70a5-4740-b5d4-d60a6cb9dafd"
# tenant_id = "e298c4a5-0566-4050-82bf-acbfb8dd454e"
# client_secret = "HQb8Q~PmNgjJlAbTMInENHm3juxV5o1xLFWNNbSK"


client_id = dbutils.secrets.get(scope = "project_phase_scope", key = "databricks-app-client-id")
tenant_id = dbutils.secrets.get(scope = "project_phase_scope", key = "databricks-app-tenant-id")
client_secret = dbutils.secrets.get(scope = "project_phase_scope", key = "databricks-app-client-secret")

# COMMAND ----------

# MAGIC %md
# MAGIC Doing config setup:

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC Creating function to mount containers:

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("landing")
mount_adls("bronze")
mount_adls("silver")
mount_adls("gold")

# dbutils.fs.unmount("/mnt/gold")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/"))

# COMMAND ----------

display(dbutils.fs.mounts())
