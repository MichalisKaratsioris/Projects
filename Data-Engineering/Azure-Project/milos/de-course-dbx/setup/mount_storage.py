# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("de-course-scope")

# COMMAND ----------

dbutils.secrets.get(scope="de-course-scope", key = "databricks-app-client-id")

# COMMAND ----------

storage_account_name = "azuregfaproject"
client_id = dbutils.secrets.get(scope="de-course-scope", key = "databricks-app-client-id")
tenant_id = dbutils.secrets.get(scope="de-course-scope", key = "databricks-app-tenant-id")
client_secret = dbutils.secrets.get(scope="de-course-scope", key = "databricks-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{container_name}",
        extra_configs = configs
    )

# COMMAND ----------

mount_adls("landing")
mount_adls("bronze")
mount_adls("silver")
mount_adls("gold")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/"))

# COMMAND ----------

display(dbutils.fs.mounts())
