# Databricks notebook source
##Create views from tables

# COMMAND ----------

## Azure Data Lake Gen 2 As Staging Storage - probably done in mount already
spark.conf.set(fs.azure.account.key.<<your-storage-account-name>>. dfs.core.windows.net, ”<<your-storage-account-access-key>>”)


# Azure Synapse Connection Configuration
dwDatabase = <<your-database-name>>
dwServer = <<your-sql-sever-name>>
dwUser = <<your-database-account>>
dwPass = <<your-database-account-password>>
dwJdbcPort =  "1433"
dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
sqlDwUrl = f"jdbc:sqlserver://{dwServer}:{dwJdbcPort};database={dwDatabase};user={dwUser};password={dwPass};${dwJdbcExtraOptions}"
  
# Staging Storage Configuration

# Azure Data Lake Gen 2
tempDir = "abfss://<<container >>@<<your-storage-account-name>>.dfs.core.windows.net/<<folder-for-temporary-data>>"

# Azure Synapse Table
tableName = <<your-azure-synapse-table-to-read-or-write>>

##Write Data to Azure Synapse
df.write \
  .mode('overwrite') \ # Append Data
  .format("com.databricks.spark.sqldw") \
  .option("url", sqlDwUrl) \
  .option("tempDir", tempDir) \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", tableName) \
  .save()
