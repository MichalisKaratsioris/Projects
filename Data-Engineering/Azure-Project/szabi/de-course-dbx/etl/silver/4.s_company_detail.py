# Databricks notebook source
from pyspark.sql.functions import split, col, lit, current_timestamp, to_utc_timestamp, to_date, col
from pyspark.sql.types import BooleanType, DateType, TimestampType
from datetime import datetime
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run "../../setup/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

# COMMAND ----------

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Reading bronze_db.b_company_detail:

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESC HISTORY bronze_db.b_company_detail

# COMMAND ----------

# df = spark.read.format("delta").option("versionAsOf", "1").load(f"{bronze_folder_path}/b_company_detail/")
df = spark.read.format("delta").load(f"{bronze_folder_path}/b_company_detail/")
#        .withColumn("is_current", lit("True").cast(BooleanType())) \
#        .withColumnRenamed("valid_date","valid_from_date") \
#        .withColumn("valid_to_date", lit("").cast(DateType())) \
#        .withColumn("dbx_created_at_datetime_utc", lit("").cast(TimestampType())) \
#        .withColumn("dbx_updated_at_datetime_utc", lit("").cast(TimestampType()))

# COMMAND ----------

display(df)

# COMMAND ----------

# df.write.format("delta").mode("overwrite").save(f"{silver_folder_path}/s_company_detail_source")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS silver_db.s_company_detail
# MAGIC 
# MAGIC -- SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC -- VACUUM silver_db.s_company_detail RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver_db.s_company_detail (
# MAGIC                                                         _pk STRING,
# MAGIC                                                         organization STRING,
# MAGIC                                                         repository_account STRING,
# MAGIC                                                         repository_name STRING,
# MAGIC                                                         is_open_source_available BOOLEAN,
# MAGIC                                                         load_date_datetime_utc TIMESTAMP,
# MAGIC                                                         tags_array
# MAGIC                                                             ARRAY <STRING>,
# MAGIC                                                         l1_type STRING,
# MAGIC                                                         l2_type STRING,
# MAGIC                                                         l3_type STRING,
# MAGIC                                                         is_current BOOLEAN,
# MAGIC                                                         valid_from_date DATE,
# MAGIC                                                         valid_to_date DATE,
# MAGIC                                                         dbx_created_at_datetime_utc TIMESTAMP,
# MAGIC                                                         dbx_updated_at_datetime_utc TIMESTAMP
# MAGIC                                                         )                                                        
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/silver/s_company_detail'
# MAGIC 
# MAGIC -- https://cloud.google.com/bigquery/docs/nested-repeated

# COMMAND ----------

# MAGIC %md
# MAGIC Creating hash value from multiple columns:  
# MAGIC https://learn.microsoft.com/en-us/sql/t-sql/functions/hashbytes-transact-sql?view=azuresqldb-current  
# MAGIC https://stackoverflow.com/questions/66747821/creating-hash-value-from-multiple-columns  
# MAGIC > %sql  
# MAGIC > SELECT * , Md5(organization||valid_date) AS _pk  
# MAGIC > FROM bronze_db.b_company_detail

# COMMAND ----------

merge_query = f"""MERGE INTO silver_db.s_company_detail target
USING(

-- -- select new records from source table to INSERT -- --
SELECT source.organization AS mergeKey, source.l1_type,
                                        source.l2_type,
                                        source.l3_type,
                                        source.is_open_source_available,
                                        source.organization,
                                        source.repository_account,
                                        source.repository_name,
                                        source.load_date_datetime_utc,
                                        source.tags_array,                                        
                                        to_date("{v_file_date}", "yyyyMMdd") as valid_from_date, 
                                        to_date("") as valid_to_date, 
                                        Md5(source.organization||source.valid_date) AS _pk
FROM bronze_db.b_company_detail source

UNION ALL

-- -- select old records from target table to DELETE -- --
SELECT target.organization AS mergeKey, source.l1_type,
                                        source.l2_type,
                                        source.l3_type,
                                        source.is_open_source_available,
                                        source.organization,
                                        source.repository_account,
                                        source.repository_name,
                                        source.load_date_datetime_utc,
                                        source.tags_array,                                        
                                        to_date("{v_file_date}", "yyyyMMdd") as valid_from_date, 
                                        to_date("") as valid_to_date, 
                                        Md5(source.organization||source.valid_date) AS _pk
FROM bronze_db.b_company_detail source
FULL JOIN silver_db.s_company_detail target
ON source.organization = target.organization
WHERE target.is_current = True AND source.organization is null

UNION ALL

-- -- select new records to UPDATE -- --
SELECT NULL as mergeKey, source.l1_type,
                         source.l2_type,
                         source.l3_type,
                         source.is_open_source_available,
                         source.organization,
                         source.repository_account,
                         source.repository_name,
                         source.load_date_datetime_utc,
                         source.tags_array,                                        
                         to_date("{v_file_date}", "yyyyMMdd") as valid_from_date, 
                         to_date("") as valid_to_date, 
                         Md5(source.organization||source.valid_date) AS _pk
FROM bronze_db.b_company_detail source
JOIN silver_db.s_company_detail target
ON source.organization = target.organization
WHERE target.is_current = True AND (
    source.repository_account <> target.repository_account or
    source.repository_name <> target.repository_name or
    source.is_open_source_available <> target.is_open_source_available or
    source.tags_array <> target.tags_array or
    source.l1_type <> target.l1_type or
    source.l2_type <> target.l2_type or
    source.l3_type <> target.l3_type    
    )
    
) staged_updates
ON target.organization = mergeKey AND target.is_current = True
WHEN MATCHED AND (
    target.repository_account <> staged_updates.repository_account or
    target.repository_name <> staged_updates.repository_name or
    target.is_open_source_available <> staged_updates.is_open_source_available or
    target.tags_array <> staged_updates.tags_array or
    target.l1_type <> staged_updates.l1_type or
    target.l2_type <> staged_updates.l2_type or
    target.l3_type <> staged_updates.l3_type or
    staged_updates.organization is null
    )
THEN
    UPDATE SET is_current = False,
               valid_to_date = to_date("{v_file_date}", "yyyyMMdd")-1,
               dbx_updated_at_datetime_utc = CURRENT_TIMESTAMP()
    
WHEN NOT MATCHED
THEN
    INSERT(
        _pk,
        organization,
        repository_account,
        repository_name,
        is_open_source_available,
        load_date_datetime_utc,
        tags_array,
        l1_type,
        l2_type,
        l3_type,
        is_current,
        valid_from_date,
        dbx_created_at_datetime_utc
        )
    VALUES(
        staged_updates._pk,
        staged_updates.organization,
        staged_updates.repository_account,
        staged_updates.repository_name,
        staged_updates.is_open_source_available,
        staged_updates.load_date_datetime_utc,
        staged_updates.tags_array,
        staged_updates.l1_type,
        staged_updates.l2_type,
        staged_updates.l3_type,
        True,
        staged_updates.valid_from_date,
        CURRENT_TIMESTAMP()
        )
"""

# COMMAND ----------

spark.sql(merge_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_db.s_company_detail
# MAGIC WHERE organization = "Airbyte"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT organization, l1_type, tags_array, is_current, valid_from_date, valid_to_date
# MAGIC FROM silver_db.s_company_detail
# MAGIC WHERE organization = "Dremio" OR
# MAGIC       organization = "Airbyte"
# MAGIC ORDER BY organization ASC

# COMMAND ----------

dbutils.notebook.exit(f"This notebook successfully run and finished by {v_file_date}!")
