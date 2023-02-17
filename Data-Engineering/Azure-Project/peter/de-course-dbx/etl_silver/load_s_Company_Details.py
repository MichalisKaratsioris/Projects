# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, DateType, TimestampType, ArrayType, LongType
from pyspark.sql.functions import current_timestamp, col, to_timestamp, lit, split, to_date, to_utc_timestamp
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
    
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../setup/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - load Bronze table

# COMMAND ----------

silver_company_df = spark.read.table("bronze_db.b_company")

# COMMAND ----------

display(silver_company_df)
silver_company_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC - ↑ _pk: a hash value created from the organization_name and valid_from_date column values (or try to use an identity column available in Databricks Runtime 10.4+)
# MAGIC - ↑ is_current: boolean, True if the row is currently valid
# MAGIC - valid_from_date: business validity start date (p_load_date/p_file_date pa-rameter, for example: ‘2022-07-31’)
# MAGIC - valid_to_date: business validity end date, blank when a row is current (one day prior of the load date when the change/delete operation happened, for example: ‘2022-08-30’)
# MAGIC - dbx_created_at_datetiime_utc: current timestamp of databricks notebook run when the row is created
# MAGIC - dbx_updated_at_datetime_utc: current timestamp of databricks notebook run when the row is updated

# COMMAND ----------

from pyspark.sql.functions import *
silver_company_df = silver_company_df.withColumn("new_pk",concat(col("_pk"),lit('-'),col("valid_date"))).drop("_pk").withColumnRenamed("new_pk", "_pk")
silver_company_df = silver_company_df.withColumn("is_current", lit(True))
silver_company_df = silver_company_df.withColumn("valid_from_date", to_timestamp(col("valid_date")))
silver_company_df = silver_company_df.drop("valid_date")
silver_company_df = silver_company_df.withColumn("valid_to_date", to_timestamp(lit("")))
silver_company_df = silver_company_df.withColumn("dbx_created_at_datetime_utc", lit(current_timestamp()))
silver_company_df = silver_company_df.withColumn("dbx_updated_at_datetime_utc", lit(current_timestamp()))


display(silver_company_df)
silver_company_df.printSchema()

# COMMAND ----------

if spark._jsparkSession.catalog().tableExists('silver_db', 's_company'):
    print("Table called: silver_db.s_company exist! Let's make a s_company_new_source to merge the tables together!")
    silver_company_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("silver_db.s_company_new_source")
    
else:
    print("Not exist, creating table called: silver_db.s_company.")
    silver_company_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("silver_db.s_company")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN silver_db;

# COMMAND ----------

#%sql
#DROP TABLE silver_db.s_company_new_source

# COMMAND ----------

merge_query = f"""MERGE INTO silver_db.s_company base_table
USING(

    -- select new records for INSERT:
    SELECT update_table.Organization AS mergeKey, update_table.*
    FROM silver_db.s_company_new_source update_table

    UNION ALL

    -- select old records to DELETE
    SELECT base_table.organization AS mergeKey, update_table.*
    FROM silver_db.s_company_new_source update_table
    FULL JOIN silver_db.s_company base_table
    ON update_table.organization = base_table.organization
    WHERE base_table.is_current = True AND update_table.organization is null

    UNION ALL

    -- select new records to UPDATE
    SELECT NULL as mergeKey, update_table.*
    FROM silver_db.s_company_new_source update_table
    JOIN silver_db.s_company base_table
    ON update_table.organization = base_table.organization
    WHERE base_table.is_current = True AND (
        update_table.L1_type <> base_table.L1_type or
        update_table.L2_type <> base_table.L2_type or
        update_table.L3_type <> base_table.L3_type or
        update_table.Is_open_source_available <> base_table.Is_open_source_available or
        update_table.Repository_account <> base_table.Repository_account or
        update_table.Repository_name <> base_table.Repository_name or
        update_table.loaded_at_datetime_utc <> base_table.loaded_at_datetime_utc or
        update_table.Array_of_Tags <> base_table.Array_of_Tags
        
        )
    
) staged_updates
ON base_table.organization = mergeKey
WHEN MATCHED AND base_table.is_current = true AND (
    --update_table.L1_type <> staged_updates.L1_type or
    --update_table.L2_type <> staged_updates.L2_type or
    --update_table.L3_type <> staged_updates.L3_type or
    --update_table.Is_open_source_available <> staged_updates.Is_open_source_available or
    --update_table.Repository_account <> staged_updates.Repository_account or
    --update_table.Repository_name <> staged_updates.Repository_name or
    --update_table.loaded_at_datetime_utc <> staged_updates.loaded_at_datetime_utc or
    --update_table.Array_of_Tags <> staged_updates.Array_of_Tags or
    staged_updates.Repository_name is null)
THEN
    UPDATE SET is_current = False, valid_to_date = staged_updates.valid_from_date, dbx_updated_at_datetime_utc = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT(L1_type, L2_type, L3_type, Is_open_source_available, Organization, Repository_account, Repository_name, loaded_at_datetime_utc, Array_of_Tags, _pk, is_current, valid_from_date, valid_to_date, dbx_created_at_datetime_utc, dbx_updated_at_datetime_utc)
    VALUES(staged_updates.L1_type, staged_updates.L2_type, staged_updates.L3_type, staged_updates.Is_open_source_available, staged_updates.Organization, staged_updates.Repository_account, staged_updates.Repository_name, staged_updates.loaded_at_datetime_utc, staged_updates.Array_of_Tags, staged_updates._pk, staged_updates.is_current, staged_updates.valid_from_date, staged_updates.valid_to_date, staged_updates.dbx_created_at_datetime_utc, staged_updates.dbx_updated_at_datetime_utc)"""

spark.sql(merge_query)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Testing

# COMMAND ----------

#display(company_df)
#company_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_db.s_company

# COMMAND ----------


