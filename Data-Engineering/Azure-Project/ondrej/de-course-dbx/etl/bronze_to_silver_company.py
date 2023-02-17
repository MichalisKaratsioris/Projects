# Databricks notebook source
# MAGIC %md
# MAGIC ###Create a widget for the file date parameter

# COMMAND ----------

# dbutils.widgets.text("p_file_date", "")

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

v_valid_date = f"{v_file_date[:4]}-{v_file_date[4:6]}-{v_file_date[6:]}"
v_valid_date

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Silver Table

# COMMAND ----------

# %sql
# SELECT *
# FROM bronze_db.b_company
# LIMIT 10;

# COMMAND ----------

# %sql
# DROP TABLE silver_db.s_company;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_db.s_company (
# MAGIC organization_name STRING,
# MAGIC repository_account STRING,
# MAGIC repository_name STRING,
# MAGIC l1_type STRING,
# MAGIC l2_type STRING,
# MAGIC l3_type STRING,
# MAGIC tags_array ARRAY<STRING>,
# MAGIC is_open_source_available BOOLEAN,
# MAGIC _pk INT,
# MAGIC is_current BOOLEAN,
# MAGIC valid_from_date DATE,
# MAGIC valid_to_date DATE,
# MAGIC dbx_created_at_datetime_utc TIMESTAMP,
# MAGIC dbx_updated_at_datetime_utc TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# %sql
# DESCRIBE EXTENDED silver_db.s_company;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_db.s_company
# MAGIC ORDER BY organization_name ASC, valid_from_date ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Merge into Silver Table

# COMMAND ----------

one_more_select_query = ("""
SELECT null AS mergeKey, new_table.organization_name, new_table.repository_account, new_table.repository_name, new_table.l1_type, new_table.l2_type, new_table.l3_type, new_table.tags_array, new_table.is_open_source_available, hash(new_table.organization_name, new_table.valid_date) AS _pk, new_table.valid_date AS valid_from_date
FROM bronze_db.b_company AS new_table
WHERE new_table.organization_name IN (

SELECT DISTINCT new_table.organization_name
FROM bronze_db.b_company AS new_table
JOIN silver_db.s_company AS base_table
ON new_table.organization_name = base_table.organization_name
WHERE base_table.is_current = false

) AND new_table.organization_name NOT IN (

SELECT new_table.organization_name
FROM bronze_db.b_company AS new_table
JOIN silver_db.s_company AS base_table
ON new_table.organization_name = base_table.organization_name
WHERE base_table.is_current = true

)
""")

# COMMAND ----------

spark.sql(f"""
MERGE INTO silver_db.s_company AS base_table
USING (

SELECT new_table.organization_name AS mergeKey, new_table.organization_name, new_table.repository_account, new_table.repository_name, new_table.l1_type, new_table.l2_type, new_table.l3_type, new_table.tags_array, new_table.is_open_source_available, hash(new_table.organization_name, new_table.valid_date) AS _pk, new_table.valid_date AS valid_from_date
FROM bronze_db.b_company AS new_table

UNION ALL

-- Extra data 1 for INSERT
-- (some data of this mergeKey already in the base_table, is_current = true, different data in new_table)

SELECT null AS mergeKey, new_table.organization_name, new_table.repository_account, new_table.repository_name, new_table.l1_type, new_table.l2_type, new_table.l3_type, new_table.tags_array, new_table.is_open_source_available, hash(new_table.organization_name, new_table.valid_date) AS _pk, new_table.valid_date AS valid_from_date
FROM bronze_db.b_company AS new_table
JOIN silver_db.s_company AS base_table
ON new_table.organization_name = base_table.organization_name
WHERE base_table.is_current = true AND (new_table.repository_account <> base_table.repository_account OR new_table.repository_name <> base_table.repository_name OR new_table.l1_type <> base_table.l1_type OR new_table.l2_type <> base_table.l2_type OR new_table.l3_type <> base_table.l3_type OR new_table.tags_array <> base_table.tags_array OR new_table.is_open_source_available <> base_table.is_open_source_available)

UNION ALL

-- Extra data 2 for UPDATE
-- (some data of this mergeKey already in the base_table, is_current = true, no data in new_table)

SELECT base_table.organization_name AS mergeKey, new_table.organization_name, new_table.repository_account, new_table.repository_name, new_table.l1_type, new_table.l2_type, new_table.l3_type, new_table.tags_array, new_table.is_open_source_available, hash(new_table.organization_name, new_table.valid_date) AS _pk, new_table.valid_date AS valid_from_date
FROM bronze_db.b_company AS new_table
FULL JOIN silver_db.s_company AS base_table
ON new_table.organization_name = base_table.organization_name
WHERE base_table.is_current = true AND new_table.organization_name IS null

UNION ALL

-- Extra data 3 for INSERT
-- (some data of this mergeKey already in the base_table, is_current = false, some data in new_table)

{one_more_select_query}

) AS staged_updates
ON base_table.organization_name = mergeKey
WHEN MATCHED AND base_table.is_current = true AND (staged_updates.repository_account <> base_table.repository_account OR staged_updates.repository_name <> base_table.repository_name OR staged_updates.l1_type <> base_table.l1_type OR staged_updates.l2_type <> base_table.l2_type OR staged_updates.l3_type <> base_table.l3_type OR staged_updates.tags_array <> base_table.tags_array OR staged_updates.is_open_source_available <> base_table.is_open_source_available OR staged_updates.organization_name IS null) THEN UPDATE SET
base_table.is_current = false,
base_table.valid_to_date = date_sub('{v_valid_date}', 1),
base_table.dbx_updated_at_datetime_utc = current_timestamp
WHEN NOT MATCHED THEN INSERT (organization_name, repository_account, repository_name, l1_type, l2_type, l3_type, tags_array, is_open_source_available, _pk, is_current, valid_from_date, dbx_created_at_datetime_utc) VALUES (staged_updates.organization_name, staged_updates.repository_account, staged_updates.repository_name, staged_updates.l1_type, staged_updates.l2_type, staged_updates.l3_type, staged_updates.tags_array, staged_updates.is_open_source_available, staged_updates._pk, true, staged_updates.valid_from_date, current_timestamp);
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_db.s_company
# MAGIC ORDER BY organization_name ASC, valid_from_date ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_db.s_company
# MAGIC WHERE organization_name = 'Airbyte'
# MAGIC ORDER BY valid_from_date ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_db.s_company
# MAGIC WHERE organization_name = 'Dremio'
# MAGIC ORDER BY valid_from_date ASC;

# COMMAND ----------

dbutils.notebook.exit(f"Success for file date parameter {v_file_date}")
