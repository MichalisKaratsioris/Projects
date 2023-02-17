# Databricks notebook source
from pyspark.sql.functions import split, col, lit, current_timestamp, to_utc_timestamp, to_date, col
from datetime import datetime
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run "../../setup/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

# COMMAND ----------

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Reading delta table into a df and filter as task said:

# COMMAND ----------

silver_df = spark.read.format("delta").load(f"{bronze_folder_path}/b_stackoverflow_post_answers/") \
    .where(to_date(col("last_activity_date_datetime_utc")) <= to_date(lit(f"{v_file_date}"), "yyyyMMdd"))

# COMMAND ----------

display(silver_df)

# COMMAND ----------

silver_df = silver_df \
    .withColumn("valid_date", to_date(lit(v_file_date), "yyyyMMdd"))

# COMMAND ----------

display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Creating blank delta table for SCD Type 1 merging:

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
        .tableName("silver_db.s_stackoverflow_post_answers") \
        .addColumn("id", "INT") \
        .addColumn("title", "STRING") \
        .addColumn("accepted_answer_id", "INT") \
        .addColumn("answer_count", "INT") \
        .addColumn("comment_count", "INT") \
        .addColumn("community_owned_date_datetime_utc", "TIMESTAMP") \
        .addColumn("creation_date_datetime_utc", "TIMESTAMP") \
        .addColumn("favorite_count", "INT") \
        .addColumn("last_activity_date_datetime_utc", "TIMESTAMP") \
        .addColumn("last_edit_date_datetime_utc", "TIMESTAMP") \
        .addColumn("last_editor_display_name", "STRING") \
        .addColumn("last_editor_user_id", "INT") \
        .addColumn("owner_display_name", "STRING") \
        .addColumn("owner_user_id", "INT") \
        .addColumn("parent_id", "INT") \
        .addColumn("post_type_id", "INT") \
        .addColumn("score", "INT") \
        .addColumn("tags", "STRING") \
        .addColumn("view_count", "INT") \
        .addColumn("load_date_datetime_utc", "TIMESTAMP") \
        .addColumn("_pk", "INT") \
        .addColumn("valid_date", "DATE") \
        .addColumn("dbx_created_at_datetime_utc", "TIMESTAMP") \
        .addColumn("dbx_updated_at_datetime_utc", "TIMESTAMP") \
        .location("dbfs:/mnt/silver/s_stackoverflow_post_answers") \
        .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS silver_db.s_stackoverflow_post_questions
# MAGIC 
# MAGIC -- SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC -- VACUUM silver_db.s_stackoverflow_post_questions RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %md
# MAGIC Creating delta table instance:

# COMMAND ----------

target_df = DeltaTable.forName(spark, "silver_db.s_stackoverflow_post_answers")
display(target_df.toDF())

# COMMAND ----------

# MAGIC %md
# MAGIC Merging:  
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/delta/merge  
# MAGIC https://www.advancinganalytics.co.uk/blog/2021/9/7/slowly-changing-dimensions-scd-with-delta-and-databricks  

# COMMAND ----------

target_df.alias("target") \
    .merge(
    silver_df.alias("source"),
    "target._pk = source._pk"
    ) \
    .whenMatchedUpdate(set = 
                     {
                        "id": "source.id",
                        "title": "source.title",
                        "accepted_answer_id": "source.accepted_answer_id",
                        "answer_count": "source.answer_count",
                        "comment_count": "source.comment_count",
                        "community_owned_date_datetime_utc": "source.community_owned_date_datetime_utc",
                        "creation_date_datetime_utc": "source.creation_date_datetime_utc",
                        "favorite_count": "source.favorite_count",
                        "last_activity_date_datetime_utc": "source.last_activity_date_datetime_utc",
                        "last_edit_date_datetime_utc": "source.last_edit_date_datetime_utc",
                        "last_editor_display_name": "source.last_editor_display_name",
                        "last_editor_user_id": "source.last_editor_user_id",
                        "owner_display_name": "source.owner_display_name",
                        "owner_user_id": "source.owner_user_id",
                        "parent_id": "source.parent_id",
                        "post_type_id": "source.post_type_id",
                        "score": "source.score",
                        "tags": "source.tags",
                        "view_count": "source.view_count",
                        "load_date_datetime_utc": "source.load_date_datetime_utc",
                        "_pk": "source._pk", 
                        "valid_date": "source.valid_date",
                        "dbx_updated_at_datetime_utc": "to_utc_timestamp(current_timestamp(),'UTC')"
                     }
    ) \
    .whenNotMatchedInsert(values = 
                     {
                        "id": "source.id",
                        "title": "source.title",
                        "accepted_answer_id": "source.accepted_answer_id",
                        "answer_count": "source.answer_count",
                        "comment_count": "source.comment_count",
                        "community_owned_date_datetime_utc": "source.community_owned_date_datetime_utc",
                        "creation_date_datetime_utc": "source.creation_date_datetime_utc",
                        "favorite_count": "source.favorite_count",
                        "last_activity_date_datetime_utc": "source.last_activity_date_datetime_utc",
                        "last_edit_date_datetime_utc": "source.last_edit_date_datetime_utc",
                        "last_editor_display_name": "source.last_editor_display_name",
                        "last_editor_user_id": "source.last_editor_user_id",
                        "owner_display_name": "source.owner_display_name",
                        "owner_user_id": "source.owner_user_id",
                        "parent_id": "source.parent_id",
                        "post_type_id": "source.post_type_id",
                        "score": "source.score",
                        "tags": "source.tags",
                        "view_count": "source.view_count",
                        "load_date_datetime_utc": "source.load_date_datetime_utc",
                        "_pk": "source._pk",
                        "valid_date": "source.valid_date",
                        "dbx_created_at_datetime_utc": "to_utc_timestamp(current_timestamp(),'UTC')"
                     }
    ) \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM silver_db.s_stackoverflow_post_answers
# MAGIC -- WHERE valid_date = "2022-08-31"
# MAGIC -- WHERE _pk = "72595789"

# COMMAND ----------

dbutils.notebook.exit("This notebook successfully run and finished!")
