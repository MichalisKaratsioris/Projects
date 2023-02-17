# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, DateType, TimestampType, LongType
from pyspark.sql.functions import current_timestamp, col, to_timestamp, lit, split, to_date, to_utc_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read bronze table and adding technical columns

# COMMAND ----------

silver_questions_df = spark.read.format("delta").load(f"/mnt/gfaproject/bronze/b_stackoverflow_questions/")
#silver_questions_df.where(to_date(col(f"last_activity_date_datetime_utc"), "yyyyMMdd") <= to_date(lit(f"{v_file_date}"), "yyyyMMdd")).show(5, False)

# COMMAND ----------

creating_time = current_timestamp()
silver_questions_df = silver_questions_df.withColumn("valid_date", to_date(lit(v_file_date), "yyyyMMdd"))
silver_questions_df = silver_questions_df.withColumn("dbx_created_at_datetiime_utc", creating_time)
silver_questions_df = silver_questions_df.withColumn("dbx_updated_at_datetime_utc", col("dbx_created_at_datetiime_utc"))

# COMMAND ----------



# COMMAND ----------

display(silver_questions_df)
silver_questions_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - create a table if not exists or merge tables

# COMMAND ----------

if spark._jsparkSession.catalog().tableExists('silver_db', 's_stackoverflow_questions'):
    print("Table called: silver_db.s_stackoverflow_questions exist! Let's merge tables!")
    # Convert table to Delta
    
    deltaTable = DeltaTable.forName(spark, "silver_db.s_stackoverflow_questions")

    # Merge Delta table with new dataset
    deltaTable.alias("original1").merge(silver_questions_df.alias("updates1"),"original1._pk = updates1._pk") \
    .whenMatchedUpdate(set = 
                         {
                            "id": "updates1.id",
                            "title": "updates1.title",
                            "accepted_answer_id": "updates1.accepted_answer_id",
                            "answer_count": "updates1.answer_count",
                            "comment_count": "updates1.comment_count",
                            "community_owned_date_datetime_utc": "updates1.community_owned_date_datetime_utc",
                            "created_at_datetime_utc": "updates1.created_at_datetime_utc",
                            "favorite_count": "updates1.favorite_count",
                            "last_activity_date_datetime_utc": "updates1.last_activity_date_datetime_utc",
                            "last_edit_date_datetime_utc": "updates1.last_edit_date_datetime_utc",
                            "last_editor_display_name": "updates1.last_editor_display_name",
                            "last_editor_user_id": "updates1.last_editor_user_id",
                            "owner_display_name": "updates1.owner_display_name",
                            "owner_user_id": "updates1.owner_user_id",
                            "parent_id": "updates1.parent_id",
                            "post_type_id": "updates1.post_type_id",
                            "score": "updates1.score",
                            "tags": "updates1.tags",
                            "view_count": "updates1.view_count",
                            "_pk": "updates1._pk",
                            "loaded_at_datetime_utc": "updates1.loaded_at_datetime_utc",
                            "valid_date": "updates1.valid_date",
                            "dbx_updated_at_datetime_utc": "to_utc_timestamp(current_timestamp(),'UTC')"
                         }
        ) \
        .whenNotMatchedInsert(values = 
                         {
                            "id": "updates1.id",
                            "title": "updates1.title",
                            "accepted_answer_id": "updates1.accepted_answer_id",
                            "answer_count": "updates1.answer_count",
                            "comment_count": "updates1.comment_count",
                            "community_owned_date_datetime_utc": "updates1.community_owned_date_datetime_utc",
                            "created_at_datetime_utc": "updates1.created_at_datetime_utc",
                            "favorite_count": "updates1.favorite_count",
                            "last_activity_date_datetime_utc": "updates1.last_activity_date_datetime_utc",
                            "last_edit_date_datetime_utc": "updates1.last_edit_date_datetime_utc",
                            "last_editor_display_name": "updates1.last_editor_display_name",
                            "last_editor_user_id": "updates1.last_editor_user_id",
                            "owner_display_name": "updates1.owner_display_name",
                            "owner_user_id": "updates1.owner_user_id",
                            "parent_id": "updates1.parent_id",
                            "post_type_id": "updates1.post_type_id",
                            "score": "updates1.score",
                            "tags": "updates1.tags",
                            "view_count": "updates1.view_count",
                            "_pk": "updates1._pk",
                            "loaded_at_datetime_utc": "updates1.loaded_at_datetime_utc",
                            "valid_date": "updates1.valid_date",
                            "dbx_created_at_datetiime_utc": "to_utc_timestamp(current_timestamp(),'UTC')",
                            "dbx_updated_at_datetime_utc": "to_utc_timestamp(current_timestamp(),'UTC')"
                         }
        ) \
    .execute()

else:
    print("Not exist, creating table called: silver_db.s_stackoverflow_questions.")
    silver_questions_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("silver_db.s_stackoverflow_questions")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_db.s_stackoverflow_questions
