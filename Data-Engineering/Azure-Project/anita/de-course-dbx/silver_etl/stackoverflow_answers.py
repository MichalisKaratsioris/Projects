# Databricks notebook source
from pyspark.sql.types import StructType, IntegerType, LongType
import datetime
from pyspark.sql.functions import lit, col, split
from pyspark.sql import SparkSession	
from pyspark.sql.functions import *	



dbutils.widgets.text("p_file_date",defaultValue="20220731", label="1 file_date")
v_file_date= dbutils.widgets.get("p_file_date")

date = datetime.datetime.strptime(v_file_date, '%Y%m%d')
print(date.date())


# COMMAND ----------

#b_df = spark.read.format("delta").load("/mnt/bronze/b_stackoverflow_answers")
b_df = spark.read.table("bronze_db.b_stackoverflow_answers")

#in case of first run - create silver table

if v_file_date == "20220731":
    b_df.write.mode("overwrite").format("delta").saveAsTable("silver_db.s_stackoverflow_answers")      

# COMMAND ----------

#in case of first run - create technical columns

if v_file_date == "20220731":
    s_df = spark.read.table("silver_db.s_stackoverflow_answers")
    #valid_date: validity date (p_load_date/p_file_date parameter used when the row is created or updated, for example: ‘2022-07-31’) - DONE in bronze layer


    #dbx_created_at_datetime_utc: current timestamp of databricks notebook run when the row is created - NOT DONE
    now = datetime.datetime.utcnow()
    s_df = s_df.withColumn('dbx_created_at_datetime_utc', lit(now))

    #▪ dbx_updated_at_datetime_utc: current timestamp of databricks notebook run when the row is updated - NOT DONE
    s_df = s_df.withColumn('dbx_updated_at_datetime_utc', lit(""))
    s_df = s_df.withColumn('dbx_updated_at_datetime_utc', to_timestamp('dbx_updated_at_datetime_utc'))
    
    #check that when a stackoverflow question is updated the SCD1 operation is working correctly:
    #▪ pick a column and change its value on 1000 rows when it is running with the - answer_count
    
    
    print(s_df.dtypes)
    s_df.show()
    
    s_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("silver_db.s_stackoverflow_answers")
    

# COMMAND ----------

# MAGIC %sql
# MAGIC ---change rows for testing
# MAGIC 
# MAGIC update silver_db.s_stackoverflow_answers as silver
# MAGIC set answer_count = "20";

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO silver_db.s_stackoverflow_answers as s
# MAGIC USING bronze_db.b_stackoverflow_answers as b
# MAGIC 
# MAGIC ON s._pk = b._pk
# MAGIC 
# MAGIC WHEN MATCHED THEN UPDATE 
# MAGIC SET    s.id = b.id,
# MAGIC  s.title = b.title,
# MAGIC s.accepted_answer_id = b.accepted_answer_id,
# MAGIC s.answer_count = b.answer_count,
# MAGIC s.comment_count = b.comment_count,
# MAGIC s.community_owned_date_datetime_utc= b.community_owned_date_datetime_utc,
# MAGIC s.creation_date_datetime_utc=b.creation_date_datetime_utc,
# MAGIC s.favorite_count = b.favorite_count,
# MAGIC s.last_activity_date_datetime_utc = b.last_activity_date_datetime_utc,
# MAGIC s.last_edit_date_datetime_utc = b.last_edit_date_datetime_utc,
# MAGIC s.last_editor_display_name = b.last_editor_display_name,
# MAGIC s.last_editor_user_id = b.last_editor_user_id,
# MAGIC s.owner_display_name = b.owner_display_name,
# MAGIC s.owner_user_id = b.owner_user_id,
# MAGIC s.parent_id = b.parent_id,
# MAGIC s.post_type_id =b.post_type_id,
# MAGIC s.score=b.score,
# MAGIC s.tags=b.tags,
# MAGIC s.view_count = b.view_count,
# MAGIC s.valid_date = b.valid_date,
# MAGIC s.dbx_updated_at_datetime_utc = CURRENT_TIMESTAMP()
# MAGIC ;
# MAGIC --WHEN NOT MATCHED THEN INSERT * and update set silver.dbx_created_at_datetime_utc = getutcdate() and update set silver.dbx_updated_at_datetime_utc = GETUTCDATE()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_db.s_stackoverflow_answers LIMIT 100
