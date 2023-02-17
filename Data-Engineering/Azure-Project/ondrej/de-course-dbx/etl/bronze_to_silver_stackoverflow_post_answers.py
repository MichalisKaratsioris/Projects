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

# MAGIC %md
# MAGIC ###Pick a column and change its value on 1000 rows

# COMMAND ----------

# %sql
# SELECT COUNT(*), MIN(_pk), MAX(_pk)
# FROM bronze_db.b_stackoverflow_post_answers;

# COMMAND ----------

# %sql
# SELECT COUNT(*), MIN(_pk), MAX(_pk)
# FROM bronze_db.b_stackoverflow_post_answers
# WHERE last_activity_datetime_utc <= valid_date;

# COMMAND ----------

# %sql
# SELECT COUNT(*), MIN(_pk), MAX(_pk)
# FROM bronze_db.b_stackoverflow_post_answers
# WHERE last_activity_datetime_utc > valid_date;

# COMMAND ----------

# %sql
# SELECT COUNT(*), MIN(_pk), MAX(_pk)
# FROM bronze_db.b_stackoverflow_post_answers
# WHERE last_activity_datetime_utc <= valid_date AND _pk BETWEEN '70546320' AND '70548900';

# COMMAND ----------

# %sql
# SELECT COUNT(*), MIN(_pk), MAX(_pk)
# FROM bronze_db.b_stackoverflow_post_answers
# WHERE last_activity_datetime_utc <= valid_date AND _pk BETWEEN '73546879' AND '73549580';

# COMMAND ----------

if v_file_date == "20220731":
    v_update_start = 70546320
    v_update_end = 70548900
    spark.sql(f"UPDATE bronze_db.b_stackoverflow_post_answers SET title = 'Dummy Title {v_file_date}' WHERE last_activity_datetime_utc <= valid_date AND _pk BETWEEN '{v_update_start}' AND '{v_update_end}';")
elif v_file_date == "20220831":
    v_update_start = 73546879
    v_update_end = 73549580
    spark.sql(f"UPDATE bronze_db.b_stackoverflow_post_answers SET title = 'Dummy Title {v_file_date}' WHERE last_activity_datetime_utc <= valid_date AND _pk BETWEEN '{v_update_start}' AND '{v_update_end}';")
else:
    print("No update")

# COMMAND ----------

# %sql
# SELECT COUNT(*), MIN(_pk), MAX(_pk)
# FROM bronze_db.b_stackoverflow_post_answers
# WHERE title LIKE 'Dummy Title %%%%%%%%';

# COMMAND ----------

# %sql
# SELECT *
# FROM bronze_db.b_stackoverflow_post_answers
# WHERE title LIKE 'Dummy Title %%%%%%%%'
# LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read the data from the bronze layer using the spark dataframe reader

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# display(dbutils.fs.ls("/mnt/decoursesa/bronze/b_stackoverflow_post_answers"))

# COMMAND ----------

df = spark.read \
.format("delta") \
.load("/mnt/decoursesa/bronze/b_stackoverflow_post_answers")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Filter the source data

# COMMAND ----------

filtered_df = df.filter("last_activity_datetime_utc <= valid_date")

# COMMAND ----------

display(filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Temp View

# COMMAND ----------

filtered_df.createOrReplaceTempView("temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), MIN(_pk), MAX(_pk)
# MAGIC FROM temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), MIN(_pk), MAX(_pk)
# MAGIC FROM temp_view
# MAGIC WHERE title LIKE 'Dummy Title %%%%%%%%';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM temp_view
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM temp_view
# MAGIC WHERE title LIKE 'Dummy Title %%%%%%%%'
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Silver Table

# COMMAND ----------

# %sql
# DROP TABLE silver_db.s_stackoverflow_post_answers;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_db.s_stackoverflow_post_answers (
# MAGIC id INT,
# MAGIC title STRING,
# MAGIC accepted_answer_id INT,
# MAGIC answer_count INT,
# MAGIC comment_count INT,
# MAGIC community_owned_datetime_utc TIMESTAMP,
# MAGIC created_at_datetime_utc TIMESTAMP,
# MAGIC favorite_count INT,
# MAGIC last_activity_datetime_utc TIMESTAMP,
# MAGIC last_edit_datetime_utc TIMESTAMP,
# MAGIC last_editor_display_name STRING,
# MAGIC last_editor_user_id INT,
# MAGIC owner_display_name STRING,
# MAGIC owner_user_id INT,
# MAGIC parent_id INT,
# MAGIC post_type_id INT,
# MAGIC score INT,
# MAGIC tags STRING,
# MAGIC view_count INT,
# MAGIC _pk INT,
# MAGIC loaded_at_datetime_utc TIMESTAMP,
# MAGIC valid_date DATE,
# MAGIC dbx_created_at_datetime_utc TIMESTAMP,
# MAGIC dbx_updated_at_datetime_utc TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# %sql
# DESCRIBE EXTENDED silver_db.s_stackoverflow_post_answers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), MIN(_pk), MAX(_pk)
# MAGIC FROM silver_db.s_stackoverflow_post_answers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), MIN(_pk), MAX(_pk)
# MAGIC FROM silver_db.s_stackoverflow_post_answers
# MAGIC WHERE title LIKE 'Dummy Title %%%%%%%%';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_db.s_stackoverflow_post_answers
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_db.s_stackoverflow_post_answers
# MAGIC WHERE title LIKE 'Dummy Title %%%%%%%%'
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Merge into Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_db.s_stackoverflow_post_answers AS target
# MAGIC USING temp_view AS source
# MAGIC ON target._pk = source._pk
# MAGIC WHEN MATCHED AND (!(target.id <=> source.id) OR !(target.title <=> source.title) OR !(target.accepted_answer_id <=> source.accepted_answer_id) OR !(target.answer_count <=> source.answer_count) OR !(target.comment_count <=> source.comment_count) OR !(target.community_owned_datetime_utc <=> source.community_owned_datetime_utc) OR !(target.created_at_datetime_utc <=> source.created_at_datetime_utc) OR !(target.favorite_count <=> source.favorite_count) OR !(target.last_activity_datetime_utc <=> source.last_activity_datetime_utc) OR !(target.last_edit_datetime_utc <=> source.last_edit_datetime_utc) OR !(target.last_editor_display_name <=> source.last_editor_display_name) OR !(target.last_editor_user_id <=> source.last_editor_user_id) OR !(target.owner_display_name <=> source.owner_display_name) OR !(target.owner_user_id <=> source.owner_user_id) OR !(target.parent_id <=> source.parent_id) OR !(target.post_type_id <=> source.post_type_id) OR !(target.score <=> source.score) OR !(target.tags <=> source.tags) OR !(target.view_count <=> source.view_count)) THEN UPDATE SET
# MAGIC target.id = source.id,
# MAGIC target.title = source.title,
# MAGIC target.accepted_answer_id = source.accepted_answer_id,
# MAGIC target.answer_count = source.answer_count,
# MAGIC target.comment_count = source.comment_count,
# MAGIC target.community_owned_datetime_utc = source.community_owned_datetime_utc,
# MAGIC target.created_at_datetime_utc = source.created_at_datetime_utc,
# MAGIC target.favorite_count = source.favorite_count,
# MAGIC target.last_activity_datetime_utc = source.last_activity_datetime_utc,
# MAGIC target.last_edit_datetime_utc = source.last_edit_datetime_utc,
# MAGIC target.last_editor_display_name = source.last_editor_display_name,
# MAGIC target.last_editor_user_id = source.last_editor_user_id,
# MAGIC target.owner_display_name = source.owner_display_name,
# MAGIC target.owner_user_id = source.owner_user_id,
# MAGIC target.parent_id = source.parent_id,
# MAGIC target.post_type_id = source.post_type_id,
# MAGIC target.score = source.score,
# MAGIC target.tags = source.tags,
# MAGIC target.view_count = source.view_count,
# MAGIC target.loaded_at_datetime_utc = source.loaded_at_datetime_utc,
# MAGIC target.valid_date = source.valid_date,
# MAGIC target.dbx_updated_at_datetime_utc = current_timestamp
# MAGIC WHEN NOT MATCHED THEN INSERT (id, title, accepted_answer_id, answer_count, comment_count, community_owned_datetime_utc, created_at_datetime_utc, favorite_count, last_activity_datetime_utc, last_edit_datetime_utc, last_editor_display_name, last_editor_user_id, owner_display_name, owner_user_id, parent_id, post_type_id, score, tags, view_count, _pk, loaded_at_datetime_utc, valid_date, dbx_created_at_datetime_utc) VALUES (id, title, accepted_answer_id, answer_count, comment_count, community_owned_datetime_utc, created_at_datetime_utc, favorite_count, last_activity_datetime_utc, last_edit_datetime_utc, last_editor_display_name, last_editor_user_id, owner_display_name, owner_user_id, parent_id, post_type_id, score, tags, view_count, _pk, loaded_at_datetime_utc, valid_date, current_timestamp);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), MIN(_pk), MAX(_pk)
# MAGIC FROM silver_db.s_stackoverflow_post_answers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), MIN(_pk), MAX(_pk)
# MAGIC FROM silver_db.s_stackoverflow_post_answers
# MAGIC WHERE title LIKE 'Dummy Title %%%%%%%%';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_db.s_stackoverflow_post_answers
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_db.s_stackoverflow_post_answers
# MAGIC WHERE title LIKE 'Dummy Title %%%%%%%%'
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_db.s_stackoverflow_post_answers
# MAGIC WHERE id = '70546320';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_db.s_stackoverflow_post_answers
# MAGIC WHERE id = '73549580';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_db.s_stackoverflow_post_answers
# MAGIC WHERE id = '73842328';

# COMMAND ----------

dbutils.notebook.exit(f"Success for file date parameter {v_file_date}")
