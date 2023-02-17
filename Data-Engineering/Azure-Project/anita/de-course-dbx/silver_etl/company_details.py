# Databricks notebook source
from pyspark.sql.types import StructType, IntegerType, LongType, BooleanType
import datetime
from pyspark.sql.functions import lit, col, split
from pyspark.sql import SparkSession	
from pyspark.sql.functions import *	
from pyspark.sql import SparkSession
from delta.tables import *
from pyspark.sql import Window
from pyspark.sql.functions import hash 


dbutils.widgets.text("p_file_date",defaultValue="20220731", label="1 file_date")
v_file_date= dbutils.widgets.get("p_file_date")
#v_file_date = '20220930'
date = datetime.datetime.strptime(v_file_date, '%Y%m%d')
print(date.date())

#df = spark.read.format("delta").load("/mnt/bronze/b_company_details")
df = spark.read.table("bronze_db.b_company_detail")

display(df)

# COMMAND ----------


#▪ _pk: a hash value created from the organization_name and valid_from_date column values (or try to use an identity column available in Databricks Runtime 10.4+)
df = df.withColumn('_pk', lit(None))
df = df.withColumn("_pk",col("_pk").cast(IntegerType()))

#▪ is_current: boolean, True if the row is currently valid 
df = df.withColumn('is_current', lit(True))
    
#▪ valid_from_date: business validity start date (p_load_date/p_file_date parameter, for example: ‘2022-07-31’)
df = df.withColumnRenamed("valid_date_datetime_utc", 'valid_from_date')

#▪ valid_to_date: business validity end date, blank when a row is current (one day prior of the load date when the change/delete operation happened, for example: ‘2022-08-30’)
df = df.withColumn('valid_to_date', to_timestamp(lit("")))

#▪ dbx_created_at_datetime_utc: current timestamp of databricks notebook run when the row is created
now = datetime.datetime.utcnow()
df = df.withColumn('dbx_created_at_datetime_utc', lit(now))

#▪ dbx_updated_at_datetime_utc: current timestamp of databricks notebook run when the row is updated
df = df.withColumn('dbx_updated_at_datetime_utc', lit(now))
    
print(df.dtypes)
display(df.orderBy("Organization"))
    
#in case of first run - create silver table

if v_file_date == "20220731":
     df.write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("silver_db.s_company_detail")




# COMMAND ----------


s_df = spark.read.table("silver_db.s_company_detail")
s_df.withColumn('mergeKey', when((col('is_current') == True), expr('Organization')).otherwise(None)
s_df.write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("silver_db.s_company_detail")
                                
def_df = s_df.filter(s_df.is_current == True)
                
display(def_df.orderBy("Organization"))
    

# COMMAND ----------

# Find out records that needs to be inserted or deleted;
stg_df = df.withColumn('action', lit('C')) #\
#            .withColumn('mergeKey', expr('Organization'))
df_indel = stg_df.alias('b').join(def_df.alias('s'), on='Organization', how='full') \
        .where("""(s.is_current=true 
                     AND (s.L1_type <> b.L1_type or 
                     s.L2_type <> b.L2_type or 
                     s.L3_type <> b.L3_type or
                     s.Repository_account <> b.Repository_account or
                     s.Repository_name <> b.Repository_name or
                     s.Tags_array <> b.Tags_array or
                     s.is_Open_source_available <> b.is_Open_source_available
                     )) 
                  OR b.valid_from_date is null
                  OR s.valid_from_date is null""") \
        .select("Organization", "b.valid_from_date", "s.valid_to_date", "b.L1_type", "b.L2_type", "b.L3_type",
                "b.Repository_account", "b.Repository_name", "b.Tags_array", "b.is_Open_source_available", "s.mergeKey", "s.dbx_created_at_datetime_utc","b.dbx_updated_at_datetime_utc",  
                expr("""case when s.valid_from_date is null then 'I' 
                when s.valid_from_date <> b.valid_from_date then 'U' 
             when b.valid_from_date is null then 'D' end as action"""),
                expr("""case when mergeKey is null then Organization 
             else b.mergeKey end as mergeKey""")
                )

display(df_indel)

#staging table for merging inserts
df_insert = df_indel.filter(df_indel.action == 'I')
df_insert = df_insert.withColumn('mergeKey', lit(None))
df_indel2 = df_indel.filter(df_indel.action != 'I')

# COMMAND ----------

# Create a staging table as src for merge
df_src = df_indel2.unionByName(stg_df, allowMissingColumns=True)

display(df_src)
df_src = df_src.withColumn('valid_to_date',
                               expr("""case when action = 'C' 
            then '{0}' - interval 1 seconds
            when action='D' then '{1}'
            else valid_to_date end""".format(date, date))) \
        .withColumn('mergeKey', when((col('action') == 'U'), None).otherwise(col("mergeKey"))) 


display(df_src.orderBy("Organization"))



# COMMAND ----------

deltaTable = DeltaTable.forName(spark, "silver_db.s_company_detail")
merge2 = deltaTable.alias('s').merge(df_insert.alias('b'),
                                              "b.mergeKey = s.mergeKey") \
        .whenMatchedUpdate(set={"mergeKey": col('s.mergeKey'),
                                "Organization": col('s.Organization'),
                                "valid_from_date": col('s.valid_from_date'),
                                "valid_to_date": col('s.valid_to_date'),
                                "L1_type": col('s.L1_type'),
                                "L2_type": col('s.L2_type'),
                                "L3_type": col('s.L3_type'),
                                "Repository_account": col('s.Repository_account'),
                                "Repository_name": col('s.Repository_name'),
                                "Tags_array": col('s.Tags_array'),
                                "is_Open_source_available": col('s.is_Open_source_available'),
                               "dbx_created_at_datetime_utc": col('s.dbx_created_at_datetime_utc'),
                               "dbx_updated_at_datetime_utc": expr('current_timestamp')}) \
        .whenNotMatchedInsert(values={"mergeKey": col('b.mergeKey'),
                                      "Organization": col("b.Organization"),
                                      "L1_type": col('b.L1_type'),
                                      "L2_type": col('b.L2_type'),
                                      "L3_type": col('b.L3_type'),
                                      "Repository_account": col('b.Repository_account'),
                                      "Repository_name": col('b.Repository_name'),
                                      "Tags_array": col('b.Tags_array'),
                                      "is_Open_source_available": col('b.is_Open_source_available'),
                                      "dbx_created_at_datetime_utc": expr('current_timestamp'),
                                      "dbx_updated_at_datetime_utc": expr('current_timestamp'),
                                      "is_current": lit(True),
                                      "valid_from_date": col("b.valid_from_date"),
                                      "valid_to_date": col("b.valid_to_date")
                                     })

merge2.execute()

merge = deltaTable.alias('s').merge(df_src.alias('b'),
                                              "b.mergeKey = s.mergeKey") \
        .whenMatchedUpdate(condition="""s.is_current=true AND b.action='C' AND (s.L1_type <> b.L1_type or
        s.L2_type <> b.L2_type or
        s.L3_type <> b.L3_type or
        s.Repository_account <> b.Repository_account or
        s.Repository_name <> b.Repository_name or
        s.Tags_array <> b.Tags_array or
        s.is_Open_source_available <> b.is_Open_source_available
        )""",
                           set={"mergeKey": col('s.mergeKey'),
                                "Organization": col('s.Organization'),
                                "is_current": lit(False),
                                "valid_from_date": col('s.valid_from_date'),
                                "valid_to_date": col('b.valid_to_date'),
                                "L1_type": col('s.L1_type'),
                                "L2_type": col('s.L2_type'),
                                "L3_type": col('s.L3_type'),
                                "Repository_account": col('s.Repository_account'),
                                "Repository_name": col('s.Repository_name'),
                                "Tags_array": col('s.Tags_array'),
                                "is_Open_source_available": col('s.is_Open_source_available'),
                               "dbx_created_at_datetime_utc": col('s.dbx_created_at_datetime_utc'),
                               "dbx_updated_at_datetime_utc": expr('current_timestamp')}) \
        .whenMatchedUpdate(condition="""s.is_current=true AND b.action='C' AND (s.L1_type == b.L1_type or 
                     s.L2_type == b.L2_type or 
                     s.L3_type == b.L3_type or
                     s.Repository_account == b.Repository_account or
                     s.Repository_name == b.Repository_name or
                     s.Tags_array == b.Tags_array or
                     s.is_Open_source_available == b.is_Open_source_available
                     )""",
                           set={"mergeKey": col('s.mergeKey'),
                                "Organization": col('s.Organization'),
                                "valid_from_date": col('s.valid_from_date'),
                                "valid_to_date": col('s.valid_to_date'),
                                "L1_type": col('s.L1_type'),
                                "L2_type": col('s.L2_type'),
                                "L3_type": col('s.L3_type'),
                                "Repository_account": col('s.Repository_account'),
                                "Repository_name": col('s.Repository_name'),
                                "Tags_array": col('s.Tags_array'),
                                "is_Open_source_available": col('s.is_Open_source_available'),
                               "dbx_created_at_datetime_utc": col('s.dbx_created_at_datetime_utc'),
                               "dbx_updated_at_datetime_utc": expr('current_timestamp')}) \
        .whenMatchedUpdate(condition="s.is_current=true AND b.action='D'",
                           set={"mergeKey": col('s.mergeKey'),
                                "Organization": col('s.Organization'),
                                "is_current": lit(False),
                                "valid_from_date": col('s.valid_from_date'),
                                "valid_to_date": col('b.valid_to_date'),
                                "L1_type": col('s.L1_type'),
                                "L2_type": col('s.L2_type'),
                                "L3_type": col('s.L3_type'),
                                "Repository_account": col('s.Repository_account'),
                                "Repository_name": col('s.Repository_name'),
                                "Tags_array": col('s.Tags_array'),
                                "is_Open_source_available": col('s.is_Open_source_available'),
                               "dbx_created_at_datetime_utc": col('s.dbx_created_at_datetime_utc'),
                               "dbx_updated_at_datetime_utc": expr('current_timestamp')}) \
        .whenNotMatchedInsert(values={"mergeKey": col('b.mergeKey'),
                                      "Organization": col("b.Organization"),
                                      "L1_type": col('b.L1_type'),
                                      "L2_type": col('b.L2_type'),
                                      "L3_type": col('b.L3_type'),
                                      "Repository_account": col('b.Repository_account'),
                                      "Repository_name": col('b.Repository_name'),
                                      "Tags_array": col('b.Tags_array'),
                                      "is_Open_source_available": col('b.is_Open_source_available'),
                                      "dbx_created_at_datetime_utc": expr('current_timestamp'),
                                      "dbx_updated_at_datetime_utc": expr('current_timestamp'),
                                      "is_current": lit(True),
                                      "valid_from_date": col("b.valid_from_date"),
                                      "valid_to_date": col("b.valid_to_date")
                                     })
# Execute the merge
merge.execute()

display(deltaTable.toDF().orderBy("Organization"))

# COMMAND ----------

s_df = spark.read.table("silver_db.s_company_detail")

s_df =(s_df.withColumn('order', row_number().over(Window.partitionBy(lit('1')).orderBy(lit('1'))))#Create increasing id by name
          .withColumn('_pk', dense_rank().over(Window.partitionBy().orderBy('Organization'))).orderBy('order')
          .drop('order'))

s_df = s_df.withColumn("hash_pk", hash(col("Organization"), col("valid_from_date")))

s_df = sd.drop(col('mergeKey'))
display(s_df)

s_df.write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("silver_db.s_company_detail")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from silver_db.s_company_detail
