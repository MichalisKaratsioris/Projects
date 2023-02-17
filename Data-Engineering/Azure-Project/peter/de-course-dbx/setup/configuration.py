# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("loaded_at_datetime_utc", current_timestamp())
    return output_df
