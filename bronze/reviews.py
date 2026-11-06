# Databricks notebook source
# Bronze : copie du CSV en texte, avec provenance.
from pyspark.sql import functions as F
SOURCE = "/Volumes/workspace/default/premierdatasetolist/olist_order_reviews_dataset.csv"
TARGET = "workspace.bronze.reviews"
# COMMAND ----------
df = (spark.read.option("header", "true").option("inferSchema", "false")
      .option("multiLine", "true").option("escape", '"')
      .option("mode", "FAILFAST").csv(SOURCE))
df = (df.withColumn("_source_file", F.lit(SOURCE))
      .withColumn("_load_id", F.lit("initial_load"))
      .withColumn("_ingested_at", F.current_timestamp()))
# COMMAND ----------
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
# Chargement complet : remplace cette table cible.
(df.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(TARGET))
display(spark.table(TARGET).limit(10))
