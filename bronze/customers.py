# Databricks notebook source
# BRONZE / customers: copie fidèle du CSV, sans nettoyage métier.
# Exécuter une fois par fichier source. Version initiale : overwrite (pas encore incrémentale).

from pyspark.sql import functions as F

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
SOURCE = "/Volumes/workspace/default/premierdatasetolist/olist_customers_dataset.csv"
TARGET = f"{CATALOG}.{BRONZE_SCHEMA}.customers"

# COMMAND ----------
# Tout lire en texte pour ne pas perdre des valeurs à cause de inferSchema.
df = (spark.read.option("header", "true")
      .option("inferSchema", "false")
      .option("mode", "PERMISSIVE")
      .csv(SOURCE))

# Métadonnées de provenance, nécessaires à la traçabilité.
# TODO projet : fournir un load_id stable par lot depuis un Databricks Job.
load_id = "initial_load"
df_bronze = (df.withColumn("_source_file", F.lit(SOURCE))
               .withColumn("_load_id", F.lit(load_id))
               .withColumn("_ingested_at", F.current_timestamp()))

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
(df_bronze.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(TARGET))
print(f"Bronze créée : {TARGET} — {spark.table(TARGET).count()} lignes")
display(spark.table(TARGET).limit(10))
