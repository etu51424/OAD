# Databricks notebook source
# Silver : nettoyage technique et typage ; erreurs conservées séparément.
from pyspark.sql import functions as F
SOURCE = "workspace.bronze.geolocation"
TARGET = "workspace.silver.geolocation"
TYPES = {"geolocation_lat":"DOUBLE","geolocation_lng":"DOUBLE"}
REQUIRED = ["geolocation_zip_code_prefix"]
COORDINATE_BOUNDS = {"geolocation_lat": (-90, 90), "geolocation_lng": (-180, 180)}
# COMMAND ----------
df = spark.table(SOURCE)
for column, dtype in df.dtypes:
    if dtype == "string" and not column.startswith("_"):
        df = df.withColumn(column, F.when(F.trim(F.col(column)) == "", None)
                           .otherwise(F.trim(F.col(column))))
errors = F.array().cast("array<string>")
for column, dtype in TYPES.items():
    raw = column + "_raw"
    df = df.withColumn(raw, F.col(column))
    df = df.withColumn(column, F.expr(f"try_cast(`{column}` AS {dtype})"))
    errors = F.concat(errors, F.when(
        F.col(raw).isNotNull() & F.col(column).isNull(),
        F.array(F.lit(column + ": type invalide"))
    ).otherwise(F.array().cast("array<string>")))
for column in REQUIRED:
    errors = F.concat(errors, F.when(F.col(column).isNull(),
        F.array(F.lit(column + ": obligatoire")))
        .otherwise(F.array().cast("array<string>")))
# Une ligne de geolocalisation doit avoir deux coordonnees utilisables.
for column, (minimum, maximum) in COORDINATE_BOUNDS.items():
    coordinate = F.col(column)
    errors = F.concat(
        errors,
        F.when(F.col(column + "_raw").isNull(),
               F.array(F.lit(column + ": obligatoire")))
        .when(coordinate.isNotNull() & ~coordinate.between(minimum, maximum),
              F.array(F.lit(column + ": hors limites")))
        .otherwise(F.array().cast("array<string>")),
    )
df = df.withColumn("_validation_errors", errors)
invalid = df.filter(F.size("_validation_errors") > 0)
valid = df.filter(F.size("_validation_errors") == 0)
# Ignorer les valeurs avant conversion pour comparer les coordonnées nettoyées.
raw_coordinates = {column + "_raw" for column in COORDINATE_BOUNDS}
business_columns = [
    column for column in valid.columns
    if not column.startswith("_") and column not in raw_coordinates
]
valid = valid.dropDuplicates(business_columns)
valid = valid.withColumn("_cleaned_at", F.current_timestamp())
# COMMAND ----------
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
for output, table in [(invalid, TARGET + "_rejects"), (valid, TARGET)]:
    (output.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(table))
display(spark.table(TARGET).limit(10))
display(spark.table(TARGET + "_rejects").limit(10))
