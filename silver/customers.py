# Databricks notebook source
# Silver : nettoyage technique et typage ; erreurs conservées séparément.
from pyspark.sql import functions as F
SOURCE = "workspace.bronze.customers"
TARGET = "workspace.silver.customers"
TYPES = {}
REQUIRED = ["customer_id"]
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
if "review_score" in df.columns:
    errors = F.concat(errors, F.when(
        F.col("review_score").isNotNull() & ~F.col("review_score").between(1, 5),
        F.array(F.lit("review_score: hors intervalle 1-5"))
    ).otherwise(F.array().cast("array<string>")))
df = df.withColumn("_validation_errors", errors)
invalid = df.filter(F.size("_validation_errors") > 0)
valid = df.filter(F.size("_validation_errors") == 0)
# Ne supprimer que les doublons exacts, sans arbitrer les conflits de clés.
business_columns = [c for c in valid.columns if not c.startswith("_")]
valid = valid.dropDuplicates(business_columns)
valid = valid.withColumn("_cleaned_at", F.current_timestamp())
# COMMAND ----------
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
for output, table in [(invalid, TARGET + "_rejects"), (valid, TARGET)]:
    (output.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(table))
display(spark.table(TARGET).limit(10))
display(spark.table(TARGET + "_rejects").limit(10))
