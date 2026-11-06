"""Lecture du CSV source dans un DataFrame Spark."""

SOURCE = "/Volumes/workspace/default/premierdatasetolist/product_category_name_translation.csv"


def load_category_translation(spark):
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .csv(SOURCE)
    )
