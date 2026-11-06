"""Lecture du CSV source dans un DataFrame Spark."""

SOURCE = "/Volumes/workspace/default/premierdatasetolist/olist_order_items_dataset.csv"


def load_order_items(spark):
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .csv(SOURCE)
    )
