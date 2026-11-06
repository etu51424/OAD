"""Lecture du CSV source dans un DataFrame Spark."""

SOURCE = "/Volumes/workspace/default/premierdatasetolist/olist_marketing_qualified_leads_dataset.csv"


def load_marketing_qualified_leads(spark):
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .csv(SOURCE)
    )
