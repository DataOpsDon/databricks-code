from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="reconciliation_product",
    comment="Bronze-to-silver-to-gold reconciliation checks for Adventure Works product dimension row counts."
)
@dp.expect("product_row_counts_match", "bronze_row_count = silver_row_count AND silver_row_count = gold_row_count")
def reconciliation_product():
    bronze = spark.read.table("edp_bronze_dev.adventure_works.product")
    silver = spark.read.table("edp_silver_dev.default.silver_product")
    gold = spark.read.table("dim_product")

    bronze_agg = bronze.agg(F.count("*").alias("bronze_row_count"))
    silver_agg = silver.agg(F.count("*").alias("silver_row_count"))
    gold_agg = gold.agg(F.count("*").alias("gold_row_count"))

    return (
        bronze_agg.crossJoin(silver_agg).crossJoin(gold_agg)
        .select(
            F.col("bronze_row_count"),
            F.col("silver_row_count"),
            F.col("gold_row_count"),
            (F.col("silver_row_count") - F.col("bronze_row_count")).alias("bronze_to_silver_row_diff"),
            (F.col("gold_row_count") - F.col("silver_row_count")).alias("silver_to_gold_row_diff")
        )
    )
