from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="reconciliation_customer",
    comment="Bronze-to-silver-to-gold reconciliation checks for Adventure Works customer dimension row counts."
)
@dp.expect("customer_row_counts_match", "bronze_row_count = silver_row_count AND silver_row_count = gold_row_count")
def reconciliation_customer():
    bronze = spark.read.table("edp_bronze_dev.adventure_works.customer")
    silver = spark.read.table("edp_silver_dev.default.silver_customer")
    gold = spark.read.table("dim_customer")

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
