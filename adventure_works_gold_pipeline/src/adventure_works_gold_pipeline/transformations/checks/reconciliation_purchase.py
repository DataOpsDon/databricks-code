from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="reconciliation_purchase",
    comment="Bronze-to-silver-to-gold reconciliation checks for Adventure Works purchase order line counts and purchase amounts."
)
@dp.expect("purchase_row_counts_match", "bronze_row_count = silver_row_count AND silver_row_count = gold_row_count")
@dp.expect("purchase_amounts_match", "bronze_line_total_amount = silver_line_total_amount AND silver_line_total_amount = gold_line_total_amount")
def reconciliation_purchase():
    bronze = spark.read.table("edp_bronze_dev.adventure_works.purchaseorderdetail")
    silver = spark.read.table("edp_silver_dev.default.silver_purchase_order_detail")
    gold = spark.read.table("fact_purchase")

    bronze_agg = bronze.agg(
        F.count("*").alias("bronze_row_count"),
        F.coalesce(F.sum(F.col("LineTotal").cast("decimal(19,4)")), F.lit(0).cast("decimal(19,4)")).alias("bronze_line_total_amount")
    )
    silver_agg = silver.agg(
        F.count("*").alias("silver_row_count"),
        F.coalesce(F.sum(F.col("line_total").cast("decimal(19,4)")), F.lit(0).cast("decimal(19,4)")).alias("silver_line_total_amount")
    )
    gold_agg = gold.agg(
        F.count("*").alias("gold_row_count"),
        F.coalesce(F.sum(F.col("line_total").cast("decimal(19,4)")), F.lit(0).cast("decimal(19,4)")).alias("gold_line_total_amount")
    )

    return (
        bronze_agg.crossJoin(silver_agg).crossJoin(gold_agg)
        .select(
            F.col("bronze_row_count"),
            F.col("silver_row_count"),
            F.col("gold_row_count"),
            F.col("bronze_line_total_amount"),
            F.col("silver_line_total_amount"),
            F.col("gold_line_total_amount"),
            (F.col("silver_row_count") - F.col("bronze_row_count")).alias("bronze_to_silver_row_diff"),
            (F.col("gold_row_count") - F.col("silver_row_count")).alias("silver_to_gold_row_diff"),
            (F.col("silver_line_total_amount") - F.col("bronze_line_total_amount")).alias("bronze_to_silver_amount_diff"),
            (F.col("gold_line_total_amount") - F.col("silver_line_total_amount")).alias("silver_to_gold_amount_diff")
        )
    )
