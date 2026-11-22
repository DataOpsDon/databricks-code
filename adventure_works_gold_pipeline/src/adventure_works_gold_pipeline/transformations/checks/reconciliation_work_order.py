from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="reconciliation_work_order",
    comment="Bronze-to-silver-to-gold reconciliation checks for Adventure Works work order counts and production quantities."
)
@dp.expect("work_order_row_counts_match", "bronze_row_count = silver_row_count AND silver_row_count = gold_row_count")
@dp.expect("work_order_order_qty_match", "bronze_order_qty = silver_order_qty AND silver_order_qty = gold_order_qty")
def reconciliation_work_order():
    bronze = spark.read.table("edp_bronze_dev.adventure_works.workorder")
    silver = spark.read.table("edp_silver_dev.default.silver_work_order")
    gold = spark.read.table("fact_work_order")

    bronze_agg = bronze.agg(
        F.count("*").alias("bronze_row_count"),
        F.coalesce(F.sum(F.col("OrderQty").cast("bigint")), F.lit(0)).alias("bronze_order_qty"),
        F.coalesce(F.sum(F.col("StockedQty").cast("bigint")), F.lit(0)).alias("bronze_stocked_qty"),
        F.coalesce(F.sum(F.col("ScrappedQty").cast("bigint")), F.lit(0)).alias("bronze_scrapped_qty")
    )
    silver_agg = silver.agg(
        F.count("*").alias("silver_row_count"),
        F.coalesce(F.sum(F.col("order_qty").cast("bigint")), F.lit(0)).alias("silver_order_qty"),
        F.coalesce(F.sum(F.col("stocked_qty").cast("bigint")), F.lit(0)).alias("silver_stocked_qty"),
        F.coalesce(F.sum(F.col("scrapped_qty").cast("bigint")), F.lit(0)).alias("silver_scrapped_qty")
    )
    gold_agg = gold.agg(
        F.count("*").alias("gold_row_count"),
        F.coalesce(F.sum(F.col("order_qty").cast("bigint")), F.lit(0)).alias("gold_order_qty"),
        F.coalesce(F.sum(F.col("stocked_qty").cast("bigint")), F.lit(0)).alias("gold_stocked_qty"),
        F.coalesce(F.sum(F.col("scrapped_qty").cast("bigint")), F.lit(0)).alias("gold_scrapped_qty")
    )

    return (
        bronze_agg.crossJoin(silver_agg).crossJoin(gold_agg)
        .select(
            F.col("bronze_row_count"),
            F.col("silver_row_count"),
            F.col("gold_row_count"),
            F.col("bronze_order_qty"),
            F.col("silver_order_qty"),
            F.col("gold_order_qty"),
            F.col("bronze_stocked_qty"),
            F.col("silver_stocked_qty"),
            F.col("gold_stocked_qty"),
            F.col("bronze_scrapped_qty"),
            F.col("silver_scrapped_qty"),
            F.col("gold_scrapped_qty"),
            (F.col("silver_row_count") - F.col("bronze_row_count")).alias("bronze_to_silver_row_diff"),
            (F.col("gold_row_count") - F.col("silver_row_count")).alias("silver_to_gold_row_diff"),
            (F.col("silver_order_qty") - F.col("bronze_order_qty")).alias("bronze_to_silver_order_qty_diff"),
            (F.col("gold_order_qty") - F.col("silver_order_qty")).alias("silver_to_gold_order_qty_diff")
        )
    )
