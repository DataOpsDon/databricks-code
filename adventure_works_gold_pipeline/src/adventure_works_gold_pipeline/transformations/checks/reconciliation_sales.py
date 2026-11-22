from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="reconciliation_sales",
    comment="Bronze-to-silver-to-gold reconciliation checks for Adventure Works sales order line counts and net sales amounts."
)
@dp.expect("sales_row_counts_match", "bronze_row_count = silver_row_count AND silver_row_count = gold_row_count")
@dp.expect("sales_amounts_match", "bronze_net_sales_amount = silver_net_sales_amount AND silver_net_sales_amount = gold_net_sales_amount")
def reconciliation_sales():
    bronze = spark.read.table("edp_bronze_dev.adventure_works.salesorderdetail")
    silver = spark.read.table("edp_silver_dev.default.silver_sales_order_detail")
    gold = spark.read.table("fact_sales")

    bronze_agg = bronze.agg(
        F.count("*").alias("bronze_row_count"),
        F.coalesce(F.sum(F.col("LineTotal").cast("decimal(38,6)")), F.lit(0).cast("decimal(38,6)")).alias("bronze_net_sales_amount")
    )
    silver_agg = silver.agg(
        F.count("*").alias("silver_row_count"),
        F.coalesce(F.sum(F.col("line_total").cast("decimal(38,6)")), F.lit(0).cast("decimal(38,6)")).alias("silver_net_sales_amount")
    )
    gold_agg = gold.agg(
        F.count("*").alias("gold_row_count"),
        F.coalesce(F.sum(F.col("net_sales_amount").cast("decimal(38,6)")), F.lit(0).cast("decimal(38,6)")).alias("gold_net_sales_amount")
    )

    return (
        bronze_agg.crossJoin(silver_agg).crossJoin(gold_agg)
        .select(
            F.col("bronze_row_count"),
            F.col("silver_row_count"),
            F.col("gold_row_count"),
            F.col("bronze_net_sales_amount"),
            F.col("silver_net_sales_amount"),
            F.col("gold_net_sales_amount"),
            (F.col("silver_row_count") - F.col("bronze_row_count")).alias("bronze_to_silver_row_diff"),
            (F.col("gold_row_count") - F.col("silver_row_count")).alias("silver_to_gold_row_diff"),
            (F.col("silver_net_sales_amount") - F.col("bronze_net_sales_amount")).alias("bronze_to_silver_amount_diff"),
            (F.col("gold_net_sales_amount") - F.col("silver_net_sales_amount")).alias("silver_to_gold_amount_diff")
        )
    )
