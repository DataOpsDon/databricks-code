from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="fact_work_order",
    comment="Adventure Works manufacturing work order fact table at the work order grain."
)
@dp.expect_or_drop("valid_work_order_key", "work_order_id IS NOT NULL")
@dp.expect("positive_work_order_quantity", "order_qty > 0")
def fact_work_order():
    work_order = spark.read.table("edp_silver_dev.default.silver_work_order")

    return work_order.select(
        F.col("work_order_id").alias("fact_work_order_key"),
        F.col("work_order_id"),
        F.date_format(F.col("start_date"), "yyyyMMdd").cast("int").alias("start_date_key"),
        F.date_format(F.col("end_date"), "yyyyMMdd").cast("int").alias("end_date_key"),
        F.date_format(F.col("due_date"), "yyyyMMdd").cast("int").alias("due_date_key"),
        F.col("product_id").alias("product_key"),
        F.col("scrap_reason_id"),
        F.col("order_qty"),
        F.col("stocked_qty"),
        F.col("scrapped_qty"),
        F.col("source_modified_at").alias("latest_source_modified_at")
    )
