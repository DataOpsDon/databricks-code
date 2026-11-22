from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="fact_purchase",
    comment="Adventure Works purchase fact table at the purchase order line grain."
)
@dp.expect_or_drop("valid_purchase_line", "purchase_order_id IS NOT NULL AND purchase_order_detail_id IS NOT NULL")
@dp.expect("positive_purchase_quantity", "order_qty > 0")
@dp.expect("non_negative_purchase_amount", "line_total >= 0")
def fact_purchase():
    purchase_header = spark.read.table("edp_silver_dev.default.silver_purchase_order_header").alias("h")
    purchase_detail = spark.read.table("edp_silver_dev.default.silver_purchase_order_detail").alias("d")

    return (
        purchase_detail.join(purchase_header, "purchase_order_id", "inner")
        .select(
            F.sha2(F.concat_ws("||", F.col("purchase_order_id").cast("string"), F.col("purchase_order_detail_id").cast("string")), 256).alias("fact_purchase_key"),
            F.col("purchase_order_id"),
            F.col("purchase_order_detail_id"),
            F.date_format(F.col("h.order_date"), "yyyyMMdd").cast("int").alias("order_date_key"),
            F.date_format(F.col("d.due_date"), "yyyyMMdd").cast("int").alias("due_date_key"),
            F.date_format(F.col("h.ship_date"), "yyyyMMdd").cast("int").alias("ship_date_key"),
            F.col("h.vendor_id").alias("vendor_key"),
            F.col("d.product_id").alias("product_key"),
            F.col("h.employee_id"),
            F.col("h.ship_method_id"),
            F.col("h.status_code"),
            F.col("h.status_name"),
            F.col("d.order_qty"),
            F.col("d.unit_price"),
            F.col("d.line_total"),
            F.col("d.received_qty"),
            F.col("d.rejected_qty"),
            F.col("d.stocked_qty"),
            F.greatest(F.col("h.source_modified_at"), F.col("d.source_modified_at")).alias("latest_source_modified_at")
        )
    )
