from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dim_vendor",
    comment="Adventure Works vendor dimension derived from available vendor identifiers and purchasing relationship metrics because the source does not include a standalone vendor master."
)
@dp.expect_or_drop("valid_vendor_key", "vendor_key IS NOT NULL")
def dim_vendor():
    purchase_header = (
        spark.read.table("edp_silver_dev.default.silver_purchase_order_header")
        .where(F.col("vendor_id").isNotNull())
    )
    product_vendor = (
        spark.read.table("edp_silver_dev.default.silver_product_vendor")
        .where(F.col("vendor_id").isNotNull())
    )

    vendor_keys = (
        purchase_header.select("vendor_id")
        .unionByName(product_vendor.select("vendor_id"))
        .distinct()
    )

    purchase_rollup = purchase_header.groupBy("vendor_id").agg(
        F.countDistinct("purchase_order_id").alias("purchase_order_count"),
        F.min("order_date").alias("first_purchase_order_date"),
        F.max("order_date").alias("latest_purchase_order_date"),
        F.max("ship_date").alias("latest_purchase_ship_date"),
        F.max("source_modified_at").alias("purchase_source_modified_at")
    )

    product_vendor_rollup = product_vendor.groupBy("vendor_id").agg(
        F.countDistinct("product_id").alias("supplied_product_count"),
        F.round(F.avg(F.col("average_lead_time_days").cast("double")), 2).alias("average_lead_time_days"),
        F.round(F.avg(F.col("standard_price").cast("double")), 4).alias("average_standard_price"),
        F.round(F.avg(F.col("last_receipt_cost").cast("double")), 4).alias("average_last_receipt_cost"),
        F.max("last_receipt_date").alias("latest_receipt_date"),
        F.max("source_modified_at").alias("product_vendor_source_modified_at")
    )

    return (
        vendor_keys.alias("vk")
        .join(purchase_rollup.alias("ph"), "vendor_id", "left")
        .join(product_vendor_rollup.alias("pv"), "vendor_id", "left")
        .select(
            F.col("vendor_id").alias("vendor_key"),
            F.col("vendor_id"),
            F.lit(None).cast("string").alias("vendor_name"),
            F.col("purchase_order_count"),
            F.col("supplied_product_count"),
            F.col("first_purchase_order_date"),
            F.col("latest_purchase_order_date"),
            F.col("latest_purchase_ship_date"),
            F.col("latest_receipt_date"),
            F.col("average_lead_time_days"),
            F.col("average_standard_price"),
            F.col("average_last_receipt_cost"),
            F.when(F.col("purchase_order_count").isNotNull(), F.lit(True)).otherwise(F.lit(False)).alias("has_purchase_orders"),
            F.when(F.col("supplied_product_count").isNotNull(), F.lit(True)).otherwise(F.lit(False)).alias("has_product_vendor_relationships"),
            F.lit("No standalone vendor master exists in bronze or silver; this dimension is limited to vendor identifiers and derived purchasing relationship metrics.").alias("vendor_attribute_limitation"),
            F.greatest(F.col("purchase_source_modified_at"), F.col("product_vendor_source_modified_at")).alias("latest_source_modified_at")
        )
    )
