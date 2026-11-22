from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.silver_utils import (
    BRONZE_PREFIX,
    add_audit_metadata,
    cleaned_string,
    deduplicate_latest,
    standardize_business_date,
    standardize_timestamp,
)


@dp.materialized_view(
    name="silver_product_vendor",
    comment="Conformed Adventure Works product-to-vendor purchasing relationship data derived from available vendor identifiers.",
)
@dp.expect_or_drop("valid_product_vendor_key", "product_id IS NOT NULL AND vendor_id IS NOT NULL")
@dp.expect("product_vendor_standard_price_non_negative", "standard_price IS NULL OR standard_price >= 0")
@dp.expect("product_vendor_last_receipt_cost_non_negative", "last_receipt_cost IS NULL OR last_receipt_cost >= 0")
def silver_product_vendor():
    product_vendor = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.productvendor").select(
            F.col("ProductID").cast("int").alias("product_id"),
            F.col("BusinessEntityID").cast("int").alias("vendor_id"),
            F.col("AverageLeadTime").cast("int").alias("average_lead_time_days"),
            F.col("StandardPrice").cast("decimal(19,4)").alias("standard_price"),
            F.col("LastReceiptCost").cast("decimal(19,4)").alias("last_receipt_cost"),
            standardize_business_date("LastReceiptDate").alias("last_receipt_date"),
            F.col("MinOrderQty").cast("int").alias("min_order_qty"),
            F.col("MaxOrderQty").cast("int").alias("max_order_qty"),
            F.col("OnOrderQty").cast("int").alias("on_order_qty"),
            cleaned_string("UnitMeasureCode").alias("unit_measure_code"),
            standardize_timestamp("ModifiedDate").alias("source_modified_at"),
        ),
        ["product_id", "vendor_id"],
        ["source_modified_at"],
    )

    return add_audit_metadata(product_vendor, "productvendor")
