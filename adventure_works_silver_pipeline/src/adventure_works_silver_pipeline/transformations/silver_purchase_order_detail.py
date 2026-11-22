from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.silver_utils import (
    BRONZE_PREFIX,
    add_audit_metadata,
    deduplicate_latest,
    standardize_business_date,
    standardize_timestamp,
)


@dp.materialized_view(
    name="silver_purchase_order_detail",
    comment="Conformed Adventure Works purchase order line data with standardized receipt and pricing measures.",
)
@dp.expect_or_drop("valid_purchase_order_detail_key", "purchase_order_id IS NOT NULL AND purchase_order_detail_id IS NOT NULL")
@dp.expect_or_drop("valid_purchase_order_product_id", "product_id IS NOT NULL")
@dp.expect("purchase_order_detail_positive_qty", "order_qty IS NULL OR order_qty > 0")
@dp.expect("purchase_order_detail_amounts_non_negative", "line_total IS NULL OR line_total >= 0")
def silver_purchase_order_detail():
    purchase_order_detail = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.purchaseorderdetail").select(
            F.col("PurchaseOrderID").cast("int").alias("purchase_order_id"),
            F.col("PurchaseOrderDetailID").cast("int").alias("purchase_order_detail_id"),
            standardize_business_date("DueDate").alias("due_date"),
            F.col("OrderQty").cast("int").alias("order_qty"),
            F.col("ProductID").cast("int").alias("product_id"),
            F.col("UnitPrice").cast("decimal(19,4)").alias("unit_price"),
            F.col("LineTotal").cast("decimal(19,4)").alias("line_total"),
            F.col("ReceivedQty").cast("decimal(8,2)").alias("received_qty"),
            F.col("RejectedQty").cast("decimal(8,2)").alias("rejected_qty"),
            F.col("StockedQty").cast("decimal(9,2)").alias("stocked_qty"),
            standardize_timestamp("ModifiedDate").alias("source_modified_at"),
        ),
        ["purchase_order_id", "purchase_order_detail_id"],
        ["source_modified_at"],
    )

    return add_audit_metadata(purchase_order_detail, "purchaseorderdetail")
