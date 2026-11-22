from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.silver_utils import (
    BRONZE_PREFIX,
    add_audit_metadata,
    cleaned_string,
    deduplicate_latest,
    standardize_timestamp,
)


@dp.materialized_view(
    name="silver_sales_order_detail",
    comment="Conformed Adventure Works sales order line data with standardized measures and pricing attributes.",
)
@dp.expect_or_drop("valid_sales_order_detail_key", "sales_order_id IS NOT NULL AND sales_order_detail_id IS NOT NULL")
@dp.expect_or_drop("valid_sales_order_detail_product_id", "product_id IS NOT NULL")
@dp.expect("sales_order_detail_positive_qty", "order_qty IS NULL OR order_qty > 0")
@dp.expect("sales_order_detail_line_total_non_negative", "line_total IS NULL OR line_total >= 0")
def silver_sales_order_detail():
    sales_order_detail = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.salesorderdetail").select(
            F.col("SalesOrderID").cast("int").alias("sales_order_id"),
            F.col("SalesOrderDetailID").cast("int").alias("sales_order_detail_id"),
            cleaned_string("CarrierTrackingNumber").alias("carrier_tracking_number"),
            F.col("OrderQty").cast("int").alias("order_qty"),
            F.col("ProductID").cast("int").alias("product_id"),
            F.col("SpecialOfferID").cast("int").alias("special_offer_id"),
            F.col("UnitPrice").cast("decimal(19,4)").alias("unit_price"),
            F.col("UnitPriceDiscount").cast("decimal(19,4)").alias("unit_price_discount"),
            F.col("LineTotal").cast("decimal(38,6)").alias("line_total"),
            standardize_timestamp("ModifiedDate").alias("source_modified_at"),
        ),
        ["sales_order_id", "sales_order_detail_id"],
        ["source_modified_at"],
    )

    return add_audit_metadata(sales_order_detail, "salesorderdetail")
