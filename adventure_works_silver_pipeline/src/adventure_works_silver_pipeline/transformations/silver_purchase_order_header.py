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
    name="silver_purchase_order_header",
    comment="Conformed Adventure Works purchase order header data with standardized dates, amounts, and status semantics.",
)
@dp.expect_or_drop("valid_purchase_order_id", "purchase_order_id IS NOT NULL")
@dp.expect_or_drop("valid_purchase_order_vendor_id", "vendor_id IS NOT NULL")
@dp.expect("purchase_order_total_due_non_negative", "total_due IS NULL OR total_due >= 0")
@dp.expect("purchase_order_ship_after_order", "ship_date IS NULL OR ship_date >= order_date")
def silver_purchase_order_header():
    purchase_order_header = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.purchaseorderheader").select(
            F.col("PurchaseOrderID").cast("int").alias("purchase_order_id"),
            F.col("RevisionNumber").cast("int").alias("revision_number"),
            F.col("Status").cast("int").alias("status_code"),
            F.when(F.col("Status") == 1, F.lit("PENDING"))
            .when(F.col("Status") == 2, F.lit("APPROVED"))
            .when(F.col("Status") == 3, F.lit("REJECTED"))
            .when(F.col("Status") == 4, F.lit("COMPLETE"))
            .otherwise(F.lit("UNKNOWN"))
            .alias("status_name"),
            F.col("EmployeeID").cast("int").alias("employee_id"),
            F.col("VendorID").cast("int").alias("vendor_id"),
            F.col("ShipMethodID").cast("int").alias("ship_method_id"),
            standardize_business_date("OrderDate").alias("order_date"),
            standardize_business_date("ShipDate").alias("ship_date"),
            F.col("SubTotal").cast("decimal(19,4)").alias("subtotal"),
            F.col("TaxAmt").cast("decimal(19,4)").alias("tax_amount"),
            F.col("Freight").cast("decimal(19,4)").alias("freight_amount"),
            F.col("TotalDue").cast("decimal(19,4)").alias("total_due"),
            standardize_timestamp("ModifiedDate").alias("source_modified_at"),
        ),
        ["purchase_order_id"],
        ["source_modified_at"],
    )

    return add_audit_metadata(purchase_order_header, "purchaseorderheader")
