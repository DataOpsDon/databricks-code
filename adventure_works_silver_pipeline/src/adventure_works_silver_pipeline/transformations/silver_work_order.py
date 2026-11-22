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
    name="silver_work_order",
    comment="Conformed Adventure Works work order data for manufacturing and production analytics.",
)
@dp.expect_or_drop("valid_work_order_id", "work_order_id IS NOT NULL")
@dp.expect_or_drop("valid_work_order_product_id", "product_id IS NOT NULL")
@dp.expect("work_order_positive_qty", "order_qty IS NULL OR order_qty > 0")
@dp.expect("work_order_end_after_start", "end_date IS NULL OR end_date >= start_date")
def silver_work_order():
    work_order = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.workorder").select(
            F.col("WorkOrderID").cast("int").alias("work_order_id"),
            F.col("ProductID").cast("int").alias("product_id"),
            F.col("OrderQty").cast("int").alias("order_qty"),
            F.col("StockedQty").cast("int").alias("stocked_qty"),
            F.col("ScrappedQty").cast("int").alias("scrapped_qty"),
            standardize_business_date("StartDate").alias("start_date"),
            standardize_business_date("EndDate").alias("end_date"),
            standardize_business_date("DueDate").alias("due_date"),
            F.col("ScrapReasonID").cast("int").alias("scrap_reason_id"),
            standardize_timestamp("ModifiedDate").alias("source_modified_at"),
        ),
        ["work_order_id"],
        ["source_modified_at"],
    )

    return add_audit_metadata(work_order, "workorder")
