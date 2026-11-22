from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dim_date",
    comment="Calendar date dimension generated from the full range of dates present in the Adventure Works silver layer."
)
@dp.expect_or_drop("valid_date_key", "date_key IS NOT NULL")
def dim_date():
    date_frames = [
        spark.read.table("edp_silver_dev.default.silver_sales_order_header").select(F.col("order_date").alias("calendar_date")),
        spark.read.table("edp_silver_dev.default.silver_sales_order_header").select(F.col("due_date").alias("calendar_date")),
        spark.read.table("edp_silver_dev.default.silver_sales_order_header").select(F.col("ship_date").alias("calendar_date")),
        spark.read.table("edp_silver_dev.default.silver_purchase_order_header").select(F.col("order_date").alias("calendar_date")),
        spark.read.table("edp_silver_dev.default.silver_purchase_order_header").select(F.col("ship_date").alias("calendar_date")),
        spark.read.table("edp_silver_dev.default.silver_purchase_order_detail").select(F.col("due_date").alias("calendar_date")),
        spark.read.table("edp_silver_dev.default.silver_work_order").select(F.col("start_date").alias("calendar_date")),
        spark.read.table("edp_silver_dev.default.silver_work_order").select(F.col("end_date").alias("calendar_date")),
        spark.read.table("edp_silver_dev.default.silver_work_order").select(F.col("due_date").alias("calendar_date")),
        spark.read.table("edp_silver_dev.default.silver_product").select(F.col("sell_start_date").alias("calendar_date")),
        spark.read.table("edp_silver_dev.default.silver_product").select(F.col("sell_end_date").alias("calendar_date")),
        spark.read.table("edp_silver_dev.default.silver_product").select(F.col("discontinued_date").alias("calendar_date")),
        spark.read.table("edp_silver_dev.default.silver_product_bom").select(F.col("start_date").alias("calendar_date")),
        spark.read.table("edp_silver_dev.default.silver_product_bom").select(F.col("end_date").alias("calendar_date")),
        spark.read.table("edp_silver_dev.default.silver_product_vendor").select(F.col("last_receipt_date").alias("calendar_date")),
    ]

    all_dates = reduce(lambda left, right: left.unionByName(right), date_frames).where(F.col("calendar_date").isNotNull())
    bounds = all_dates.agg(
        F.min("calendar_date").alias("min_date"),
        F.max("calendar_date").alias("max_date")
    )

    return (
        bounds.select(F.explode(F.sequence("min_date", "max_date")).alias("calendar_date"))
        .select(
            F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("calendar_date"),
            F.year("calendar_date").alias("calendar_year"),
            F.quarter("calendar_date").alias("calendar_quarter"),
            F.month("calendar_date").alias("calendar_month"),
            F.date_format("calendar_date", "MMMM").alias("month_name"),
            F.weekofyear("calendar_date").alias("week_of_year"),
            F.dayofmonth("calendar_date").alias("day_of_month"),
            F.dayofyear("calendar_date").alias("day_of_year"),
            F.date_format("calendar_date", "EEEE").alias("day_name"),
            F.dayofweek("calendar_date").alias("day_of_week"),
            F.when(F.dayofweek("calendar_date").isin([1, 7]), F.lit(True)).otherwise(F.lit(False)).alias("is_weekend"),
            F.trunc("calendar_date", "month").alias("month_start_date"),
            F.last_day("calendar_date").alias("month_end_date")
        )
    )
