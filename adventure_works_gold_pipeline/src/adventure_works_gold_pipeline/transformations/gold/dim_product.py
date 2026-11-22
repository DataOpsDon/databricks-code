from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dim_product",
    comment="Adventure Works product dimension enriched with hierarchy and merchandising attributes from the silver layer."
)
@dp.expect_or_drop("valid_product_key", "product_key IS NOT NULL")
def dim_product():
    product = spark.read.table("edp_silver_dev.default.silver_product")

    return product.select(
        F.col("product_id").alias("product_key"),
        F.col("product_id"),
        F.col("product_name"),
        F.col("product_number"),
        F.col("product_subcategory_id"),
        F.col("product_subcategory_name"),
        F.col("product_category_id"),
        F.col("product_category_name"),
        F.col("product_model_id"),
        F.col("make_flag"),
        F.col("finished_goods_flag"),
        F.when(F.col("finished_goods_flag"), F.lit("Finished Good")).otherwise(F.lit("Component or Raw Material")).alias("product_role"),
        F.col("color"),
        F.col("size"),
        F.col("size_unit_measure_code"),
        F.col("weight_unit_measure_code"),
        F.col("weight"),
        F.col("days_to_manufacture"),
        F.col("product_line"),
        F.col("product_class"),
        F.col("style"),
        F.col("standard_cost"),
        F.col("list_price"),
        F.col("reorder_point"),
        F.col("safety_stock_level"),
        F.col("sell_start_date"),
        F.col("sell_end_date"),
        F.col("discontinued_date"),
        F.when(F.col("discontinued_date").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_current_product"),
        F.col("source_modified_at").alias("latest_source_modified_at")
    )
