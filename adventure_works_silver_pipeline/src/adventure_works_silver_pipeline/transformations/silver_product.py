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
    name="silver_product",
    comment="Conformed Adventure Works product entity enriched with product hierarchy attributes.",
)
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect("product_list_price_non_negative", "list_price IS NULL OR list_price >= 0")
@dp.expect("product_standard_cost_non_negative", "standard_cost IS NULL OR standard_cost >= 0")
def silver_product():
    product = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.product").select(
            F.col("ProductID").cast("int").alias("product_id"),
            cleaned_string("Name").alias("product_name"),
            cleaned_string("ProductNumber").alias("product_number"),
            F.col("MakeFlag").cast("boolean").alias("make_flag"),
            F.col("FinishedGoodsFlag").cast("boolean").alias("finished_goods_flag"),
            cleaned_string("Color").alias("color"),
            F.col("SafetyStockLevel").cast("int").alias("safety_stock_level"),
            F.col("ReorderPoint").cast("int").alias("reorder_point"),
            F.col("StandardCost").cast("decimal(19,4)").alias("standard_cost"),
            F.col("ListPrice").cast("decimal(19,4)").alias("list_price"),
            cleaned_string("Size").alias("size"),
            cleaned_string("SizeUnitMeasureCode").alias("size_unit_measure_code"),
            cleaned_string("WeightUnitMeasureCode").alias("weight_unit_measure_code"),
            F.col("Weight").cast("decimal(8,2)").alias("weight"),
            F.col("DaysToManufacture").cast("int").alias("days_to_manufacture"),
            cleaned_string("ProductLine").alias("product_line"),
            cleaned_string("Class").alias("product_class"),
            cleaned_string("Style").alias("style"),
            F.col("ProductSubcategoryID").cast("int").alias("product_subcategory_id"),
            F.col("ProductModelID").cast("int").alias("product_model_id"),
            standardize_business_date("SellStartDate").alias("sell_start_date"),
            standardize_business_date("SellEndDate").alias("sell_end_date"),
            standardize_business_date("DiscontinuedDate").alias("discontinued_date"),
            standardize_timestamp("ModifiedDate").alias("product_modified_at"),
        ),
        ["product_id"],
        ["product_modified_at"],
    )

    subcategory = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.productsubcategory").select(
            F.col("ProductSubcategoryID").cast("int").alias("product_subcategory_id"),
            F.col("ProductCategoryID").cast("int").alias("product_category_id"),
            cleaned_string("Name").alias("product_subcategory_name"),
            standardize_timestamp("ModifiedDate").alias("subcategory_modified_at"),
        ),
        ["product_subcategory_id"],
        ["subcategory_modified_at"],
    )

    category = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.productcategory").select(
            F.col("ProductCategoryID").cast("int").alias("product_category_id"),
            cleaned_string("Name").alias("product_category_name"),
            standardize_timestamp("ModifiedDate").alias("category_modified_at"),
        ),
        ["product_category_id"],
        ["category_modified_at"],
    )

    result = (
        product.alias("product")
        .join(subcategory.alias("subcategory"), "product_subcategory_id", "left")
        .join(category.alias("category"), "product_category_id", "left")
        .select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("product_number"),
            F.col("make_flag"),
            F.col("finished_goods_flag"),
            F.col("color"),
            F.col("safety_stock_level"),
            F.col("reorder_point"),
            F.col("standard_cost"),
            F.col("list_price"),
            F.col("size"),
            F.col("size_unit_measure_code"),
            F.col("weight_unit_measure_code"),
            F.col("weight"),
            F.col("days_to_manufacture"),
            F.col("product_line"),
            F.col("product_class"),
            F.col("style"),
            F.col("product_subcategory_id"),
            F.col("product_subcategory_name"),
            F.col("product_category_id"),
            F.col("product_category_name"),
            F.col("product_model_id"),
            F.col("sell_start_date"),
            F.col("sell_end_date"),
            F.col("discontinued_date"),
            F.greatest(
                F.col("product_modified_at"),
                F.col("subcategory_modified_at"),
                F.col("category_modified_at"),
            ).alias("source_modified_at"),
        )
    )

    return add_audit_metadata(result, "product")
