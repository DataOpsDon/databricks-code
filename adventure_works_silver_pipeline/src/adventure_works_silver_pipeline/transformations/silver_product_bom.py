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
    name="silver_product_bom",
    comment="Conformed Adventure Works bill of materials bridge data with assembly and component product context.",
)
@dp.expect_or_drop("valid_bom_id", "bill_of_materials_id IS NOT NULL")
@dp.expect_or_drop("valid_bom_component_id", "component_id IS NOT NULL")
@dp.expect("bom_per_assembly_qty_positive", "per_assembly_qty IS NULL OR per_assembly_qty > 0")
@dp.expect("bom_end_after_start", "end_date IS NULL OR end_date >= start_date")
def silver_product_bom():
    bill_of_materials = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.billofmaterials").select(
            F.col("BillOfMaterialsID").cast("int").alias("bill_of_materials_id"),
            F.col("ProductAssemblyID").cast("int").alias("product_assembly_id"),
            F.col("ComponentID").cast("int").alias("component_id"),
            standardize_business_date("StartDate").alias("start_date"),
            standardize_business_date("EndDate").alias("end_date"),
            cleaned_string("UnitMeasureCode").alias("unit_measure_code"),
            F.col("BOMLevel").cast("int").alias("bom_level"),
            F.col("PerAssemblyQty").cast("decimal(8,2)").alias("per_assembly_qty"),
            standardize_timestamp("ModifiedDate").alias("bom_modified_at"),
        ),
        ["bill_of_materials_id"],
        ["bom_modified_at"],
    )

    product = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.product").select(
            F.col("ProductID").cast("int").alias("product_id"),
            cleaned_string("Name").alias("product_name"),
            standardize_timestamp("ModifiedDate").alias("product_modified_at"),
        ),
        ["product_id"],
        ["product_modified_at"],
    )

    result = (
        bill_of_materials.alias("bom")
        .join(
            product.alias("assembly_product"),
            F.col("bom.product_assembly_id") == F.col("assembly_product.product_id"),
            "left",
        )
        .join(
            product.alias("component_product"),
            F.col("bom.component_id") == F.col("component_product.product_id"),
            "left",
        )
        .select(
            F.col("bom.bill_of_materials_id"),
            F.col("bom.product_assembly_id"),
            F.col("assembly_product.product_name").alias("assembly_product_name"),
            F.col("bom.component_id"),
            F.col("component_product.product_name").alias("component_product_name"),
            F.col("bom.start_date"),
            F.col("bom.end_date"),
            F.col("bom.unit_measure_code"),
            F.col("bom.bom_level"),
            F.col("bom.per_assembly_qty"),
            F.greatest(
                F.col("bom.bom_modified_at"),
                F.col("assembly_product.product_modified_at"),
                F.col("component_product.product_modified_at"),
            ).alias("source_modified_at"),
        )
    )

    return add_audit_metadata(result, "billofmaterials")
