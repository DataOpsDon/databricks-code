from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="bridge_product_bom",
    comment="Adventure Works product bill of materials bridge at the assembly-component-effective-date grain."
)
@dp.expect_or_drop("valid_bom_relationship", "assembly_product_key IS NOT NULL AND component_product_key IS NOT NULL")
def bridge_product_bom():
    product_bom = spark.read.table("edp_silver_dev.default.silver_product_bom")

    return product_bom.select(
        F.sha2(
            F.concat_ws(
                "||",
                F.col("bill_of_materials_id").cast("string"),
                F.col("product_assembly_id").cast("string"),
                F.col("component_id").cast("string"),
                F.coalesce(F.col("start_date").cast("string"), F.lit("null"))
            ),
            256
        ).alias("product_bom_bridge_key"),
        F.col("bill_of_materials_id"),
        F.col("product_assembly_id").alias("assembly_product_key"),
        F.col("component_id").alias("component_product_key"),
        F.date_format(F.col("start_date"), "yyyyMMdd").cast("int").alias("start_date_key"),
        F.date_format(F.col("end_date"), "yyyyMMdd").cast("int").alias("end_date_key"),
        F.col("assembly_product_name"),
        F.col("component_product_name"),
        F.col("bom_level"),
        F.col("per_assembly_qty"),
        F.col("unit_measure_code"),
        F.col("start_date"),
        F.col("end_date"),
        F.col("source_modified_at").alias("latest_source_modified_at")
    )
