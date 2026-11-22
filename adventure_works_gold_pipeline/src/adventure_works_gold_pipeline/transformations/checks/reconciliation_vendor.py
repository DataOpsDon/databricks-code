from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="reconciliation_vendor",
    comment="Identifier-based bronze-to-silver-to-gold reconciliation checks for Adventure Works vendor coverage because the source has no standalone vendor master."
)
@dp.expect("vendor_identifier_counts_match", "bronze_vendor_count = silver_vendor_count AND silver_vendor_count = gold_vendor_count")
def reconciliation_vendor():
    bronze_purchase = spark.read.table("edp_bronze_dev.adventure_works.purchaseorderheader").select(F.col("VendorID").alias("vendor_id"))
    bronze_product_vendor = spark.read.table("edp_bronze_dev.adventure_works.productvendor").select(F.col("BusinessEntityID").alias("vendor_id"))
    silver_purchase = spark.read.table("edp_silver_dev.default.silver_purchase_order_header").select("vendor_id")
    silver_product_vendor = spark.read.table("edp_silver_dev.default.silver_product_vendor").select("vendor_id")
    gold_vendor = spark.read.table("dim_vendor").select("vendor_id")

    bronze_agg = (
        bronze_purchase.unionByName(bronze_product_vendor)
        .where(F.col("vendor_id").isNotNull())
        .distinct()
        .agg(F.count("*").alias("bronze_vendor_count"))
    )
    silver_agg = (
        silver_purchase.unionByName(silver_product_vendor)
        .where(F.col("vendor_id").isNotNull())
        .distinct()
        .agg(F.count("*").alias("silver_vendor_count"))
    )
    gold_agg = (
        gold_vendor.where(F.col("vendor_id").isNotNull())
        .distinct()
        .agg(F.count("*").alias("gold_vendor_count"))
    )

    return (
        bronze_agg.crossJoin(silver_agg).crossJoin(gold_agg)
        .select(
            F.col("bronze_vendor_count"),
            F.col("silver_vendor_count"),
            F.col("gold_vendor_count"),
            (F.col("silver_vendor_count") - F.col("bronze_vendor_count")).alias("bronze_to_silver_vendor_diff"),
            (F.col("gold_vendor_count") - F.col("silver_vendor_count")).alias("silver_to_gold_vendor_diff"),
            F.lit("Vendor reconciliation is identifier-based only because no standalone vendor master attributes exist in the bronze source.").alias("reconciliation_scope_note")
        )
    )
