from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dim_territory",
    comment="Adventure Works sales territory dimension derived from the silver layer."
)
@dp.expect_or_drop("valid_territory_key", "territory_key IS NOT NULL")
def dim_territory():
    territory = spark.read.table("edp_silver_dev.default.silver_territory")

    return territory.select(
        F.col("territory_id").alias("territory_key"),
        F.col("territory_id"),
        F.col("territory_name"),
        F.col("country_region_code"),
        F.col("territory_group"),
        F.col("sales_ytd"),
        F.col("sales_last_year"),
        F.col("cost_ytd"),
        F.col("cost_last_year"),
        F.col("source_modified_at").alias("latest_source_modified_at")
    )
