from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dim_geography",
    comment="Adventure Works geography dimension for address, state, country, and territory attributes from the silver layer."
)
@dp.expect_or_drop("valid_geography_key", "geography_key IS NOT NULL")
def dim_geography():
    geography = spark.read.table("edp_silver_dev.default.silver_geography")

    return geography.select(
        F.col("address_id").alias("geography_key"),
        F.col("address_id"),
        F.col("territory_id").alias("territory_key"),
        F.col("address_line_1"),
        F.col("address_line_2"),
        F.col("city"),
        F.col("state_province_id"),
        F.col("state_province_code"),
        F.col("state_province_name"),
        F.col("country_region_code"),
        F.col("country_region_name"),
        F.col("postal_code"),
        F.col("is_only_state_province"),
        F.concat_ws(", ", F.col("city"), F.col("state_province_name"), F.col("country_region_name")).alias("geography_label"),
        F.col("source_modified_at").alias("latest_source_modified_at")
    )
