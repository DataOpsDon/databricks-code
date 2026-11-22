from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dim_salesperson",
    comment="Adventure Works salesperson dimension enriched with territory and performance attributes from the silver layer."
)
@dp.expect_or_drop("valid_salesperson_key", "salesperson_key IS NOT NULL")
def dim_salesperson():
    salesperson = spark.read.table("edp_silver_dev.default.silver_salesperson")

    return salesperson.select(
        F.col("salesperson_id").alias("salesperson_key"),
        F.col("salesperson_id"),
        F.col("territory_id").alias("territory_key"),
        F.col("full_name").alias("salesperson_name"),
        F.col("person_type"),
        F.col("title"),
        F.col("first_name"),
        F.col("middle_name"),
        F.col("last_name"),
        F.col("suffix"),
        F.col("email_promotion"),
        F.col("sales_quota"),
        F.col("bonus"),
        F.col("commission_pct"),
        F.col("sales_ytd"),
        F.col("sales_last_year"),
        F.col("source_modified_at").alias("latest_source_modified_at")
    )
