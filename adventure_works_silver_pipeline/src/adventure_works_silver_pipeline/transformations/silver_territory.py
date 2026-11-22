from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.silver_utils import (
    BRONZE_PREFIX,
    add_audit_metadata,
    cleaned_string,
    deduplicate_latest,
    standardize_timestamp,
)


@dp.materialized_view(
    name="silver_territory",
    comment="Conformed Adventure Works sales territory reference data.",
)
@dp.expect_or_drop("valid_territory_id", "territory_id IS NOT NULL")
@dp.expect("territory_sales_ytd_non_negative", "sales_ytd IS NULL OR sales_ytd >= 0")
def silver_territory():
    territory = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.salesterritory").select(
            F.col("TerritoryID").cast("int").alias("territory_id"),
            cleaned_string("Name").alias("territory_name"),
            cleaned_string("CountryRegionCode").alias("country_region_code"),
            cleaned_string("Group").alias("territory_group"),
            F.col("SalesYTD").cast("decimal(19,4)").alias("sales_ytd"),
            F.col("SalesLastYear").cast("decimal(19,4)").alias("sales_last_year"),
            F.col("CostYTD").cast("decimal(19,4)").alias("cost_ytd"),
            F.col("CostLastYear").cast("decimal(19,4)").alias("cost_last_year"),
            standardize_timestamp("ModifiedDate").alias("source_modified_at"),
        ),
        ["territory_id"],
        ["source_modified_at"],
    )

    return add_audit_metadata(territory, "salesterritory")
