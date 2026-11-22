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
    name="silver_geography",
    comment="Conformed Adventure Works address and geography reference data for downstream dimensions and facts.",
)
@dp.expect_or_drop("valid_address_id", "address_id IS NOT NULL")
@dp.expect("geography_city_present", "city IS NOT NULL")
def silver_geography():
    address = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.address").select(
            F.col("AddressID").cast("int").alias("address_id"),
            cleaned_string("AddressLine1").alias("address_line_1"),
            cleaned_string("AddressLine2").alias("address_line_2"),
            cleaned_string("City").alias("city"),
            F.col("StateProvinceID").cast("int").alias("state_province_id"),
            cleaned_string("PostalCode").alias("postal_code"),
            standardize_timestamp("ModifiedDate").alias("address_modified_at"),
        ),
        ["address_id"],
        ["address_modified_at"],
    )

    state_province = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.stateprovince").select(
            F.col("StateProvinceID").cast("int").alias("state_province_id"),
            cleaned_string("StateProvinceCode").alias("state_province_code"),
            cleaned_string("CountryRegionCode").alias("country_region_code"),
            F.col("IsOnlyStateProvinceFlag").cast("boolean").alias("is_only_state_province"),
            cleaned_string("Name").alias("state_province_name"),
            F.col("TerritoryID").cast("int").alias("territory_id"),
            standardize_timestamp("ModifiedDate").alias("state_province_modified_at"),
        ),
        ["state_province_id"],
        ["state_province_modified_at"],
    )

    country_region = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.countryregion").select(
            cleaned_string("CountryRegionCode").alias("country_region_code"),
            cleaned_string("Name").alias("country_region_name"),
            standardize_timestamp("ModifiedDate").alias("country_region_modified_at"),
        ),
        ["country_region_code"],
        ["country_region_modified_at"],
    )

    geography = (
        address.alias("address")
        .join(state_province.alias("state_province"), "state_province_id", "left")
        .join(country_region.alias("country_region"), "country_region_code", "left")
        .select(
            F.col("address_id"),
            F.col("address_line_1"),
            F.col("address_line_2"),
            F.col("city"),
            F.col("state_province_id"),
            F.col("state_province_code"),
            F.col("state_province_name"),
            F.col("country_region_code"),
            F.col("country_region_name"),
            F.col("territory_id"),
            F.col("postal_code"),
            F.col("is_only_state_province"),
            F.greatest(
                F.col("address_modified_at"),
                F.col("state_province_modified_at"),
                F.col("country_region_modified_at"),
            ).alias("source_modified_at"),
        )
    )

    return add_audit_metadata(geography, "address")
