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
    name="silver_salesperson",
    comment="Conformed Adventure Works salesperson entity enriched with person attributes.",
)
@dp.expect_or_drop("valid_salesperson_id", "salesperson_id IS NOT NULL")
@dp.expect("salesperson_sales_ytd_non_negative", "sales_ytd IS NULL OR sales_ytd >= 0")
def silver_salesperson():
    salesperson = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.salesperson").select(
            F.col("BusinessEntityID").cast("int").alias("salesperson_id"),
            F.col("TerritoryID").cast("int").alias("territory_id"),
            F.col("SalesQuota").cast("decimal(19,4)").alias("sales_quota"),
            F.col("Bonus").cast("decimal(19,4)").alias("bonus"),
            F.col("CommissionPct").cast("decimal(10,4)").alias("commission_pct"),
            F.col("SalesYTD").cast("decimal(19,4)").alias("sales_ytd"),
            F.col("SalesLastYear").cast("decimal(19,4)").alias("sales_last_year"),
            standardize_timestamp("ModifiedDate").alias("salesperson_modified_at"),
        ),
        ["salesperson_id"],
        ["salesperson_modified_at"],
    )

    person = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.person").select(
            F.col("BusinessEntityID").cast("int").alias("salesperson_id"),
            cleaned_string("PersonType").alias("person_type"),
            cleaned_string("Title").alias("title"),
            cleaned_string("FirstName").alias("first_name"),
            cleaned_string("MiddleName").alias("middle_name"),
            cleaned_string("LastName").alias("last_name"),
            cleaned_string("Suffix").alias("suffix"),
            F.col("EmailPromotion").cast("int").alias("email_promotion"),
            standardize_timestamp("ModifiedDate").alias("person_modified_at"),
        ),
        ["salesperson_id"],
        ["person_modified_at"],
    )

    result = (
        salesperson.alias("salesperson")
        .join(person.alias("person"), "salesperson_id", "left")
        .select(
            F.col("salesperson_id"),
            F.col("territory_id"),
            F.col("person_type"),
            F.col("title"),
            F.col("first_name"),
            F.col("middle_name"),
            F.col("last_name"),
            F.col("suffix"),
            F.concat_ws(" ", F.col("first_name"), F.col("middle_name"), F.col("last_name")).alias("full_name"),
            F.col("email_promotion"),
            F.col("sales_quota"),
            F.col("bonus"),
            F.col("commission_pct"),
            F.col("sales_ytd"),
            F.col("sales_last_year"),
            F.greatest(F.col("salesperson_modified_at"), F.col("person_modified_at")).alias("source_modified_at"),
        )
    )

    return add_audit_metadata(result, "salesperson")
