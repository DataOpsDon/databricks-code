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
    name="silver_customer",
    comment="Conformed Adventure Works customer entity combining individual and store customers.",
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("customer_name_present", "customer_name IS NOT NULL")
def silver_customer():
    customer = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.customer").select(
            F.col("CustomerID").cast("int").alias("customer_id"),
            F.col("PersonID").cast("int").alias("person_id"),
            F.col("StoreID").cast("int").alias("store_id"),
            F.col("TerritoryID").cast("int").alias("territory_id"),
            cleaned_string("AccountNumber").alias("account_number"),
            standardize_timestamp("ModifiedDate").alias("customer_modified_at"),
        ),
        ["customer_id"],
        ["customer_modified_at"],
    )

    person = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.person").select(
            F.col("BusinessEntityID").cast("int").alias("person_id"),
            cleaned_string("PersonType").alias("person_type"),
            cleaned_string("Title").alias("title"),
            cleaned_string("FirstName").alias("first_name"),
            cleaned_string("MiddleName").alias("middle_name"),
            cleaned_string("LastName").alias("last_name"),
            cleaned_string("Suffix").alias("suffix"),
            F.col("EmailPromotion").cast("int").alias("email_promotion"),
            standardize_timestamp("ModifiedDate").alias("person_modified_at"),
        ),
        ["person_id"],
        ["person_modified_at"],
    )

    store = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.store").select(
            F.col("BusinessEntityID").cast("int").alias("store_id"),
            cleaned_string("Name").alias("store_name"),
            F.col("SalesPersonID").cast("int").alias("store_salesperson_id"),
            standardize_timestamp("ModifiedDate").alias("store_modified_at"),
        ),
        ["store_id"],
        ["store_modified_at"],
    )

    result = (
        customer.alias("customer")
        .join(person.alias("person"), "person_id", "left")
        .join(store.alias("store"), "store_id", "left")
        .select(
            F.col("customer_id"),
            F.when(F.col("person_id").isNotNull(), F.lit("INDIVIDUAL"))
            .when(F.col("store_id").isNotNull(), F.lit("STORE"))
            .otherwise(F.lit("UNKNOWN"))
            .alias("customer_type"),
            F.col("person_id"),
            F.col("store_id"),
            F.col("territory_id"),
            F.col("account_number"),
            F.col("person_type"),
            F.col("title"),
            F.col("first_name"),
            F.col("middle_name"),
            F.col("last_name"),
            F.col("suffix"),
            F.concat_ws(" ", F.col("first_name"), F.col("middle_name"), F.col("last_name")).alias("person_full_name"),
            F.col("store_name"),
            F.col("store_salesperson_id"),
            F.col("email_promotion"),
            F.coalesce(
                F.col("store_name"),
                F.concat_ws(" ", F.col("first_name"), F.col("middle_name"), F.col("last_name")),
                F.col("account_number"),
            ).alias("customer_name"),
            F.greatest(
                F.col("customer_modified_at"),
                F.col("person_modified_at"),
                F.col("store_modified_at"),
            ).alias("source_modified_at"),
        )
    )

    return add_audit_metadata(result, "customer")
