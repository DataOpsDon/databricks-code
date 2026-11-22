from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dim_customer",
    comment="Adventure Works customer dimension for individual and store customers from the silver layer."
)
@dp.expect_or_drop("valid_customer_key", "customer_key IS NOT NULL")
def dim_customer():
    customer = spark.read.table("edp_silver_dev.default.silver_customer")

    return customer.select(
        F.col("customer_id").alias("customer_key"),
        F.col("customer_id"),
        F.col("customer_type"),
        F.col("territory_id").alias("territory_key"),
        F.col("store_salesperson_id").alias("salesperson_key"),
        F.col("person_id"),
        F.col("store_id"),
        F.col("account_number"),
        F.col("customer_name"),
        F.col("person_full_name"),
        F.col("store_name"),
        F.col("person_type"),
        F.col("title"),
        F.col("first_name"),
        F.col("middle_name"),
        F.col("last_name"),
        F.col("suffix"),
        F.col("email_promotion"),
        F.when(F.col("store_id").isNotNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_store_customer"),
        F.col("source_modified_at").alias("latest_source_modified_at")
    )
