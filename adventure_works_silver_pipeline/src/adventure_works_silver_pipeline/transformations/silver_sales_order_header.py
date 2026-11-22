from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.silver_utils import (
    BRONZE_PREFIX,
    add_audit_metadata,
    cleaned_string,
    deduplicate_latest,
    standardize_business_date,
    standardize_timestamp,
)


@dp.materialized_view(
    name="silver_sales_order_header",
    comment="Conformed Adventure Works sales order header data with standardized dates, amounts, and status semantics.",
)
@dp.expect_or_drop("valid_sales_order_id", "sales_order_id IS NOT NULL")
@dp.expect_or_drop("valid_sales_order_customer_id", "customer_id IS NOT NULL")
@dp.expect("sales_order_total_due_non_negative", "total_due IS NULL OR total_due >= 0")
@dp.expect("sales_order_ship_after_order", "ship_date IS NULL OR ship_date >= order_date")
def silver_sales_order_header():
    sales_order_header = deduplicate_latest(
        spark.read.table(f"{BRONZE_PREFIX}.salesorderheader").select(
            F.col("SalesOrderID").cast("int").alias("sales_order_id"),
            F.col("RevisionNumber").cast("int").alias("revision_number"),
            standardize_business_date("OrderDate").alias("order_date"),
            standardize_business_date("DueDate").alias("due_date"),
            standardize_business_date("ShipDate").alias("ship_date"),
            F.col("Status").cast("int").alias("status_code"),
            F.when(F.col("Status") == 1, F.lit("IN_PROCESS"))
            .when(F.col("Status") == 2, F.lit("APPROVED"))
            .when(F.col("Status") == 3, F.lit("BACKORDERED"))
            .when(F.col("Status") == 4, F.lit("REJECTED"))
            .when(F.col("Status") == 5, F.lit("SHIPPED"))
            .when(F.col("Status") == 6, F.lit("CANCELLED"))
            .otherwise(F.lit("UNKNOWN"))
            .alias("status_name"),
            F.col("OnlineOrderFlag").cast("boolean").alias("online_order_flag"),
            cleaned_string("SalesOrderNumber").alias("sales_order_number"),
            cleaned_string("PurchaseOrderNumber").alias("purchase_order_number"),
            cleaned_string("AccountNumber").alias("account_number"),
            F.col("CustomerID").cast("int").alias("customer_id"),
            F.col("SalesPersonID").cast("int").alias("salesperson_id"),
            F.col("TerritoryID").cast("int").alias("territory_id"),
            F.col("BillToAddressID").cast("int").alias("bill_to_address_id"),
            F.col("ShipToAddressID").cast("int").alias("ship_to_address_id"),
            F.col("ShipMethodID").cast("int").alias("ship_method_id"),
            F.col("CreditCardID").cast("int").alias("credit_card_id"),
            cleaned_string("CreditCardApprovalCode").alias("credit_card_approval_code"),
            F.col("CurrencyRateID").cast("int").alias("currency_rate_id"),
            F.col("SubTotal").cast("decimal(19,4)").alias("subtotal"),
            F.col("TaxAmt").cast("decimal(19,4)").alias("tax_amount"),
            F.col("Freight").cast("decimal(19,4)").alias("freight_amount"),
            F.col("TotalDue").cast("decimal(19,4)").alias("total_due"),
            cleaned_string("Comment").alias("comment"),
            standardize_timestamp("ModifiedDate").alias("source_modified_at"),
        ),
        ["sales_order_id"],
        ["source_modified_at"],
    )

    return add_audit_metadata(sales_order_header, "salesorderheader")
