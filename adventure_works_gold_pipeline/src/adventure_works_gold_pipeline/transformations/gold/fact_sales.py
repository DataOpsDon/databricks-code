from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="fact_sales",
    comment="Adventure Works sales fact table at the sales order line grain."
)
@dp.expect_or_drop("valid_sales_line", "sales_order_id IS NOT NULL AND sales_order_detail_id IS NOT NULL")
@dp.expect("positive_order_quantity", "order_qty > 0")
@dp.expect("non_negative_sales_amount", "net_sales_amount >= 0")
def fact_sales():
    sales_header = spark.read.table("edp_silver_dev.default.silver_sales_order_header").alias("h")
    sales_detail = spark.read.table("edp_silver_dev.default.silver_sales_order_detail").alias("d")

    gross_sales_amount = F.col("d.unit_price").cast("decimal(38,6)") * F.col("d.order_qty").cast("decimal(38,6)")
    net_sales_amount = F.col("d.line_total").cast("decimal(38,6)")
    discount_amount = gross_sales_amount - net_sales_amount

    return (
        sales_detail.join(sales_header, "sales_order_id", "inner")
        .select(
            F.sha2(F.concat_ws("||", F.col("sales_order_id").cast("string"), F.col("sales_order_detail_id").cast("string")), 256).alias("fact_sales_key"),
            F.col("sales_order_id"),
            F.col("sales_order_detail_id"),
            F.date_format(F.col("h.order_date"), "yyyyMMdd").cast("int").alias("order_date_key"),
            F.date_format(F.col("h.due_date"), "yyyyMMdd").cast("int").alias("due_date_key"),
            F.date_format(F.col("h.ship_date"), "yyyyMMdd").cast("int").alias("ship_date_key"),
            F.col("h.customer_id").alias("customer_key"),
            F.col("h.salesperson_id").alias("salesperson_key"),
            F.col("h.territory_id").alias("territory_key"),
            F.col("h.bill_to_address_id").alias("bill_to_geography_key"),
            F.col("h.ship_to_address_id").alias("ship_to_geography_key"),
            F.col("d.product_id").alias("product_key"),
            F.col("h.sales_order_number"),
            F.col("h.purchase_order_number"),
            F.col("h.account_number"),
            F.col("h.status_code"),
            F.col("h.status_name"),
            F.col("h.online_order_flag"),
            F.col("d.carrier_tracking_number"),
            F.col("d.special_offer_id"),
            F.col("d.order_qty"),
            F.col("d.unit_price"),
            F.col("d.unit_price_discount"),
            gross_sales_amount.alias("gross_sales_amount"),
            discount_amount.alias("discount_amount"),
            net_sales_amount.alias("net_sales_amount"),
            F.greatest(F.col("h.source_modified_at"), F.col("d.source_modified_at")).alias("latest_source_modified_at")
        )
    )
