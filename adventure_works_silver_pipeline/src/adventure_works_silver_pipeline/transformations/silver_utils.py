from typing import Iterable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

BRONZE_CATALOG = "edp_bronze_dev"
BRONZE_SCHEMA = "adventure_works"
BRONZE_PREFIX = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}"


def cleaned_string(column_name: str):
    return F.when(F.col(column_name).isNull(), F.lit(None)).otherwise(F.trim(F.col(column_name)))


def standardize_business_date(column_name: str):
    return F.to_date(F.col(column_name))


def standardize_timestamp(column_name: str):
    return F.col(column_name).cast("timestamp")


def deduplicate_latest(
    df: DataFrame,
    key_cols: Iterable[str],
    order_cols: Iterable[str],
) -> DataFrame:
    window = Window.partitionBy(*[F.col(column_name) for column_name in key_cols]).orderBy(
        *[F.col(column_name).desc_nulls_last() for column_name in order_cols],
        *[F.col(column_name).desc_nulls_last() for column_name in key_cols],
    )
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .where(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def add_audit_metadata(df: DataFrame, source_table: str) -> DataFrame:
    business_columns = list(df.columns)
    hash_columns = [F.coalesce(F.col(column_name).cast("string"), F.lit("∅")) for column_name in business_columns]
    return (
        df.withColumn("source_system", F.lit("adventure_works"))
        .withColumn("source_catalog", F.lit(BRONZE_CATALOG))
        .withColumn("source_schema", F.lit(BRONZE_SCHEMA))
        .withColumn("source_table", F.lit(source_table))
        .withColumn("silver_record_hash", F.sha2(F.concat_ws("||", *hash_columns), 256))
    )
