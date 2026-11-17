from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, DateType


def transform_products(df_bronze: DataFrame) -> DataFrame:
    """Limpa, deduplica e tipa a dimensão de produtos (removendo '$' de price)."""
    return (
        df_bronze
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
        .select(
            F.col("product_id").cast(IntegerType()).alias("product_id"),
            F.trim(F.col("product")).alias("product_name"),
            F.regexp_replace(F.col("price"), r"[^0-9.]", "").cast(DecimalType(10, 2)).alias("unit_price"),
            F.col("_ingestion_timestamp").alias("_bronze_ingested_at"),
            F.current_timestamp().alias("_silver_processed_at"),
        )
    )


def transform_users(df_bronze: DataFrame) -> DataFrame:
    """Padroniza e tipa a dimensão de clientes (descartando colunas sensíveis de login/senha)."""
    id_col = "client_id" if "client_id" in df_bronze.columns else "user_id"

    return (
        df_bronze
        .filter(F.col(id_col).isNotNull())
        .dropDuplicates([id_col])
        .select(
            F.col(id_col).cast(IntegerType()).alias("client_id"),
            F.initcap(F.trim(F.col("name"))).alias("client_name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.col("_ingestion_timestamp").alias("_bronze_ingested_at"),
            F.current_timestamp().alias("_silver_processed_at"),
        )
    )


def transform_sales(
    df_sales_bronze: DataFrame,
    df_users_silver: DataFrame,
    df_products_silver: DataFrame,
) -> DataFrame:
    """Limpa vendas, aplica integridade referencial e calcula total_amount."""
    df_clean = (
        df_sales_bronze
        .filter(
            F.col("sale_id").isNotNull()
            & F.col("product_id").isNotNull()
            & F.col("client_id").isNotNull()
        )
        .dropDuplicates(["sale_id"])
        .select(
            F.col("sale_id").cast(IntegerType()).alias("sale_id"),
            F.col("date").cast(DateType()).alias("sale_date"),
            F.col("client_id").cast(IntegerType()).alias("client_id"),
            F.col("product_id").cast(IntegerType()).alias("product_id"),
            F.col("qtde").cast(IntegerType()).alias("quantity"),
            F.col("_ingestion_timestamp").alias("_bronze_ingested_at"),
        )
    )

    return (
        df_clean
        .join(df_users_silver.select("client_id"), on="client_id", how="left_semi")
        .join(df_products_silver.select("product_id", "unit_price"), on="product_id", how="inner")
        .withColumn("total_amount", (F.col("quantity") * F.col("unit_price")).cast(DecimalType(12, 2)))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .select(
            "sale_id",
            "client_id",
            "product_id",
            "sale_date",
            "quantity",
            "unit_price",
            "total_amount",
            "_bronze_ingested_at",
            "_silver_processed_at",
        )
    )