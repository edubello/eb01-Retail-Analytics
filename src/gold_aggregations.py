from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_obt_sales(
    df_sales_silver: DataFrame,
    df_products_silver: DataFrame,
    df_users_silver: DataFrame,
) -> DataFrame:
    """Constrói a One Big Table (OBT) consolidando vendas, produtos e clientes."""
    return (
        df_sales_silver.alias("s")
        .join(df_products_silver.alias("p"), on="product_id", how="inner")
        .join(df_users_silver.alias("u"), on="client_id", how="inner")
        .select(
            F.col("s.sale_id"),
            F.col("s.sale_date"),
            F.col("u.client_id"),
            F.col("u.client_name"),
            F.col("u.email").alias("client_email"),
            F.col("p.product_id"),
            F.col("p.product_name"),
            F.col("s.quantity"),
            F.col("s.unit_price"),
            F.col("s.total_amount"),
            F.current_timestamp().alias("_gold_processed_at"),
        )
    )


def build_agg_sales_by_product(df_obt_sales: DataFrame) -> DataFrame:
    """Agrega métricas comerciais por produto (receita, volume e pedidos)."""
    return (
        df_obt_sales.groupBy("product_id", "product_name")
        .agg(
            F.countDistinct("sale_id").alias("total_orders"),
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
        .orderBy(F.col("total_revenue").desc())
    )


def build_agg_customer_ltv(df_obt_sales: DataFrame) -> DataFrame:
    """Calcula o Lifetime Value (LTV), frequência de pedidos e ticket médio por cliente."""
    return (
        df_obt_sales.groupBy("client_id", "client_name", "client_email")
        .agg(
            F.countDistinct("sale_id").alias("total_orders"),
            F.sum("quantity").alias("total_items_purchased"),
            F.round(F.sum("total_amount"), 2).alias("lifetime_value"),
            F.round(F.avg("total_amount"), 2).alias("avg_ticket"),
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
        .orderBy(F.col("lifetime_value").desc())
    )