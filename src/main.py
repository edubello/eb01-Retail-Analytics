"""
Script Principal de Orquestração do Pipeline Lakehouse
Projeto: Retail Analytics Lakehouse (PySpark & Delta Lake)
Autor: Eduardo de Castro Bello

Responsável por centralizar a execução das transformações Silver e Gold 
caso o pipeline seja acionado via script Python (ex: Databricks Workflows).
"""

from pathlib import Path
import yaml
from pyspark.sql import SparkSession

from src.utils import load_settings
from src.silver_transformations import transform_products, transform_users, transform_sales
from src.gold_aggregations import build_obt_sales, build_agg_sales_by_product, build_agg_customer_ltv


def run_pipeline() -> None:
    # Inicializa a sessão Spark (caso executado fora de um notebook)
    spark = SparkSession.builder.appName("RetailAnalyticsPipeline").getOrCreate()

    # 1. Carrega configurações do settings.yaml
    config = load_settings()
    catalog = config["databricks"]["catalog"]
    bronze_schema = config["databricks"]["schemas"]["bronze"]
    silver_schema = config["databricks"]["schemas"]["silver"]
    gold_schema = config["databricks"]["schemas"]["gold"]
    datasets = config["datasets"]

    print(f"[INFO] Iniciando pipeline analítico no catálogo: {catalog}")

    # --- CAMADA SILVER ---
    print("[INFO] Processando Camada Silver...")
    df_prod_bronze = spark.table(f"{catalog}.{bronze_schema}.{datasets['products']['bronze_table']}")
    df_user_bronze = spark.table(f"{catalog}.{bronze_schema}.{datasets['users']['bronze_table']}")
    df_sales_bronze = spark.table(f"{catalog}.{bronze_schema}.{datasets['sales']['bronze_table']}")

    df_prod_silver = transform_products(df_prod_bronze)
    df_user_silver = transform_users(df_user_bronze)
    df_sales_silver = transform_sales(df_sales_bronze, df_user_silver, df_prod_silver)

    # Gravação Silver
    df_prod_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{silver_schema}.dim_products")
    df_user_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{silver_schema}.dim_users")
    df_sales_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{silver_schema}.fact_sales")
    print("[SUCCESS] Camada Silver processada e salva com sucesso!")

    # --- CAMADA GOLD ---
    print("[INFO] Processando Camada Gold...")
    df_obt = build_obt_sales(df_sales_silver, df_prod_silver, df_user_silver)
    df_agg_prod = build_agg_sales_by_product(df_obt)
    df_agg_cust = build_agg_customer_ltv(df_obt)

    # Gravação Gold
    df_obt.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{gold_schema}.obt_sales_analytics")
    df_agg_prod.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{gold_schema}.agg_sales_by_product")
    df_agg_cust.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{gold_schema}.agg_customer_ltv")
    print("[SUCCESS] Camada Gold processada e salva com sucesso!")

    print("[SUCCESS] Pipeline completo executado com êxito!")


if __name__ == "__main__":
    run_pipeline()