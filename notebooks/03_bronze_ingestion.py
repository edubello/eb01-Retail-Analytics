# Databricks notebook source
# MAGIC %md
# MAGIC ## 03_bronze_ingestion / Notebook 03
# MAGIC Lê os arquivos brutos do Volume da Landing Zone no Unity Catalog, 
# MAGIC adiciona metadados de auditoria e grava as Delta Tables na camada Bronze.
# MAGIC

# COMMAND ----------

"""
03_bronze_ingestion / Notebook 03
Lê os arquivos brutos do Volume da Landing Zone no Unity Catalog, 
adiciona metadados de auditoria e grava as Delta Tables na camada Bronze.
"""

from pathlib import Path
import yaml
from pyspark.sql import functions as F

# 1. Raiz do projeto
current_path = Path.cwd()
PROJECT_ROOT = current_path.parent if current_path.name in ("src", "notebooks") else current_path
config_path = PROJECT_ROOT / "config" / "settings.yaml"

print(f"[INFO] Carregando configurações de: {config_path}")

# 2. Leitura do arquivo YAML
with open(config_path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# Extração das variáveis de catálogo, schemas, volumes e datasets
catalog_name = config["databricks"]["catalog"]
landing_schema = config["databricks"]["schemas"]["landing"]
bronze_schema = config["databricks"]["schemas"]["bronze"]
volume_name = config["databricks"]["volumes"]["raw_volume"]
datasets = config["datasets"]

# 3. Caminho oficial do Volume da Landing Zone
volume_path = f"/Volumes/{catalog_name}/{landing_schema}/{volume_name}"
print(f"[INFO] Lendo dados brutos do Volume: {volume_path}")


# --- 1. INGESTÃO: PRODUCTS (JSON) ---
prod_info = datasets["products"]
products_file = prod_info["filename"]
bronze_products_table = f"{catalog_name}.{bronze_schema}.{prod_info['bronze_table']}"

print(f"[INFO] Processando {products_file} para a tabela {bronze_products_table}...")

df_products = spark.read \
    .option("multiline", "true") \
    .json(f"{volume_path}/{products_file}")

df_products.withColumn("_ingestion_timestamp", F.current_timestamp()) \
    .withColumn("_source_file", F.lit(products_file)) \
    .write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(bronze_products_table)

print(f"[SUCCESS] Tabela {bronze_products_table} salva com sucesso!")


# --- 2. INGESTÃO: SALES (CSV) ---
sales_info = datasets["sales"]
sales_file = sales_info["filename"]
bronze_sales_table = f"{catalog_name}.{bronze_schema}.{sales_info['bronze_table']}"

print(f"[INFO] Processando {sales_file} para a tabela {bronze_sales_table}...")

df_sales = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{volume_path}/{sales_file}")

df_sales.withColumn("_ingestion_timestamp", F.current_timestamp()) \
    .withColumn("_source_file", F.lit(sales_file)) \
    .write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(bronze_sales_table)

print(f"[SUCCESS] Tabela {bronze_sales_table} salva com sucesso!")


# --- 3. INGESTÃO: USERS (CSV) ---
users_info = datasets["users"]
users_file = users_info["filename"]
bronze_users_table = f"{catalog_name}.{bronze_schema}.{users_info['bronze_table']}"

print(f"[INFO] Processando {users_file} para a tabela {bronze_users_table}...")

df_users = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{volume_path}/{users_file}")

df_users.withColumn("_ingestion_timestamp", F.current_timestamp()) \
    .withColumn("_source_file", F.lit(users_file)) \
    .write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(bronze_users_table)

print(f"[SUCCESS] Tabela {bronze_users_table} salva com sucesso!")

print(f"[SUCCESS] Pipeline da Camada Bronze concluído para todas as tabelas em {catalog_name}.{bronze_schema}!")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_analytics.bronze.bronze_sales order by date,sale_id desc limit 10
