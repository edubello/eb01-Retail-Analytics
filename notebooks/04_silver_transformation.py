# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Silver Layer Transformation (Limpeza, Tipagem e Modelagem Relacional)
# MAGIC **Projeto:** Retail Analytics Lakehouse (PySpark & Delta Lake)  
# MAGIC **Autor:** Eduardo Bello  
# MAGIC
# MAGIC ### Responsabilidades da Camada Silver
# MAGIC * **Deduplicação:** Remoção de registros duplicados com base nas chaves de negócio (`product_id`, `client_id`, `sale_id`).
# MAGIC * **Tipagem Forte (Schema Enforcement):** Conversão explícita de strings brutas para `IntegerType`, `DecimalType(10,2)`, `DateType` e `TimestampType`.
# MAGIC * **Higienização e Padronização:** Remoção de espaços em branco (`trim`), normalização de caixa (`lower`/`initcap`) e tratamento de valores nulos.
# MAGIC * **Integridade Referencial:** Garantia de que a tabela fato (`fact_sales`) possui correspondência válida nas dimensões (`dim_products` e `dim_users`).

# COMMAND ----------

import sys
from pathlib import Path

project_root = Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

import src.utils
import src.silver_transformations

from src.utils import load_settings
from src.silver_transformations import transform_products, transform_users, transform_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Transformação: `dim_products`
# MAGIC Lê `bronze.products`, remove duplicidades por `product_id`, limpa espaços nos textos, garante tipagem monetária precisa (`Decimal(10,2)`) e preenche categorias ausentes com `"Não Categorizado"`.

# COMMAND ----------

prod_cfg = datasets["products"]
bronze_products_tbl = f"{catalog_name}.{bronze_schema}.{prod_cfg['bronze_table']}"
silver_products_tbl = f"{catalog_name}.{silver_schema}.{prod_cfg.get('silver_table', 'dim_products')}"

df_products_bronze = spark.table(bronze_products_tbl)
df_products_silver = transform_products(df_products_bronze)

df_products_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_products_tbl)

print(f"[SUCCESS] Tabela '{silver_products_tbl}' salva com {df_products_silver.count()} registros!")
df_products_silver.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transformação: `dim_users`
# MAGIC Lê `bronze.users`, remove duplicidades por `client_id` (ou `user_id`, ajustado conforme o schema de origem), padroniza nomes e e-mails e aplica tipagem forte.
# MAGIC Importante: foram retrirados os campos de senha, que não são utilizados

# COMMAND ----------

users_cfg = datasets["users"]
bronze_users_tbl = f"{catalog_name}.{bronze_schema}.{users_cfg['bronze_table']}"
silver_users_tbl = f"{catalog_name}.{silver_schema}.{users_cfg.get('silver_table', 'dim_users')}"

df_users_bronze = spark.table(bronze_users_tbl)
df_users_silver = transform_users(df_users_bronze)

df_users_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_users_tbl)

print(f"[SUCCESS] Tabela '{silver_users_tbl}' salva com {df_users_silver.count()} registros!")
df_users_silver.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transformação: `fact_sales` (Com Integridade Referencial)
# MAGIC Lê `bronze.sales`, aplica tipagem nas chaves e métricas, calcula o valor total da venda (`total_amount`) cruzando com o preço unitário de `dim_products` (caso não venha na origem) e garante integridade referencial via `left_semi` join com as dimensões Silver.

# COMMAND ----------

sales_cfg = datasets["sales"]
bronze_sales_tbl = f"{catalog_name}.{bronze_schema}.{sales_cfg['bronze_table']}"
silver_sales_tbl = f"{catalog_name}.{silver_schema}.{sales_cfg.get('silver_table', 'fact_sales')}"

df_sales_bronze = spark.table(bronze_sales_tbl)
df_valid_sales = transform_sales(df_sales_bronze, df_users_silver, df_products_silver)

df_valid_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_sales_tbl)

print(f"[SUCCESS] Tabela '{silver_sales_tbl}' salva com {df_valid_sales.count()} registros válidos!")
df_valid_sales.show(5, truncate=False)