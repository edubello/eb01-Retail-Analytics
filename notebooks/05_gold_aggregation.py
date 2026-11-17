# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Gold Layer Aggregation (Business KPIs & Analytical Models)
# MAGIC **Projeto:** Retail Analytics Lakehouse (PySpark & Delta Lake)  
# MAGIC **Autor:** Eduardo Bello  
# MAGIC
# MAGIC ### Responsabilidades da Camada Gold
# MAGIC * **Modelagem Orientada ao Negócio:** Criação de tabelas agregadas e desnormalizadas prontas para consumo em dashboards (Power BI / Databricks SQL).
# MAGIC * **Cálculo de KPIs:** Receita bruta, volume de itens vendidos, ticket médio e ranking de produtos e clientes.
# MAGIC * **Governança e Linhagem:** Persistência em formato Delta no schema `gold` do Unity Catalog.

# COMMAND ----------

import sys
from pathlib import Path

project_root = Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils import load_settings
from src.gold_aggregations import (
    build_obt_sales,
    build_agg_sales_by_product,
    build_agg_customer_ltv,
)

config = load_settings()
catalog_name = config["databricks"]["catalog"]
silver_schema = config["databricks"]["schemas"]["silver"]
gold_schema = config["databricks"]["schemas"]["gold"]
datasets = config["datasets"]

df_sales = spark.table(f"{catalog_name}.{silver_schema}.{datasets['sales'].get('silver_table', 'fact_sales')}")
df_products = spark.table(f"{catalog_name}.{silver_schema}.{datasets['products'].get('silver_table', 'dim_products')}")
df_users = spark.table(f"{catalog_name}.{silver_schema}.{datasets['users'].get('silver_table', 'dim_users')}")

print(f"[INFO] Tabelas Silver carregadas de '{catalog_name}.{silver_schema}'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Visão Consolidada: `gold.obt_sales_analytics`
# MAGIC
# MAGIC obt = One Big Table
# MAGIC
# MAGIC Tabela desnormalizada unindo vendas, produtos e clientes para facilitar filtros e análises ad-hoc.

# COMMAND ----------

gold_obt_tbl = f"{catalog_name}.{gold_schema}.obt_sales_analytics"
df_obt_sales = build_obt_sales(df_sales, df_products, df_users)

df_obt_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_obt_tbl)

print(f"[SUCCESS] Tabela '{gold_obt_tbl}' salva com {df_obt_sales.count()} registros!")
df_obt_sales.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. KPI de Produtos: `gold.agg_sales_by_product`
# MAGIC Agrupa o desempenho comercial por produto (receita total, quantidade vendida e número de pedidos).

# COMMAND ----------

gold_prod_tbl = f"{catalog_name}.{gold_schema}.agg_sales_by_product"
df_agg_products = build_agg_sales_by_product(df_obt_sales)

df_agg_products.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_prod_tbl)

print(f"[SUCCESS] Tabela '{gold_prod_tbl}' salva!")
df_agg_products.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. KPI de Clientes: `gold.agg_customer_ltv`
# MAGIC Calcula o *Lifetime Value* (LTV), total de pedidos e ticket médio por cliente.

# COMMAND ----------

gold_cust_tbl = f"{catalog_name}.{gold_schema}.agg_customer_ltv"
df_agg_customers = build_agg_customer_ltv(df_obt_sales)

df_agg_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_cust_tbl)

print(f"[SUCCESS] Tabela '{gold_cust_tbl}' salva!")
df_agg_customers.show(10, truncate=False)