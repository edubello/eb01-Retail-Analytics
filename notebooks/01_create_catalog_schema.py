# Databricks notebook source
# MAGIC %md
# MAGIC ### 01_create_catalog_schema
# MAGIC Responsável por criar dinamicamente a infraestrutura de Catálogo, Schemas 
# MAGIC e Volumes no Unity Catalog com base no arquivo settings.yaml (load_settings)

# COMMAND ----------

import sys
from pathlib import Path

# Adiciona a raiz do projeto ao sys.path para importar src
project_root = Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils import load_settings

# 1. Carregamento centralizado das configurações
config = load_settings()
catalog_name = config["databricks"]["catalog"]
schemas = config["databricks"]["schemas"]
volume_name = config["databricks"]["volumes"]["raw_volume"]

print(f"[INFO] Iniciando provisionamento da estrutura de metadados e armazenamento no Unity Catalog: {catalog_name}")

# 2. Criação do Catálogo Dedicado
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name};")
print(f"[SUCCESS] Catálogo garantido: {catalog_name}")

# 3. Criação dinâmica de todos os Schemas (Landing + Medallion: Bronze, Silver, Gold)
for schema_key, schema_name in schemas.items():
    full_schema_name = f"{catalog_name}.{schema_name}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema_name};")
    print(f"[SUCCESS] Schema garantido: {full_schema_name}")

# 4. Criação do Volume Gerenciado na Landing Zone para os arquivos brutos
landing_volume = f"{catalog_name}.{schemas['landing']}.{volume_name}"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {landing_volume};")
print(f"[SUCCESS] Volume Unity Catalog garantido: {landing_volume}")

# 5. Configuração padrão do contexto da sessão Spark
spark.sql(f"USE CATALOG {catalog_name};")
spark.sql(f"USE SCHEMA {schemas['bronze']};")
print(f"[SUCCESS] Sessão configurada! Catálogo: {catalog_name} | Schema padrão: {schemas['bronze']}")