# Databricks notebook source
# MAGIC %md
# MAGIC # 02.1 — Data Exploring, Diagnosis & Raw Fixing
# MAGIC **Projeto:** Retail Analytics Lakehouse (PySpark & Delta Lake)  
# MAGIC **Autor:** Eduardo Bello  
# MAGIC
# MAGIC Data Discovery & Quality Audit (Exploração Inicial)
# MAGIC
# MAGIC ### Objetivo deste Notebook
# MAGIC Antes de estruturar a ingestão definitiva para a camada **Bronze** (`03_bronze_ingestion`), realizamos uma etapa de **Data Discovery** diretamente sobre os arquivos brutos armazenados no **Volume da Landing Zone** no Unity Catalog.
# MAGIC
# MAGIC Arquitetura Medallion são estruturadas utilizando tabelas Delta Lake(parquet) dentro de um Data Lakehouse.
# MAGIC
# MAGIC
# MAGIC Neste fluxo passo a passo, vamos:
# MAGIC 1. Carregar os metadados do projeto via `settings.yaml`.
# MAGIC 2. Inspecionar os arquivos CSV (`sales.csv` e `users.csv`) para validar delimitadores e inferência inicial.
# MAGIC 3. Diagnosticar uma falha estrutural no arquivo `products.json` (ausência de colchetes de array JSON `[]`).
# MAGIC 4. Aplicar a sanitização no arquivo bruto da Landing Zone e validar a leitura via PySpark antes de seguir para a camada Bronze.

# COMMAND ----------

# MAGIC %md
# MAGIC 00- Setup e Leitura do settings.**yaml**

# COMMAND ----------

import sys
import json
from pathlib import Path
from pyspark.sql import functions as F

project_root = Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils import load_settings

# 1. Carregamento centralizado das configurações
config = load_settings()
catalog_name = config["databricks"]["catalog"]
landing_schema = config["databricks"]["schemas"]["landing"]
volume_name = config["databricks"]["volumes"]["raw_volume"]
datasets = config["datasets"]

# Caminho POSIX oficial do Volume no Unity Catalog
volume_path = Path(f"/Volumes/{catalog_name}/{landing_schema}/{volume_name}")

print(f"[INFO] Configurações carregadas com sucesso via src.utils")
print(f"[INFO] Volume da Landing Zone: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01. Inspeção Exploratória dos Arquivos CSV (`sales` e `users`)
# MAGIC Validando se os arquivos delimitados por vírgula estão íntegros, possuem cabeçalho legível e permitem inferência básica de tipos pelo Spark.

# COMMAND ----------

sales_path = str(volume_path / datasets["sales"]["filename"])
users_path = str(volume_path / datasets["users"]["filename"])

df_sales_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_path)
df_users_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(users_path)

print("--- Schema & Amostra: SALES ---")
df_sales_raw.printSchema()
df_sales_raw.show(3, truncate=False)

print("--- Schema & Amostra: USERS ---")
df_users_raw.printSchema()
df_users_raw.show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Diagnóstico de Anomalia Estrutural em `products.json`
# MAGIC Ao tentar processar arquivos JSON de origem externa, um erro comum é receber múltiplos objetos JSON separados por vírgula, mas **sem os colchetes `[]` delimitando um array válido** (ou sem estar no formato JSON Lines puro).
# MAGIC
# MAGIC Vamos primeiro inspecionar o texto bruto e testar o parser nativo `json.loads()` do Python para capturar o erro exato antes de qualquer correção.

# COMMAND ----------

products_file = volume_path / datasets["products"]["filename"]

# Lê o conteúdo textual bruto direto do Volume
raw_text = products_file.read_text(encoding="utf-8").strip()

print("=== 1. INSPEÇÃO VISUAL DO CONTEÚDO BRUTO ===")
print(f"Primeiro caractere : '{raw_text[0]}'")
print(f"Último caractere   : '{raw_text[-1]}'")
print(f"\nAmostra dos primeiros 250 caracteres:\n{raw_text[:250]}...")
print(f"\nAmostra dos últimos 250 caracteres:\n...{raw_text[-250:]}")

print("\n=== 2. DIAGNÓSTICO ESTRUTURAL (VALIDAÇÃO DE SINTAXE JSON) ===")
try:
    json.loads(raw_text)
    print("[OK] O arquivo já é um JSON válido.")
except json.JSONDecodeError as e:
    print(f"[ALERTA - JSON MALFORMADO] Falha no parser padrão: {e}")
    
    # Verifica especificamente a ausência dos colchetes de array
    if not raw_text.startswith("[") or not raw_text.endswith("]"):
        print("[DIAGNÓSTICO] O arquivo contém objetos JSON soltos sem os colchetes delimitadores de lista '[' e ']'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Higienização (Sanitização) e Validação via PySpark
# MAGIC Com a causa raiz confirmada, aplicamos uma regra idempotente de correção:
# MAGIC * Se o texto não iniciar com `[`, envolvemos o conteúdo com `[` e `]` (removendo qualquer vírgula sobrando no final com `.rstrip(",")`).
# MAGIC * Salvamos o arquivo higienizado no próprio Volume da Landing Zone.
# MAGIC * Validamos a leitura distribuída com `spark.read.option("multiline", "true").json(...)`.

# COMMAND ----------

print("=== 3. APLICANDO CORREÇÃO ESTRUTURAL ===")

if not raw_text.startswith("[") or not raw_text.endswith("]"):
    # Remove possíveis colchetes, vírgula final antes de fechar o colchete
    clean_body = raw_text.lstrip("[").rstrip("]").rstrip(",")
    # Aqui ocorre a transformação (texto entre colchetes e quebra de linha no final)
    sanitized_text = f"[\n{clean_body}\n]"
    
    # Sobrescreve o arquivo corrigido no Volume da Landing Zone
    products_file.write_text(sanitized_text, encoding="utf-8")
    print(f"[FIX APLICADO] Colchetes '[' e ']' adicionados com sucesso em: {products_file}")
    print(f"Novo início do arquivo: {sanitized_text[:60]}...")
else:
    print("[INFO] Nenhuma correção necessária. Arquivo já está encapsulado em colchetes.")

print("\n=== 4. VALIDAÇÃO DE LEITURA NO PYSPARK ===")
df_products_raw = spark.read.option("multiline", "true").json(str(products_file))
df_products_raw.printSchema()
df_products_raw.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quality Gates Exploratórios (Integridade Referencial)
# MAGIC Com as três fontes legíveis pelo Spark, executamos verificações rápidas de integridade antes de liberar o fluxo para a ingestão Bronze (`03_bronze_ingestion`):
# MAGIC * Contagem total de registros.
# MAGIC * Existência de chaves primárias duplicadas ou nulas em `sales`.
# MAGIC * Integridade referencial entre `sales` e `products` (`left_anti` join).

# COMMAND ----------

n_sales = df_sales_raw.count()
dup_sales = n_sales - df_sales_raw.select("sale_id").distinct().count()
null_keys = df_sales_raw.filter(F.col("product_id").isNull() | F.col("client_id").isNull()).count()
orphan_products = df_sales_raw.join(df_products_raw, "product_id", "left_anti").count()

print(f"Total de vendas             : {n_sales}")
print(f"Duplicatas em sale_id       : {dup_sales}")
print(f"Chaves nulas em sales       : {null_keys}")
print(f"Produtos órfãos nas vendas  : {orphan_products}")

assert dup_sales == 0 and null_keys == 0, "[FALHA DE DATA QUALITY] Vendas contêm duplicatas ou chaves nulas!"
assert orphan_products == 0, "[FALHA DE DATA QUALITY] Existem produtos órfãos nas vendas!"

print("\n[SUCCESS] Diagnóstico concluído e Quality Gates aprovados! Pronto para rodar a Bronze Ingestion.")