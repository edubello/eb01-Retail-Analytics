# Databricks notebook source
# MAGIC %md
# MAGIC ###  02_0_generate_mock_data
# MAGIC Baixa os arquivos de exemplo diretamente do repositório remoto 
# MAGIC para o Volume da Landing Zone no Unity Catalog (escrita direta via POSIX).
# MAGIC

# COMMAND ----------

import sys
import urllib.request
from pathlib import Path

project_root = Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils import load_settings

# 1. Carrega configurações centralizadas
config = load_settings()
catalog_name = config["databricks"]["catalog"]
landing_schema = config["databricks"]["schemas"]["landing"]
volume_name = config["databricks"]["volumes"]["raw_volume"]
datasets = config["datasets"]

# 2. Caminho oficial do Volume no Unity Catalog
volume_path = Path(f"/Volumes/{catalog_name}/{landing_schema}/{volume_name}")
volume_path.mkdir(parents=True, exist_ok=True)
print(f"[INFO] Landing Zone configurada no Volume: {volume_path}")

# 3. URL base e download direto via POSIX
BASE_URL = "https://raw.githubusercontent.com/KaelDucatti/pyspark_test_data/main"

for ds_name, ds_info in datasets.items():
    file_name = ds_info["filename"]
    file_url = f"{BASE_URL}/{file_name}"
    dest_path = volume_path / file_name
    
    print(f"[INFO] Baixando {ds_name} ({file_name}) de {file_url}...")
    try:
        urllib.request.urlretrieve(file_url, str(dest_path))
        print(f"[SUCCESS] {file_name} salvo com sucesso em: {dest_path}")
    except Exception as e:
        print(f"[ERROR] Falha ao processar {file_name}: {e}")

print("[SUCCESS] Ingestão da Landing Zone concluída com sucesso!")