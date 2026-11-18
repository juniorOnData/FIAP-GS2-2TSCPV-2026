# Databricks notebook source
# MAGIC %md
# MAGIC # 01_sibh_stations_landing
# MAGIC
# MAGIC ## Funções de ingestão
# MAGIC
# MAGIC `fetch_stations` faz a chamada à API com retentativas para erros transitórios e devolve os registros do cadastro. `validate` confere o formato, a presença de identificadores e de estações da capital, além de resumir duplicidades e instituições do cadastro.
# MAGIC
# MAGIC Baixa o cadastro de estações do SIBH (SP Águas) e grava no volume `fiap_analytics.bronze.sibh_stations`.
# MAGIC
# MAGIC Fonte: `https://cth.daee.sp.gov.br/sibh/api/v2/stations` (API pública, sem autenticação). A resposta traz o estado inteiro, de todas as instituições (SP Águas, SAISP, CEMADEN, ANA, SABESP, CETESB, INMET...).
# MAGIC
# MAGIC Regras (mesmo padrão do CEMADEN):
# MAGIC
# MAGIC 1. Arquivo guardado exatamente como a API devolve, em JSON por linha comprimido (gzip). Sem filtro de município nem de instituição: o recorte é feito na Silver.
# MAGIC 2. Uma coleta por dia: rodar de novo no mesmo dia sobrescreve o arquivo do dia.
# MAGIC 3. Manifesto gravado por último (contagem, SHA256, campos, horário), usado na conferência da Bronze.
# MAGIC 4. Retry com backoff para 429, 5xx e erros de rede.
# MAGIC 5. Uma linha no `fiap_analytics.bronze.ingestion_log` (etapa `landing`).
# MAGIC
# MAGIC Layout: `/Volumes/fiap_analytics/bronze/sibh_stations/data_coleta=AAAA-MM-DD/stations_AAAAMMDD.json.gz` e `.manifest.json`.

# COMMAND ----------

dbutils.widgets.text("api_url", "https://cth.daee.sp.gov.br/sibh/api/v2/stations", "URL do cadastro de estações")
dbutils.widgets.text("codibge_check", "3550308", "Município usado só para conferência")

# COMMAND ----------

import gzip
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

import requests

CATALOG = "fiap_analytics"
SCHEMA = "bronze"
VOLUME = "sibh_stations"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
DATASET = "sibh_stations"

API_URL = dbutils.widgets.get("api_url").strip()
CODIBGE_CHECK = dbutils.widgets.get("codibge_check").strip()
MAX_ATTEMPTS = 5
TIMEOUT_S = 120
HEADERS = {"Accept": "application/json", "User-Agent": "ChuvaSegura-FIAP/1.0 (projeto academico)"}

BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc)

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME} "
    "COMMENT 'Landing do cadastro de estações do SIBH/SP Águas (api/v2/stations). JSON por linha, gzip, um arquivo por coleta diária.'"
)
print(f"batch_id={BATCH_ID} | url={API_URL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log de execução

# COMMAND ----------

# Log padrão da origem (src/common/ingestion_log.py). Sobe pelas pastas até achar src/ para importar o módulo.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common.ingestion_log import IngestionLog  # noqa: E402

LOG_TABLE = "fiap_analytics.bronze.sibh_ingestion_log"
FONTE = "sibh_stations"
ETAPA = "landing"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coleta

# COMMAND ----------

def fetch_stations() -> list:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        wait = min(60, 5 * 2 ** (attempt - 1))
        try:
            resp = requests.get(API_URL, headers=HEADERS, timeout=TIMEOUT_S)
        except (requests.ConnectionError, requests.Timeout) as exc:
            print(f"Erro de rede (tentativa {attempt}/{MAX_ATTEMPTS}): {type(exc).__name__}. Nova tentativa em {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code == 429 or resp.status_code >= 500:
            print(f"API respondeu {resp.status_code} (tentativa {attempt}/{MAX_ATTEMPTS}). Nova tentativa em {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code != 200:
            raise RuntimeError(f"API respondeu {resp.status_code}: {resp.text[:300]}")
        body = json.loads(resp.content.decode("utf-8"))
        if not isinstance(body, list):
            raise RuntimeError(f"Esperava uma lista, veio {type(body).__name__}")
        return body
    raise RuntimeError(f"Falha após {MAX_ATTEMPTS} tentativas")


def validate(records: list) -> dict:
    if not records:
        raise RuntimeError("A API devolveu o cadastro vazio")
    missing_id = sum(1 for r in records if not isinstance(r, dict) or not r.get("id"))
    if missing_id:
        raise RuntimeError(f"{missing_id} registros sem 'id'")
    ids = [r["id"] for r in records]
    capital = [r for r in records if r.get("cod_ibge") == CODIBGE_CHECK]
    if not capital:
        raise RuntimeError(f"Nenhuma estação com cod_ibge={CODIBGE_CHECK}. O formato da resposta mudou?")
    return {
        "records": len(records),
        "duplicated_id": len(ids) - len(set(ids)),
        "capital": len(capital),
        "by_owner_capital": dict(Counter(r.get("station_owner") for r in capital)),
        "fields": sorted({k for r in records for k in r}),
    }

# COMMAND ----------

try:
    records = fetch_stations()
    stats = validate(records)

    collected_at = datetime.now(timezone.utc)
    snapshot_date = collected_at.strftime("%Y-%m-%d")
    base_name = f"stations_{collected_at:%Y%m%d}"
    target_dir = f"{VOLUME_PATH}/data_coleta={snapshot_date}"
    data_path = f"{target_dir}/{base_name}.json.gz"
    manifest_path = f"{target_dir}/{base_name}.manifest.json"
    os.makedirs(target_dir, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_file = os.path.join(tmp, f"{base_name}.json.gz")
        with gzip.open(tmp_file, "wt", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        with open(tmp_file, "rb") as fh:
            sha256 = hashlib.sha256(fh.read()).hexdigest()
        size_bytes = os.path.getsize(tmp_file)
        shutil.copyfile(tmp_file, data_path)

    manifest = {
        "dataset": DATASET,
        "file_name": f"{base_name}.json.gz",
        "snapshot_date": snapshot_date,
        "collected_at_utc": collected_at.isoformat(),
        "source_url": API_URL,
        "record_count": stats["records"],
        "capital_count": stats["capital"],
        "fields": stats["fields"],
        "api_calls": 1,
        "sha256": sha256,
        "size_bytes": size_bytes,
        "landing_batch_id": BATCH_ID,
    }
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)

    message = f"capital={stats['capital']}; id_duplicado={stats['duplicated_id']}; donos_capital={stats['by_owner_capital']}"
    registrar(snapshot_date, "SUCESSO", STARTED_AT, registros=stats["records"], paginas=1,
              duplicados=stats["duplicated_id"], arquivo=data_path, sha256=sha256, mensagem=message)

    print(f"OK: {stats['records']} estações do estado gravadas em {data_path}")
    print(message)
    print(f"Campos recebidos: {stats['fields']}")

except Exception as exc:
    registrar(datetime.now(timezone.utc).strftime("%Y-%m-%d"), "FALHA", STARTED_AT, mensagem=str(exc))
    raise
