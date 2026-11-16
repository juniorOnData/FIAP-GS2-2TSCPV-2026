# Databricks notebook source
# MAGIC %md
# MAGIC # SIB_LND_001 01_sibh_stations_landing
# MAGIC
# MAGIC ## 01. Identificação
# MAGIC
# MAGIC | Campo | Valor |
# MAGIC |---|---|
# MAGIC | Identificador | SIB_LND_001 |
# MAGIC | Nome | 01_sibh_stations_landing |
# MAGIC | Projeto | ChuvaSegura |
# MAGIC | Domínio de negócio | Hidrologia (SIBH) |
# MAGIC | Camada | Landing |
# MAGIC | Linguagem | Python |
# MAGIC | Responsável técnico | juniorOnData |
# MAGIC | Responsável funcional | A definir |
# MAGIC | Status | Homologação |
# MAGIC | Versão | 1.3 |
# MAGIC | Criticidade | Média |
# MAGIC | Caminho no repositório | `src/sibh/ingestion/01_sibh_stations_landing.py` |
# MAGIC
# MAGIC ## 02. Objetivo
# MAGIC
# MAGIC Baixar o cadastro de estações do SIBH (estado inteiro, todas as instituições) e gravar um arquivo por coleta diária no volume, com manifesto.
# MAGIC
# MAGIC ## 03. Contexto funcional
# MAGIC
# MAGIC O cadastro define quais sensores de chuva e nível da capital serão coletados pela landing de medições.
# MAGIC
# MAGIC ## 04. Arquitetura e fluxo
# MAGIC
# MAGIC | Posição | Item |
# MAGIC |---|---|
# MAGIC | Etapa anterior | Nenhuma |
# MAGIC | Etapa atual | Grava `data_coleta=AAAA-MM-DD/` no volume `sibh_stations` |
# MAGIC | Etapa posterior | `02_sibh_stations_bronze` |
# MAGIC
# MAGIC ## 05. Dependências
# MAGIC
# MAGIC | Tipo | Item | Uso |
# MAGIC |---|---|---|
# MAGIC | Conexão | `https://cth.daee.sp.gov.br/sibh/api/v2/stations` | Cadastro de estações |
# MAGIC | Módulo | `src/common/ingestion_log.py` | Registro no `sibh_ingestion_log` |
# MAGIC | Biblioteca | `requests` | Chamadas HTTP |
# MAGIC | Permissão | USE CATALOG em `fiap_analytics`; USE SCHEMA, CREATE VOLUME, CREATE TABLE e MODIFY em `fiap_analytics.bronze`; READ VOLUME e WRITE VOLUME no volume de landing; acesso de saída à internet no compute | Criar o volume, gravar arquivos e o log |
# MAGIC
# MAGIC ## 06. Parâmetros de entrada
# MAGIC
# MAGIC | Nome | Tipo | Obrigatório | Descrição | Valor padrão | Exemplo |
# MAGIC |---|---|---|---|---|---|
# MAGIC | `api_url` | text (URL) | Sim | Endereço do cadastro de estações | https://cth.daee.sp.gov.br/sibh/api/v2/stations | (padrão) |
# MAGIC | `codibge_check` | text (texto) | Sim | Município usado só para conferir a resposta | 3550308 | 3550308 |
# MAGIC
# MAGIC ## 07. Fontes de dados
# MAGIC
# MAGIC | Fonte | Formato | Origem | Tipo de leitura | Frequência | Chave | Filtros |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | SIBH `api/v2/stations` | JSON | SIBH, SP Águas | Completa (snapshot) | Diária | `id` | Nenhum (recorte na Bronze e na Silver) |
# MAGIC
# MAGIC ## 08. Processamento técnico
# MAGIC
# MAGIC | Etapa | Linguagem | Célula | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | E01 | PY | Parâmetros | Widgets `api_url` e `codibge_check`. |
# MAGIC | E02 | PY | Configuração | Imports, constantes, `BATCH_ID` e criação do volume `sibh_stations`. |
# MAGIC | E03 | PY | Log de ingestão | Importa o `IngestionLog` e define `registrar` (etapa `landing`). |
# MAGIC | E04 | PY | Coleta e validação | Coleta com retentativa e validação: cadastro não vazio, `id` presente, estações da capital presentes. |
# MAGIC | E05 | PY | Gravação | Arquivo gzip em `data_coleta=AAAA-MM-DD/`, manifesto e registro `SUCESSO` ou `FALHA`. |
# MAGIC
# MAGIC ## 09. Regras de negócio
# MAGIC
# MAGIC | ID | Regra | Lógica | Comportamento esperado |
# MAGIC |---|---|---|---|
# MAGIC | RN_SIB_005 | Cadastro como snapshot diário | Uma coleta por dia, sem filtro de município ou instituição; rodar de novo no mesmo dia sobrescreve. | Histórico diário do cadastro do estado inteiro. |
# MAGIC
# MAGIC ## 10. Saídas de dados
# MAGIC
# MAGIC | Destino | Formato | Granularidade | Chave | Modo de escrita |
# MAGIC |---|---|---|---|---|
# MAGIC | Volume `sibh_stations`: `data_coleta=AAAA-MM-DD/stations_AAAAMMDD.json.gz` e `.manifest.json` | JSON por linha (gzip) e manifesto JSON | Um arquivo por coleta diária | `id` | Sobrescreve o arquivo do dia |
# MAGIC
# MAGIC As colunas de cada tabela estão no dicionário de dados (`docs/02_catalogos/dicionario_de_dados.csv`).
# MAGIC
# MAGIC ## 11. Qualidade dos dados
# MAGIC
# MAGIC Para a carga se o cadastro vier vazio, se houver registro sem `id` ou se nenhuma estação for da capital. Duplicidade de `id` e estações por instituição vão na mensagem do log.
# MAGIC
# MAGIC ## 12. Tratamento de erros
# MAGIC
# MAGIC | Situação | Comportamento | Registro | Recuperação |
# MAGIC |---|---|---|---|
# MAGIC | 429, 5xx ou erro de rede | Até 5 tentativas com espera crescente | Saída do notebook | Automática |
# MAGIC | Validação reprovada | Para a execução | `FALHA` no `fiap_analytics.bronze.sibh_ingestion_log` | Conferir a API e rodar de novo |
# MAGIC
# MAGIC ## 13. Segurança e governança
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Classificação dos dados | Pública (API pública do SIBH, SP Águas; confirmar os termos de uso) |
# MAGIC | Dados sensíveis | Nenhum |
# MAGIC | Credenciais | Nenhuma (API pública, sem autenticação) |
# MAGIC | Permissões necessárias | USE CATALOG em `fiap_analytics`; USE SCHEMA, CREATE VOLUME, CREATE TABLE e MODIFY em `fiap_analytics.bronze`; READ VOLUME e WRITE VOLUME no volume de landing; acesso de saída à internet no compute |
# MAGIC | Políticas aplicáveis | Termos de uso dos dados do SIBH (confirmar) |
# MAGIC
# MAGIC ## 14. Monitoramento e operação
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Job e tarefa | `ChuvaSegura - SIBH` (`jobs/chuvasegura_sibh.job.yml`), tarefa `sibh_stations_landing` |
# MAGIC | Frequência | Diária, 07:30 (America/Sao_Paulo); agenda pausada até o backfill |
# MAGIC | SLA | A definir |
# MAGIC | Métricas | Estações do estado e da capital, por instituição |
# MAGIC | Logs | `fiap_analytics.bronze.sibh_ingestion_log` (etapa `landing`) |
# MAGIC | Alertas | Não configurado (recomendação: notificação de falha no job) |
# MAGIC | Reprocessamento | Rodar de novo no mesmo dia |
# MAGIC
# MAGIC ## 15. Histórico de alterações
# MAGIC
# MAGIC | Versão | Data | Responsável | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | 1.0 | 2026-11-16 | juniorOnData | Criação do notebook (commit `2a2af58`, sem PR) |
# MAGIC | 1.1 | 2026-11-16 | juniorOnData | Log de ingestão próprio da origem, no formato padrão (PR #8) |
# MAGIC | 1.2 | 2026-11-16 | juniorOnData | Formato de código fonte, módulo de log compartilhado e novo nome do arquivo (PR #9) |
# MAGIC | 1.3 | 2026-11-16 | juniorOnData | Documentação no padrão oficial (PR #15) |

# COMMAND ----------

# [PY] E01 Parâmetros
dbutils.widgets.text("api_url", "https://cth.daee.sp.gov.br/sibh/api/v2/stations", "URL do cadastro de estações")
dbutils.widgets.text("codibge_check", "3550308", "Município usado só para conferência")

# COMMAND ----------

# [PY] E02 Configuração
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

# [PY] E03 Log de ingestão
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

# [PY] E04 Coleta e validação
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

# [PY] E05 Gravação
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
