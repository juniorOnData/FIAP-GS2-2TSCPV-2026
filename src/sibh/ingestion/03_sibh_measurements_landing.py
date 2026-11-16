# Databricks notebook source
# MAGIC %md
# MAGIC # SIB_LND_002 03_sibh_measurements_landing
# MAGIC
# MAGIC ## 01. Identificação
# MAGIC
# MAGIC | Campo | Valor |
# MAGIC |---|---|
# MAGIC | Identificador | SIB_LND_002 |
# MAGIC | Nome | 03_sibh_measurements_landing |
# MAGIC | Projeto | ChuvaSegura |
# MAGIC | Domínio de negócio | Hidrologia (SIBH) |
# MAGIC | Camada | Landing |
# MAGIC | Linguagem | Python |
# MAGIC | Responsável técnico | juniorOnData |
# MAGIC | Responsável funcional | A definir |
# MAGIC | Status | Homologação |
# MAGIC | Versão | 1.3 |
# MAGIC | Criticidade | Média |
# MAGIC | Caminho no repositório | `src/sibh/ingestion/03_sibh_measurements_landing.py` |
# MAGIC
# MAGIC ## 02. Objetivo
# MAGIC
# MAGIC Baixar as medições de chuva e nível dos sensores da capital (exceto CEMADEN) pela API do SIBH e gravar um arquivo por mês no volume, com manifesto que serve de checkpoint.
# MAGIC
# MAGIC ## 03. Contexto funcional
# MAGIC
# MAGIC Complementa a chuva do CEMADEN com outras redes e traz o nível de rios e córregos, essencial para explicar inundações.
# MAGIC
# MAGIC ## 04. Arquitetura e fluxo
# MAGIC
# MAGIC | Posição | Item |
# MAGIC |---|---|
# MAGIC | Etapa anterior | `02_sibh_stations_bronze` (cadastro na Bronze) |
# MAGIC | Etapa atual | Grava `ano=AAAA/mes=MM/` no volume `sibh_measurements` |
# MAGIC | Etapa posterior | `04_sibh_measurements_bronze` |
# MAGIC
# MAGIC ## 05. Dependências
# MAGIC
# MAGIC | Tipo | Item | Uso |
# MAGIC |---|---|---|
# MAGIC | Conexão | `https://cth.daee.sp.gov.br/sibh/api/v2/measurements` | Medições |
# MAGIC | Tabela | `fiap_analytics.bronze.sibh_stations` | Sensores da capital |
# MAGIC | Módulo | `src/common/ingestion_log.py` | Registro no log |
# MAGIC | Biblioteca | `requests` | Chamadas HTTP |
# MAGIC | Permissão | USE CATALOG em `fiap_analytics`; USE SCHEMA, CREATE VOLUME, CREATE TABLE e MODIFY em `fiap_analytics.bronze`; READ VOLUME e WRITE VOLUME no volume de landing; acesso de saída à internet no compute; SELECT em `fiap_analytics.bronze.sibh_stations` | Ler o cadastro, gravar arquivos e o log |
# MAGIC
# MAGIC ## 06. Parâmetros de entrada
# MAGIC
# MAGIC | Nome | Tipo | Obrigatório | Descrição | Valor padrão | Exemplo |
# MAGIC |---|---|---|---|---|---|
# MAGIC | `mode` | dropdown (texto) | Sim | `probe` testa a API sem gravar; `backfill` baixa os meses fechados; `incremental` baixa o mês atual e os pendentes | backfill | incremental |
# MAGIC | `group_type` | text (texto) | Não | Agrupamento da API; `minute` é a leitura original | minute | minute |
# MAGIC | `min_month` | text (AAAA-MM) | Sim | Primeiro mês da ingestão | 2020-01 | 2026-10 |
# MAGIC | `excluded_owners` | text (lista) | Não | Instituições excluídas, separadas por vírgula | CEMADEN | CEMADEN |
# MAGIC | `min_interval_s` | text (número) | Não | Intervalo mínimo entre chamadas, em segundos | 1.0 | 1.0 |
# MAGIC | `force` | dropdown (false/true) | Não | No backfill, baixa de novo meses já completos | false | true |
# MAGIC
# MAGIC ## 07. Fontes de dados
# MAGIC
# MAGIC | Fonte | Formato | Origem | Tipo de leitura | Frequência | Chave | Filtros |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | SIBH `api/v2/measurements` | JSON | SIBH, SP Águas | Por janela mensal, em lotes de até 10 sensores | Diária no incremental | `station_prefix_id` + `date` | Sensores da capital, tipos 1 e 2, fora os donos excluídos |
# MAGIC | `fiap_analytics.bronze.sibh_stations` | Delta | Bronze do SIBH | Snapshot mais recente | A cada execução | `id` | `cod_ibge = 3550308`, `station_type_id` 1 ou 2 |
# MAGIC
# MAGIC ## 08. Processamento técnico
# MAGIC
# MAGIC | Etapa | Linguagem | Célula | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | E01 | PY | Parâmetros | Widgets `mode`, `group_type`, `min_month`, `excluded_owners`, `min_interval_s` e `force`. |
# MAGIC | E02 | PY | Configuração | Imports, constantes, carência de 3 dias, validação de `min_month`, `BATCH_ID` e criação do volume (exceto no `probe`). |
# MAGIC | E03 | PY | Log de ingestão | Importa o `IngestionLog` e define `registrar` (etapa `landing`). |
# MAGIC | E04 | PY | Chamada à API | Limitador de chamadas, `get_measurements` (até 10 sensores por chamada, com retentativa) e divisão em lotes. |
# MAGIC | E05 | PY | Modo probe | Duas chamadas de teste; devolve a resposta na saída do notebook, sem gravar nada. |
# MAGIC | E06 | PY | Sensores e funções de mês | Sensores da capital no cadastro mais recente; funções de data, caminho, manifesto, mês completo e meses incompletos. |
# MAGIC | E07 | PY | Planejamento dos meses | Backfill (período de cada sensor com `group_type = all`) ou incremental (mês atual, anterior incompleto e meses incompletos). |
# MAGIC | E08 | PY | Download de um mês | Um arquivo gzip com todos os lotes do mês e manifesto gravado por último. |
# MAGIC | E09 | PY | Execução | Baixa os meses planejados (no backfill, pula os completos), registra `SUCESSO`, `PARCIAL`, `SEM_DADOS` ou `FALHA` e termina com erro se algum mês falhou. |
# MAGIC
# MAGIC ## 09. Regras de negócio
# MAGIC
# MAGIC | ID | Regra | Lógica | Comportamento esperado |
# MAGIC |---|---|---|---|
# MAGIC | RN_SIB_001 | Sensores monitorados | Cadastro mais recente, município 3550308, tipos 1 (nível) e 2 (chuva), fora as instituições de `excluded_owners` (CEMADEN, já coletado pela PED). | Sem sobreposição com a rede CEMADEN. |
# MAGIC | RN_SIB_002 | Mês completo | Um mês é completo se foi baixado pelo menos 3 dias depois de terminar (`CLOSING_GRACE`). | Antes disso fica `PARCIAL` e volta a ser baixado. |
# MAGIC | RN_SIB_003 | Meses e sensores do incremental | Mês atual, mês anterior incompleto e meses com manifesto incompleto; sensores em operação ou com medição no mês. | Nenhum mês fica incompleto para sempre. |
# MAGIC | RN_SIB_004 | Período do backfill | Meses fechados a partir de `min_month`, dentro do período de dados de cada sensor. | Não pede meses sem dados do sensor. |
# MAGIC
# MAGIC ## 10. Saídas de dados
# MAGIC
# MAGIC | Destino | Formato | Granularidade | Chave | Modo de escrita |
# MAGIC |---|---|---|---|---|
# MAGIC | Volume `sibh_measurements`: `ano=AAAA/mes=MM/measurements_AAAAMM.json.gz` e `.manifest.json` | JSON por linha (gzip) e manifesto JSON | Um arquivo por mês | `station_prefix_id` + `date` | Sobrescreve o arquivo do mês |
# MAGIC | Volume `sibh_measurements`: `_coverage/coverage_AAAAMMDD.json` | JSON | Um arquivo por backfill | `station_prefix_id` | Sobrescreve o arquivo do dia |
# MAGIC
# MAGIC As colunas de cada tabela estão no dicionário de dados (`docs/02_catalogos/dicionario_de_dados.csv`).
# MAGIC
# MAGIC ## 11. Qualidade dos dados
# MAGIC
# MAGIC O manifesto registra sensores pedidos e com dados, contagem e SHA256 de cada mês. A conferência de linhas é feita na Bronze.
# MAGIC
# MAGIC ## 12. Tratamento de erros
# MAGIC
# MAGIC | Situação | Comportamento | Registro | Recuperação |
# MAGIC |---|---|---|---|
# MAGIC | 429, 5xx ou erro de rede | Até 5 tentativas com espera crescente | Saída do notebook | Automática |
# MAGIC | Falha em um mês | O mês fica sem manifesto e o backfill continua | `FALHA` no `fiap_analytics.bronze.sibh_ingestion_log` | Rodar de novo: o mês é refeito |
# MAGIC
# MAGIC ## 13. Segurança e governança
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Classificação dos dados | Pública (API pública do SIBH, SP Águas; confirmar os termos de uso) |
# MAGIC | Dados sensíveis | Nenhum |
# MAGIC | Credenciais | Nenhuma (API pública, sem autenticação) |
# MAGIC | Permissões necessárias | USE CATALOG em `fiap_analytics`; USE SCHEMA, CREATE VOLUME, CREATE TABLE e MODIFY em `fiap_analytics.bronze`; READ VOLUME e WRITE VOLUME no volume de landing; acesso de saída à internet no compute; SELECT em `fiap_analytics.bronze.sibh_stations` |
# MAGIC | Políticas aplicáveis | Termos de uso dos dados do SIBH (confirmar) |
# MAGIC
# MAGIC ## 14. Monitoramento e operação
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Job e tarefa | `ChuvaSegura - SIBH` (`jobs/chuvasegura_sibh.job.yml`), tarefa `sibh_measurements_landing` |
# MAGIC | Frequência | Diária, 07:30 (America/Sao_Paulo); agenda pausada até o backfill |
# MAGIC | SLA | A definir |
# MAGIC | Métricas | Meses por status, registros, chamadas e tempo por mês |
# MAGIC | Logs | `fiap_analytics.bronze.sibh_ingestion_log` (etapa `landing`, janela = AAAA-MM) |
# MAGIC | Alertas | Não configurado (recomendação: notificação de falha no job) |
# MAGIC | Reprocessamento | Backfill com `force = true` e `min_month` no mês desejado |
# MAGIC
# MAGIC ## 15. Histórico de alterações
# MAGIC
# MAGIC | Versão | Data | Responsável | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | 1.0 | 2026-11-16 | juniorOnData | Criação do notebook (commit `2a2af58`, sem PR) |
# MAGIC | 1.1 | 2026-11-16 | juniorOnData | Log de ingestão próprio da origem e meses parciais (PR #8) |
# MAGIC | 1.2 | 2026-11-16 | juniorOnData | Formato de código fonte, módulo de log compartilhado e novo nome `03_sibh_measurements_landing` (PR #9) |
# MAGIC | 1.3 | 2026-11-16 | juniorOnData | Documentação no padrão oficial (PR #15) |

# COMMAND ----------

# [PY] E01 Parâmetros
dbutils.widgets.dropdown("mode", "backfill", ["probe", "backfill", "incremental"], "Modo")
dbutils.widgets.text("group_type", "minute", "group_type (minute = leitura original)")
dbutils.widgets.text("min_month", "2020-01", "Primeiro mês da ingestão (AAAA-MM). Nada antes disso é baixado")
dbutils.widgets.text("excluded_owners", "CEMADEN", "Donos excluídos, separados por vírgula")
dbutils.widgets.text("min_interval_s", "1.0", "Intervalo mínimo entre chamadas (s)")
dbutils.widgets.dropdown("force", "false", ["false", "true"], "Rebaixar meses que já têm manifesto")

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
from datetime import datetime, timedelta, timezone

import requests
from pyspark.sql import functions as F

CATALOG = "fiap_analytics"
SCHEMA = "bronze"
VOLUME = "sibh_measurements"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
STATIONS_TABLE = f"{CATALOG}.{SCHEMA}.sibh_stations"
DATASET = "sibh_measurements"

API_URL = "https://cth.daee.sp.gov.br/sibh/api/v2/measurements"
CODIBGE = "3550308"
BATCH_SIZE = 10
MAX_ATTEMPTS = 5
TIMEOUT_S = 300
HEADERS = {"Accept": "application/json", "User-Agent": "ChuvaSegura-FIAP/1.0 (projeto academico)"}
CLOSING_GRACE = timedelta(days=3)  # o mês só é dado como completo se foi baixado 3 dias depois de terminar

MODE = dbutils.widgets.get("mode")
GROUP_TYPE = dbutils.widgets.get("group_type").strip() or "minute"
MIN_MONTH = dbutils.widgets.get("min_month").strip()
if len(MIN_MONTH) != 7 or MIN_MONTH[4] != "-":
    raise ValueError(f"min_month deve estar no formato AAAA-MM, veio {MIN_MONTH!r}")
EXCLUDED_OWNERS = [o.strip() for o in dbutils.widgets.get("excluded_owners").split(",") if o.strip()]
MIN_INTERVAL_S = float(dbutils.widgets.get("min_interval_s"))
FORCE = dbutils.widgets.get("force") == "true"

BATCH_ID = str(uuid.uuid4())
NOW = datetime.now(timezone.utc)

if MODE != "probe":
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME} "
        "COMMENT 'Landing das medições do SIBH/SP Águas (api/v2/measurements), sensores de chuva e nível da capital. JSON por linha, gzip, um arquivo por mês (UTC).'"
    )
print(f"batch_id={BATCH_ID} | mode={MODE} | group_type={GROUP_TYPE} | a partir de {MIN_MONTH} | excluídos={EXCLUDED_OWNERS}")

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
FONTE = "sibh_measurements"
ETAPA = "landing"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chamada à API

# COMMAND ----------

# [PY] E04 Chamada à API
_last_call_start = [0.0]
API_CALLS = [0]


def throttle() -> None:
    elapsed = time.monotonic() - _last_call_start[0]
    if elapsed < MIN_INTERVAL_S:
        time.sleep(MIN_INTERVAL_S - elapsed)
    _last_call_start[0] = time.monotonic()


def fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M")


def get_measurements(sensor_ids: list, start: datetime, end: datetime, group_type: str) -> list:
    """Uma chamada para até 10 sensores. Devolve a lista de registros como a API mandou."""
    if not 0 < len(sensor_ids) <= BATCH_SIZE:
        raise ValueError(f"Lote com {len(sensor_ids)} sensores (máximo {BATCH_SIZE})")
    params = [("station_prefix_ids[]", str(s)) for s in sensor_ids] + [
        ("start_date", fmt(start)),
        ("end_date", fmt(end)),
        ("group_type", group_type),
        ("serializer", "default"),
        ("format", "json"),
    ]
    for attempt in range(1, MAX_ATTEMPTS + 1):
        throttle()
        API_CALLS[0] += 1
        wait = min(120, 5 * 2 ** (attempt - 1))
        try:
            resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=TIMEOUT_S)
        except (requests.ConnectionError, requests.Timeout) as exc:
            print(f"    erro de rede (tentativa {attempt}/{MAX_ATTEMPTS}): {type(exc).__name__}; nova tentativa em {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code == 429 or resp.status_code >= 500:
            print(f"    API respondeu {resp.status_code} (tentativa {attempt}/{MAX_ATTEMPTS}); nova tentativa em {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code != 200:
            raise RuntimeError(f"API respondeu {resp.status_code}: {resp.text[:500]}")
        body = json.loads(resp.content.decode("utf-8"))
        if isinstance(body, list):
            return body
        if isinstance(body, dict) and isinstance(body.get("measurements"), list):
            return body["measurements"]
        raise RuntimeError(f"Estrutura inesperada: {str(body)[:300]}")
    raise RuntimeError(f"Falha após {MAX_ATTEMPTS} tentativas para os sensores {sensor_ids}")


def batches(items: list, size: int = BATCH_SIZE):
    for i in range(0, len(items), size):
        yield items[i:i + size]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modo probe: confirma a resposta real antes de qualquer carga

# COMMAND ----------

# [PY] E05 Modo probe
if MODE == "probe":
    test_ids = ["35416", "35417", "33665", "33664", "174", "29077"]  # Cocaia chuva/nível, Jd. Brasília chuva/nível, Mirante de Santana, E3-007
    probe = {}

    # 1) Um dia de leituras originais (group_type=minute) do Córrego Cocaia, chuva (35416) e nível (35417)
    day_end = NOW.replace(hour=0, minute=0, second=0, microsecond=0)
    day_start = datetime.fromtimestamp(day_end.timestamp() - 86400, timezone.utc)
    sample = get_measurements(["35416", "35417"], day_start, day_end, "minute")
    probe["1_minute_cocaia"] = {
        "periodo_utc": f"{fmt(day_start)} a {fmt(day_end)}",
        "registros": len(sample),
        "registros_por_sensor": {sid: sum(1 for r in sample if str(r.get("station_prefix_id")) == sid) for sid in ("35416", "35417")},
        "campos": sorted({k for r in sample for k in r}),
        "primeiros_chuva": [r for r in sample if str(r.get("station_prefix_id")) == "35416"][:3],
        "primeiros_nivel": [r for r in sample if str(r.get("station_prefix_id")) == "35417"][:3],
    }

    # 2) Período disponível de cada sensor de teste (group_type=all)
    coverage = get_measurements(test_ids, datetime(1900, 1, 1, tzinfo=timezone.utc), NOW, "all")
    probe["2_all_periodo"] = {
        "registros": coverage,
        "sem_registro": sorted(set(test_ids) - {str(r.get("station_prefix_id")) for r in coverage}),
    }

    probe_json = json.dumps(probe, ensure_ascii=False, indent=2, default=str)
    print(probe_json)
    # O resultado vai na própria mensagem de saída, para aparecer mesmo quando o Databricks esconde os prints.
    dbutils.notebook.exit(probe_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sensores da capital

# COMMAND ----------

# [PY] E06 Sensores e funções de mês
latest_snapshot = spark.table(STATIONS_TABLE).agg(F.max("_snapshot_date")).first()[0]
sensors_df = (
    spark.table(STATIONS_TABLE)
    .filter(F.col("_snapshot_date") == F.lit(latest_snapshot))
    .filter(F.col("cod_ibge") == CODIBGE)
    .filter(F.col("station_type_id").isin("1", "2"))
    .filter(~F.col("station_owner").isin(EXCLUDED_OWNERS))
    .select("id", "station_type_id", "station_owner", "operation_status", "date_last_measurement", "prefix", "name")
    .dropDuplicates(["id"])
)
sensors = [r.asDict() for r in sensors_df.orderBy("id").collect()]
print(f"Cadastro de {latest_snapshot}: {len(sensors)} sensores (chuva e nível) na capital")


def parse_last_measurement(text):
    """'Wed Nov 11 2026 16:00:00 GMT+0000 (...)' -> datetime UTC, ou None."""
    if not text:
        return None
    try:
        return datetime.strptime(text[:24], "%a %b %d %Y %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def month_start(year: int, month: int) -> datetime:
    return datetime(year, month, 1, tzinfo=timezone.utc)


def next_month(dt: datetime) -> datetime:
    return month_start(dt.year + (dt.month == 12), 1 if dt.month == 12 else dt.month + 1)


def to_month_key(text):
    """Aceita 'AAAA/MM/DD HH:mm', 'AAAA-MM-DD...' ou 'AAAA-MM' e devolve 'AAAA-MM'."""
    if not text:
        return None
    t = str(text).replace("/", "-")
    return t[:7] if len(t) >= 7 and t[4] == "-" else None


def month_paths(key: str):
    y, m = key.split("-")
    folder = f"{VOLUME_PATH}/ano={y}/mes={m}"
    return folder, f"{folder}/measurements_{y}{m}.json.gz", f"{folder}/measurements_{y}{m}.manifest.json"


def read_manifest(key: str):
    """Manifesto do mês 'AAAA-MM', ou None se o mês ainda não foi baixado."""
    manifest_path = month_paths(key)[2]
    if not os.path.exists(manifest_path):
        return None
    with open(manifest_path, encoding="utf-8") as fh:
        return json.load(fh)


def is_complete(manifest: dict) -> bool:
    """O mês está completo se foi baixado depois do fim do mês mais a carência (leituras atrasadas)."""
    y, m = map(int, manifest["window"].split("-"))
    collected_at = datetime.fromisoformat(manifest["collected_at_utc"])
    return collected_at >= next_month(month_start(y, m)) + CLOSING_GRACE


def incomplete_months() -> list:
    """Meses já baixados cujo manifesto ainda não está completo (baixados antes de fechar)."""
    keys = []
    if not os.path.isdir(VOLUME_PATH):
        return keys
    for year_dir in sorted(d for d in os.listdir(VOLUME_PATH) if d.startswith("ano=")):
        for month_dir in sorted(d for d in os.listdir(f"{VOLUME_PATH}/{year_dir}") if d.startswith("mes=")):
            key = f"{year_dir[4:]}-{month_dir[4:]}"
            manifest = read_manifest(key)
            if manifest is not None and not is_complete(manifest):
                keys.append(key)
    return keys

# COMMAND ----------

# MAGIC %md
# MAGIC ## Planejamento dos meses

# COMMAND ----------

# [PY] E07 Planejamento dos meses
current_month = month_start(NOW.year, NOW.month)

if MODE == "backfill":
    # Período de cada sensor com group_type=all (uma chamada por lote de 10).
    coverage = {}      # id -> (primeiro mês, último mês, qtd)
    answered = set()   # sensores de lotes em que a API respondeu
    for lote in batches([s["id"] for s in sensors]):
        try:
            rows = get_measurements(lote, datetime(1900, 1, 1, tzinfo=timezone.utc), NOW, "all")
            answered.update(lote)
            for r in rows:
                coverage[str(r.get("station_prefix_id"))] = (to_month_key(r.get("min_date")), to_month_key(r.get("max_date")), r.get("qtd"))
        except Exception as exc:
            print(f"Aviso: período não obtido para {lote}: {exc}. Esses sensores começam em min_month.")

    os.makedirs(f"{VOLUME_PATH}/_coverage", exist_ok=True)
    with open(f"{VOLUME_PATH}/_coverage/coverage_{NOW:%Y%m%d}.json", "w", encoding="utf-8") as fh:
        json.dump({"collected_at_utc": NOW.isoformat(), "coverage": coverage,
                   "answered_without_data": sorted(answered - set(coverage))}, fh, ensure_ascii=False, indent=2)

    plan = {}  # 'AAAA-MM' -> lista de sensores
    no_data, fallback, before_min = [], [], []
    for s in sensors:
        first, last, _ = coverage.get(s["id"], (None, None, None))
        if first is None:
            if s["id"] in answered:
                no_data.append(s["id"])  # a API respondeu e o sensor não tem medições
                continue
            fallback.append(s["id"])
            first = MIN_MONTH
        first = max(first, MIN_MONTH)  # limite inferior da ingestão
        last = last or f"{NOW:%Y-%m}"
        if last < MIN_MONTH:
            before_min.append(s["id"])  # sensor só tem dados antes do limite
            continue
        y, m = map(int, first.split("-"))
        cursor = month_start(y, m)
        while cursor < current_month and f"{cursor:%Y-%m}" <= last:  # só meses fechados
            plan.setdefault(f"{cursor:%Y-%m}", []).append(s["id"])
            cursor = next_month(cursor)

    print(f"Sensores com período descoberto: {len([s for s in sensors if coverage.get(s['id'], (None,))[0]])} | "
          f"sem medições na API: {len(no_data)} | só com dados antes de {MIN_MONTH}: {len(before_min)} | "
          f"sem resposta, começando em {MIN_MONTH}: {len(fallback)}")

else:  # incremental
    # Mês atual, mês anterior enquanto não estiver completo e meses baixados antes de fechar
    # que ainda não foram completados (por exemplo, se o job ficou dias sem rodar).
    prev = month_start(current_month.year - (current_month.month == 1), 12 if current_month.month == 1 else current_month.month - 1)
    windows = {current_month}
    prev_manifest = read_manifest(f"{prev:%Y-%m}")
    if prev_manifest is None or not is_complete(prev_manifest):
        windows.add(prev)
    windows.update(month_start(*map(int, key.split("-"))) for key in incomplete_months())
    plan = {}
    for w in sorted(windows):
        ids = []
        for s in sensors:
            last = parse_last_measurement(s["date_last_measurement"])
            if s["operation_status"] == "1" or (last is not None and last >= w):
                ids.append(s["id"])
        plan[f"{w:%Y-%m}"] = ids

months = sorted(plan)
print(f"Meses planejados: {len(months)} ({months[0] if months else '-'} a {months[-1] if months else '-'})")
print(f"Chamadas estimadas: {sum((len(v) + BATCH_SIZE - 1) // BATCH_SIZE for v in plan.values())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download por mês

# COMMAND ----------

# [PY] E08 Download de um mês
def download_month(key: str, sensor_ids: list) -> dict:
    y, m = map(int, key.split("-"))
    start = month_start(y, m)
    # Janela do primeiro minuto ao último minuto do mês em UTC: meses vizinhos não se sobrepõem.
    end = min(datetime.fromtimestamp(next_month(start).timestamp() - 60, timezone.utc), NOW)
    folder, data_path, manifest_path = month_paths(key)
    calls_before = API_CALLS[0]
    records, sensors_with_data, fields = 0, set(), set()

    with tempfile.TemporaryDirectory() as tmp:
        tmp_file = os.path.join(tmp, os.path.basename(data_path))
        with gzip.open(tmp_file, "wt", encoding="utf-8") as fh:
            for lote in batches(sensor_ids):
                rows = get_measurements(lote, start, end, GROUP_TYPE)
                for r in rows:
                    fh.write(json.dumps(r, ensure_ascii=False) + "\n")
                    sensors_with_data.add(str(r.get("station_prefix_id")))
                    fields.update(r.keys())
                records += len(rows)
        with open(tmp_file, "rb") as fh:
            sha256 = hashlib.sha256(fh.read()).hexdigest()
        size_bytes = os.path.getsize(tmp_file)
        os.makedirs(folder, exist_ok=True)
        if os.path.exists(manifest_path):
            os.remove(manifest_path)  # sem manifesto enquanto o arquivo é trocado
        shutil.copyfile(tmp_file, data_path)

    manifest = {
        "dataset": DATASET,
        "window": key,
        "file_name": os.path.basename(data_path),
        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_url": API_URL,
        "params": {"start_date": fmt(start), "end_date": fmt(end), "group_type": GROUP_TYPE, "serializer": "default", "format": "json"},
        "sensors_requested": sensor_ids,
        "sensors_with_data": sorted(sensors_with_data),
        "record_count": records,
        "fields": sorted(fields),
        "api_calls": API_CALLS[0] - calls_before,
        "sha256": sha256,
        "size_bytes": size_bytes,
        "landing_batch_id": BATCH_ID,
    }
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    return manifest

# COMMAND ----------

# [PY] E09 Execução
failed, done_secs = [], []
run_started = datetime.now(timezone.utc)
for key in months:
    _, data_path, _ = month_paths(key)
    if MODE == "backfill" and not FORCE:
        existing = read_manifest(key)
        if existing is not None and is_complete(existing):
            continue
    started = datetime.now(timezone.utc)
    try:
        man = download_month(key, plan[key])
        if not is_complete(man):
            status = "PARCIAL"
        else:
            status = "SUCESSO" if man["record_count"] else "SEM_DADOS"
        registrar(key, status, started, registros=man["record_count"], paginas=man["api_calls"],
                  arquivo=data_path, sha256=man["sha256"],
                  mensagem=f"sensores_pedidos={len(plan[key])}; sensores_com_dados={len(man['sensors_with_data'])}")
        secs = (datetime.now(timezone.utc) - started).total_seconds()
        done_secs.append(secs)
        print(f"{key}: {status} | {man['record_count']} registros | {len(man['sensors_with_data'])}/{len(plan[key])} sensores com dados | "
              f"{man['api_calls']} chamadas | {secs:.0f}s")
    except Exception as exc:
        failed.append(key)
        registrar(key, "FALHA", started, mensagem=str(exc))
        print(f"{key}: FALHOU ({exc})")

elapsed = (datetime.now(timezone.utc) - run_started).total_seconds()
print(f"Total de chamadas: {API_CALLS[0]} | meses baixados: {len(done_secs)} | tempo total: {elapsed / 60:.1f} min")
if done_secs:
    avg = sum(done_secs) / len(done_secs)
    print(f"Média por mês: {avg:.0f}s. Estimativa para 82 meses (2020-01 a 2026-10): {avg * 82 / 3600:.1f} h")
if failed:
    raise RuntimeError(f"{len(failed)} meses falharam e serão refeitos na próxima execução: {failed}")
