# Databricks notebook source
# MAGIC %md
# MAGIC # 03_sibh_measurements_landing
# MAGIC
# MAGIC Baixa as medições de chuva e nível do SIBH (SP Águas) para os sensores da capital e grava no volume `fiap_analytics.bronze.sibh_measurements`.
# MAGIC
# MAGIC Fonte: `GET https://cth.daee.sp.gov.br/sibh/api/v2/measurements` (pública, sem autenticação).
# MAGIC
# MAGIC Parâmetros usados: `station_prefix_ids[]` (até 10 ids de sensor por chamada), `start_date` e `end_date` em UTC no formato `AAAA-MM-DDTHH:mm`, `group_type`, `serializer=default`, `format=json`.
# MAGIC
# MAGIC Modos (widget `mode`):
# MAGIC
# MAGIC 1. `probe`: faz 2 chamadas de teste e devolve a resposta real na mensagem de saída, sem gravar nada (validado em 11/11/2026).
# MAGIC 2. `backfill` (padrão): descobre o período de cada sensor com `group_type=all` e baixa os meses fechados a partir de `min_month` (padrão 2020-01), pulando os que já estão completos. Para um teste rápido, use `min_month = 2026-10`.
# MAGIC 3. `incremental`: rebaixa, dos sensores ativos, o mês atual, o mês anterior enquanto ele não estiver completo e qualquer mês que tenha sido baixado antes de fechar e ainda não foi completado.
# MAGIC
# MAGIC Regras (mesmo padrão das leituras do CEMADEN):
# MAGIC
# MAGIC 1. Sensores: cadastro mais recente da `fiap_analytics.bronze.sibh_stations`, capital (`cod_ibge = 3550308`), chuva e nível (`station_type_id` 1 e 2), sem CEMADEN (já ingerido pela PED).
# MAGIC 2. Janelas mensais em UTC. Um arquivo por mês com todos os lotes de 10 sensores, em JSON por linha comprimido (gzip), registros exatamente como a API devolve.
# MAGIC 3. Idempotência por mês: o arquivo do mês é sobrescrito. O manifesto é gravado por último e serve de checkpoint.
# MAGIC 4. Mês completo: só quando foi baixado pelo menos `CLOSING_GRACE` (3 dias) depois de terminar, para pegar leituras que chegam atrasadas. Antes disso o mês entra como `PARCIAL` no log e volta a ser baixado pelo incremental ou pelo backfill.
# MAGIC 5. Retry com backoff para 429, 5xx e erros de rede. Intervalo mínimo entre chamadas configurável.
# MAGIC 6. Falha em um mês não interrompe o backfill: o mês fica sem manifesto, entra como `FALHA` no log e é refeito na próxima execução.

# COMMAND ----------

dbutils.widgets.dropdown("mode", "backfill", ["probe", "backfill", "incremental"], "Modo")
dbutils.widgets.text("group_type", "minute", "group_type (minute = leitura original)")
dbutils.widgets.text("min_month", "2020-01", "Primeiro mês da ingestão (AAAA-MM). Nada antes disso é baixado")
dbutils.widgets.text("excluded_owners", "CEMADEN", "Donos excluídos, separados por vírgula")
dbutils.widgets.text("min_interval_s", "1.0", "Intervalo mínimo entre chamadas (s)")
dbutils.widgets.dropdown("force", "false", ["false", "true"], "Rebaixar meses que já têm manifesto")

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
