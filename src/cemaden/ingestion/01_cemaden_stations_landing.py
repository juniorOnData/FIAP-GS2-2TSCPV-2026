# Databricks notebook source
# MAGIC %md
# MAGIC # 01_cemaden_stations_landing
# MAGIC
# MAGIC Baixa o cadastro de estações do CEMADEN no município (API PED `/pcds-cadastro/dados-cadastrais`) e grava no volume `fiap_analytics.bronze.cemaden_stations`.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Token da SGAA com as credenciais do secret scope `cemaden` (`ped_email` e `ped_password`).
# MAGIC 2. Uma coleta por dia em `data_coleta=AAAA-MM-DD/`, em JSON por linha comprimido (gzip). Rodar de novo no mesmo dia sobrescreve o arquivo do dia.
# MAGIC 3. Manifesto gravado por último (contagem, SHA256, campos), usado na conferência da Bronze.
# MAGIC 4. Uma linha no `cemaden_ingestion_log` (etapa `landing`).

# COMMAND ----------

dbutils.widgets.text("codibge", "3550308", "Código IBGE do município")
dbutils.widgets.text("tipoestacao", "", "Tipo de estação (vazio = todos)")
dbutils.widgets.text("formato", "JSON", "Parâmetro formato (vazio = não enviar)")

# COMMAND ----------

import base64
import gzip
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone

import requests

CATALOG = "fiap_analytics"
SCHEMA = "bronze"
VOLUME = "cemaden_stations"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
DATASET = "cemaden_stations"

SGAA_TOKEN_URL = "https://sgaa.cemaden.gov.br/SGAA/rest/controle-token/tokens"
PED_BASE_URL = "https://sws.cemaden.gov.br/PED/rest"
ENDPOINT = "/pcds-cadastro/dados-cadastrais"
PAGINATION_ENDPOINT = "/controle-paginacao/pagina"

SECRET_SCOPE = "cemaden"
MIN_CALL_INTERVAL_S = 5.5
MIN_TOKEN_VALIDITY_S = 15 * 60
MAX_ATTEMPTS = 5
MAX_PAGES = 500
TIMEOUT_S = 60

CODIBGE = dbutils.widgets.get("codibge").strip()
TIPOESTACAO = dbutils.widgets.get("tipoestacao").strip()
FORMATO = dbutils.widgets.get("formato").strip()

BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc)

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME} "
    "COMMENT 'Landing do cadastro de estações do CEMADEN (PED /pcds-cadastro/dados-cadastrais). JSON por linha, gzip, um arquivo por coleta diária.'"
)

print(f"batch_id={BATCH_ID} | codibge={CODIBGE} | tipoestacao={TIPOESTACAO or '(todos)'} | formato={FORMATO or '(não enviado)'}")

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

LOG_TABLE = "fiap_analytics.bronze.cemaden_ingestion_log"
FONTE = "cemaden_stations"
ETAPA = "landing"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Token SGAA, limitador e chamadas à PED

# COMMAND ----------

class AuthError(Exception):
    """PED respondeu 401 ou 403."""


class TokenManager:
    def __init__(self):
        self._token = None
        self._exp = 0.0

    @staticmethod
    def _jwt_exp(token: str) -> float:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return float(json.loads(base64.urlsafe_b64decode(payload))["exp"])

    def _request_token(self):
        body = {
            "email": dbutils.secrets.get(SECRET_SCOPE, "ped_email"),
            "password": dbutils.secrets.get(SECRET_SCOPE, "ped_password"),
        }
        resp = requests.post(SGAA_TOKEN_URL, json=body, timeout=TIMEOUT_S)
        if resp.status_code not in (200, 201):
            # Não imprime o corpo da resposta para não arriscar vazar dados da conta.
            raise RuntimeError(f"SGAA respondeu {resp.status_code} ao gerar o token")
        token = resp.json()["token"]
        return token, self._jwt_exp(token)

    def get(self, min_validity: float = MIN_TOKEN_VALIDITY_S) -> str:
        if self._token and self._exp - time.time() >= min_validity:
            return self._token

        token, exp = self._request_token()
        remaining = exp - time.time()
        if remaining < min_validity:
            # A SGAA devolve o mesmo token enquanto ele estiver ativo: espera expirar e pede outro.
            wait = max(0.0, remaining) + 5
            print(f"Token ativo com {remaining / 60:.1f} min de validade. Aguardando {wait / 60:.1f} min para pedir um novo.")
            time.sleep(wait)
            token, exp = self._request_token()
            if exp - time.time() < min_validity:
                raise RuntimeError("A SGAA não devolveu um token novo depois da expiração")

        self._token, self._exp = token, exp
        print(f"Token válido por mais {(self._exp - time.time()) / 60:.0f} min")
        return self._token

    def invalidate(self) -> None:
        self._token, self._exp = None, 0.0


tokens = TokenManager()
_last_call_start = [0.0]


def throttle() -> None:
    elapsed = time.monotonic() - _last_call_start[0]
    if elapsed < MIN_CALL_INTERVAL_S:
        time.sleep(MIN_CALL_INTERVAL_S - elapsed)
    _last_call_start[0] = time.monotonic()


def ped_get(path: str, params: dict) -> requests.Response:
    url = PED_BASE_URL + path
    for attempt in range(1, MAX_ATTEMPTS + 1):
        throttle()
        wait = min(60, 5 * 2 ** (attempt - 1))
        try:
            resp = requests.get(url, params=params, headers={"token": tokens.get(min_validity=60)}, timeout=TIMEOUT_S)
        except (requests.ConnectionError, requests.Timeout) as exc:
            print(f"Erro de rede em {path} (tentativa {attempt}/{MAX_ATTEMPTS}): {type(exc).__name__}. Nova tentativa em {wait}s")
            time.sleep(wait)
            continue

        if resp.status_code in (401, 403):
            raise AuthError(resp.status_code)
        if resp.status_code == 429 or resp.status_code >= 500:
            print(f"{path} respondeu {resp.status_code} (tentativa {attempt}/{MAX_ATTEMPTS}). Nova tentativa em {wait}s")
            time.sleep(wait)
            continue
        return resp

    raise RuntimeError(f"{path} falhou após {MAX_ATTEMPTS} tentativas")


def parse_body(resp: requests.Response):
    """Devolve (registros, scroll). Aceita lista pura ou objeto com lista em 'data'."""
    if resp.status_code == 202:
        return [], None
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"PED respondeu {resp.status_code}: {resp.text[:300]}")

    try:
        text = resp.content.decode("utf-8")
    except UnicodeDecodeError:
        text = resp.content.decode("latin-1")

    try:
        body = json.loads(text)
    except json.JSONDecodeError:
        raise RuntimeError(
            "A resposta não veio em JSON. Teste o widget 'formato' vazio ou com outro valor "
            f"(ex.: json). Início da resposta: {text[:200]!r}"
        )

    if isinstance(body, list):
        return body, None
    if isinstance(body, dict):
        for key in ("data", "dados", "estacoes", "items"):
            if isinstance(body.get(key), list):
                return body[key], body.get("scroll")
    raise RuntimeError(f"Estrutura de resposta inesperada. Chaves: {list(body)[:20] if isinstance(body, dict) else type(body)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coleta

# COMMAND ----------

def collect():
    params = {"codibge": CODIBGE}
    if TIPOESTACAO:
        params["tipoestacao"] = TIPOESTACAO
    if FORMATO:
        params["formato"] = FORMATO

    for auth_try in (1, 2):
        try:
            tokens.get()  # garante 15 min de validade no início da coleta
            calls = 0
            resp = ped_get(ENDPOINT, params)
            calls += 1
            page, scroll = parse_body(resp)
            records = list(page)

            # O cadastro de um município cabe em uma resposta, mas se a API paginar segue o mesmo fluxo das leituras.
            while scroll and page:
                if calls >= MAX_PAGES:
                    raise RuntimeError(f"Mais de {MAX_PAGES} páginas: interrompido por segurança")
                resp = ped_get(PAGINATION_ENDPOINT, {"scroll": scroll})
                calls += 1
                page, new_scroll = parse_body(resp)
                records.extend(page)
                scroll = new_scroll or scroll

            return records, calls, params
        except AuthError as exc:
            if auth_try == 2:
                raise RuntimeError(f"PED respondeu {exc} mesmo após renovar o token")
            print(f"PED respondeu {exc}. Renovando o token e reiniciando a coleta.")
            tokens.invalidate()


def validate(records: list) -> dict:
    if not records:
        raise RuntimeError("A PED devolveu o cadastro vazio. Confira o codibge e o parâmetro formato.")
    not_dict = sum(1 for r in records if not isinstance(r, dict))
    if not_dict:
        raise RuntimeError(f"{not_dict} registros não são objetos JSON")

    codes = [r.get("codestacao") for r in records]
    missing_code = sum(1 for c in codes if not c)
    if missing_code:
        raise RuntimeError(f"{missing_code} registros sem 'codestacao'. Campos do primeiro registro: {sorted(records[0])}")

    distinct = len(set(codes))
    outside = sum(1 for c in codes if not str(c).startswith(CODIBGE))
    keys = sorted({k for r in records for k in r})
    return {"records": len(records), "distinct_codestacao": distinct, "duplicated": len(codes) - distinct,
            "outside_municipality": outside, "fields": keys}

# COMMAND ----------

try:
    records, api_calls, params = collect()
    stats = validate(records)

    collected_at = datetime.now(timezone.utc)
    snapshot_date = collected_at.strftime("%Y-%m-%d")
    base_name = f"stations_{CODIBGE}_{collected_at:%Y%m%d}"
    target_dir = f"{VOLUME_PATH}/data_coleta={snapshot_date}"
    data_path = f"{target_dir}/{base_name}.json.gz"
    manifest_path = f"{target_dir}/{base_name}.manifest.json"
    os.makedirs(target_dir, exist_ok=True)

    # Escreve em disco local e copia para o volume: evita arquivo parcial no volume se algo falhar no meio.
    with tempfile.TemporaryDirectory() as tmp:
        tmp_file = os.path.join(tmp, f"{base_name}.json.gz")
        with gzip.open(tmp_file, "wt", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        with open(tmp_file, "rb") as fh:
            sha256 = hashlib.sha256(fh.read()).hexdigest()
        size_bytes = os.path.getsize(tmp_file)
        shutil.copyfile(tmp_file, data_path)

    # O manifesto é gravado por último: a Bronze só carrega arquivos que têm manifesto.
    manifest = {
        "dataset": DATASET,
        "file_name": f"{base_name}.json.gz",
        "snapshot_date": snapshot_date,
        "collected_at_utc": collected_at.isoformat(),
        "source_url": PED_BASE_URL + ENDPOINT,
        "params": params,
        "record_count": stats["records"],
        "distinct_codestacao": stats["distinct_codestacao"],
        "fields": stats["fields"],
        "api_calls": api_calls,
        "sha256": sha256,
        "size_bytes": size_bytes,
        "landing_batch_id": BATCH_ID,
    }
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)

    message = (f"distintas={stats['distinct_codestacao']}; duplicadas={stats['duplicated']}; "
               f"fora_do_municipio={stats['outside_municipality']}")
    registrar(snapshot_date, "SUCESSO", STARTED_AT, registros=stats["records"], paginas=api_calls,
              duplicados=stats["duplicated"], arquivo=data_path, sha256=sha256, mensagem=message)

    print(f"OK: {stats['records']} estações gravadas em {data_path}")
    print(message)
    print(f"Campos recebidos: {stats['fields']}")
    if stats["duplicated"]:
        print("Aviso: há codestacao repetido no cadastro. A deduplicação fica para a Silver.")
    if stats["outside_municipality"]:
        print("Aviso: há estações cujo código não começa com o IBGE do município. Conferir na Silver.")

except Exception as exc:
    registrar(datetime.now(timezone.utc).strftime("%Y-%m-%d"), "FALHA", STARTED_AT, mensagem=str(exc))
    raise

# COMMAND ----------

# Repassa a data da coleta para a tarefa da Bronze quando rodar dentro de um Job.
try:
    dbutils.jobs.taskValues.set(key="snapshot_date", value=snapshot_date)
except Exception:
    pass
