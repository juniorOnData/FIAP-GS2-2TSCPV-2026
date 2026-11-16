# Databricks notebook source
# MAGIC %md
# MAGIC # CEM_LND_001 01_cemaden_stations_landing
# MAGIC
# MAGIC ## 01. Identificação
# MAGIC
# MAGIC | Campo | Valor |
# MAGIC |---|---|
# MAGIC | Identificador | CEM_LND_001 |
# MAGIC | Nome | 01_cemaden_stations_landing |
# MAGIC | Projeto | ChuvaSegura |
# MAGIC | Domínio de negócio | Pluviometria (CEMADEN) |
# MAGIC | Camada | Landing |
# MAGIC | Linguagem | Python |
# MAGIC | Responsável técnico | juniorOnData |
# MAGIC | Responsável funcional | A definir |
# MAGIC | Status | Homologação |
# MAGIC | Versão | 1.3 |
# MAGIC | Criticidade | Média |
# MAGIC | Caminho no repositório | `src/cemaden/ingestion/01_cemaden_stations_landing.py` |
# MAGIC
# MAGIC ## 02. Objetivo
# MAGIC
# MAGIC Baixar o cadastro de estações do CEMADEN do município pela API PED e gravar um arquivo por coleta diária no volume, com manifesto de controle.
# MAGIC
# MAGIC ## 03. Contexto funcional
# MAGIC
# MAGIC O cadastro identifica e localiza as estações pluviométricas. É necessário para posicionar as leituras de chuva e cruzá-las com as ocorrências.
# MAGIC
# MAGIC ## 04. Arquitetura e fluxo
# MAGIC
# MAGIC | Posição | Item |
# MAGIC |---|---|
# MAGIC | Etapa anterior | Nenhuma |
# MAGIC | Etapa atual | Grava `data_coleta=AAAA-MM-DD/` no volume `cemaden_stations` |
# MAGIC | Etapa posterior | `02_cemaden_stations_bronze` |
# MAGIC
# MAGIC ## 05. Dependências
# MAGIC
# MAGIC | Tipo | Item | Uso |
# MAGIC |---|---|---|
# MAGIC | Conexão | SGAA `https://sgaa.cemaden.gov.br/SGAA/rest/controle-token/tokens` e PED `https://sws.cemaden.gov.br/PED/rest` | Token e cadastro de estações |
# MAGIC | Secret | Scope `cemaden`, chaves `ped_email` e `ped_password` | Credenciais da conta PED |
# MAGIC | Módulo | `src/common/ingestion_log.py` | Registro no `cemaden_ingestion_log` |
# MAGIC | Biblioteca | `requests` | Chamadas HTTP |
# MAGIC | Permissão | USE CATALOG em `fiap_analytics`; USE SCHEMA, CREATE VOLUME, CREATE TABLE e MODIFY em `fiap_analytics.bronze`; READ VOLUME e WRITE VOLUME no volume de landing; acesso de saída à internet no compute | Criar o volume, gravar arquivos e o log |
# MAGIC
# MAGIC ## 06. Parâmetros de entrada
# MAGIC
# MAGIC | Nome | Tipo | Obrigatório | Descrição | Valor padrão | Exemplo |
# MAGIC |---|---|---|---|---|---|
# MAGIC | `codibge` | text (texto) | Sim | Código IBGE do município | 3550308 | 3550308 |
# MAGIC | `tipoestacao` | text (texto) | Não | Tipo de estação; vazio traz todos | (vazio) | (vazio) |
# MAGIC | `formato` | text (texto) | Não | Parâmetro `formato` da API; vazio não envia | JSON | JSON |
# MAGIC
# MAGIC ## 07. Fontes de dados
# MAGIC
# MAGIC | Fonte | Formato | Origem | Tipo de leitura | Frequência | Chave | Filtros |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | PED `/pcds-cadastro/dados-cadastrais` | JSON | CEMADEN | Completa (snapshot) | Diária | `codestacao` | `codibge` e, se informado, `tipoestacao` |
# MAGIC
# MAGIC ## 08. Processamento técnico
# MAGIC
# MAGIC | Etapa | Linguagem | Célula | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | E01 | PY | Parâmetros | Widgets `codibge`, `tipoestacao` e `formato`. |
# MAGIC | E02 | PY | Configuração | Imports, constantes da API e do volume, `BATCH_ID` e criação do volume `cemaden_stations`. |
# MAGIC | E03 | PY | Log de ingestão | Importa o `IngestionLog` e define `registrar` (etapa `landing`). |
# MAGIC | E04 | PY | Token e chamadas à PED | Token da SGAA com validade mínima de 15 minutos, intervalo mínimo de 5,5 s entre chamadas, retentativa com espera crescente para 429, 5xx e erro de rede, leitura da resposta. |
# MAGIC | E05 | PY | Coleta e validação | Coleta com paginação e validação: cadastro não vazio, `codestacao` presente, duplicados e estações fora do município. |
# MAGIC | E06 | PY | Gravação | Arquivo gzip em `data_coleta=AAAA-MM-DD/`, manifesto gravado por último e registro `SUCESSO` ou `FALHA`. |
# MAGIC | E07 | PY | Valor para o job | Publica `snapshot_date` em `taskValues` para a tarefa seguinte. |
# MAGIC
# MAGIC ## 09. Regras de negócio
# MAGIC
# MAGIC | ID | Regra | Lógica | Comportamento esperado |
# MAGIC |---|---|---|---|
# MAGIC | RN_CEM_005 | Cadastro como snapshot diário | Uma coleta por dia em `data_coleta=AAAA-MM-DD`; rodar de novo no mesmo dia sobrescreve o arquivo do dia. | Histórico diário de estações ativas e inativas. |
# MAGIC | RN_CEM_006 | Estação fora do município | Código da estação que não começa com o código IBGE é contado e informado, sem ser filtrado. | Aviso na saída e na mensagem do log; o tratamento fica para a Silver. |
# MAGIC
# MAGIC ## 10. Saídas de dados
# MAGIC
# MAGIC | Destino | Formato | Granularidade | Chave | Modo de escrita |
# MAGIC |---|---|---|---|---|
# MAGIC | Volume `cemaden_stations`: `data_coleta=AAAA-MM-DD/stations_<codibge>_<AAAAMMDD>.json.gz` e `.manifest.json` | JSON por linha (gzip) e manifesto JSON | Um arquivo por coleta diária | `codestacao` (dentro do arquivo) | Sobrescreve o arquivo do dia |
# MAGIC
# MAGIC As colunas de cada tabela estão no dicionário de dados (`docs/02_catalogos/dicionario_de_dados.csv`).
# MAGIC
# MAGIC ## 11. Qualidade dos dados
# MAGIC
# MAGIC Validações na coleta: cadastro não vazio, todos os registros são objetos JSON com `codestacao`. Duplicidade de `codestacao` e estações fora do município são contadas, informadas e registradas na mensagem do log.
# MAGIC
# MAGIC ## 12. Tratamento de erros
# MAGIC
# MAGIC | Situação | Comportamento | Registro | Recuperação |
# MAGIC |---|---|---|---|
# MAGIC | Token recusado (401 ou 403) | Renova o token e reinicia a coleta uma vez | Saída do notebook | Automática; se persistir, conferir as credenciais no secret scope |
# MAGIC | 429, 5xx ou erro de rede | Até 5 tentativas com espera crescente | Saída do notebook | Automática |
# MAGIC | Resposta vazia ou sem `codestacao` | Para a execução | `FALHA` no `fiap_analytics.bronze.cemaden_ingestion_log` | Conferir os parâmetros e rodar de novo |
# MAGIC
# MAGIC ## 13. Segurança e governança
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Classificação dos dados | Pública (dados da rede observacional do CEMADEN/MCTI; confirmar os termos de uso da PED) |
# MAGIC | Dados sensíveis | Nenhum nos dados. As credenciais da conta PED são sensíveis |
# MAGIC | Credenciais | Secret scope `cemaden`, chaves `ped_email` e `ped_password` |
# MAGIC | Permissões necessárias | USE CATALOG em `fiap_analytics`; USE SCHEMA, CREATE VOLUME, CREATE TABLE e MODIFY em `fiap_analytics.bronze`; READ VOLUME e WRITE VOLUME no volume de landing; acesso de saída à internet no compute |
# MAGIC | Políticas aplicáveis | Termos de uso da API PED do CEMADEN (confirmar); credenciais nunca impressas nem gravadas |
# MAGIC
# MAGIC ## 14. Monitoramento e operação
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Job e tarefa | `ChuvaSegura - CEMADEN` (`jobs/chuvasegura_cemaden.job.yml`), tarefa `cemaden_stations_landing` |
# MAGIC | Frequência | Diária, 07:00 (America/Sao_Paulo); agenda pausada até o backfill |
# MAGIC | SLA | A definir |
# MAGIC | Métricas | Estações coletadas, duplicadas e fora do município |
# MAGIC | Logs | `fiap_analytics.bronze.cemaden_ingestion_log` (etapa `landing`, janela = data da coleta) |
# MAGIC | Alertas | Não configurado (recomendação: notificação de falha no job) |
# MAGIC | Reprocessamento | Rodar de novo no mesmo dia (sobrescreve a coleta do dia) |
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
dbutils.widgets.text("codibge", "3550308", "Código IBGE do município")
dbutils.widgets.text("tipoestacao", "", "Tipo de estação (vazio = todos)")
dbutils.widgets.text("formato", "JSON", "Parâmetro formato (vazio = não enviar)")

# COMMAND ----------

# [PY] E02 Configuração
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

LOG_TABLE = "fiap_analytics.bronze.cemaden_ingestion_log"
FONTE = "cemaden_stations"
ETAPA = "landing"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Token SGAA, limitador e chamadas à PED

# COMMAND ----------

# [PY] E04 Token e chamadas à PED
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

# [PY] E05 Coleta e validação
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

# [PY] E06 Gravação
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

# [PY] E07 Valor para o job
# Repassa a data da coleta para a tarefa da Bronze quando rodar dentro de um Job.
try:
    dbutils.jobs.taskValues.set(key="snapshot_date", value=snapshot_date)
except Exception:
    pass
