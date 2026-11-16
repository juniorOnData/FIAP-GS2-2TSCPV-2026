# Databricks notebook source
# MAGIC %md
# MAGIC # 01_cemaden_rainfall_readings_landing
# MAGIC
# MAGIC Baixa as leituras de chuva da rede CEMADEN no município de São Paulo pela API PED (`pcds/pcds-dados-pg`) e grava um arquivo por mês no volume `fiap_analytics.bronze.cemaden_rainfall_readings`.
# MAGIC
# MAGIC Modos (widget `modo`):
# MAGIC
# MAGIC 1. `backfill` (padrão): baixa de `mes_inicio` até `mes_fim` (vazio = último mês fechado), pulando os meses já concluídos no log. Com `forcar_reprocesso = sim`, rebaixa todos.
# MAGIC 2. `incremental`: baixa o mês atual, o mês anterior enquanto não estiver concluído e os meses que ficaram parciais.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Token da SGAA com as credenciais do secret scope `cemaden` (`ped_email` e `ped_password`). Limite da PED: 12 chamadas por minuto.
# MAGIC 2. Um arquivo `ano=AAAA/mes=MM/readings_AAAAMM.json.gz` por mês, em JSON por linha, com os registros exatamente como a API devolve.
# MAGIC 3. Mês completo só quando baixado pelo menos 3 dias depois de terminar (`CARENCIA`). Antes disso entra como `PARCIAL` no `cemaden_ingestion_log`.
# MAGIC 4. O `cemaden_ingestion_log` serve de checkpoint: rodar de novo continua de onde parou.

# COMMAND ----------

dbutils.widgets.dropdown("modo", "backfill", ["backfill", "incremental"], "Modo")
dbutils.widgets.text("mes_inicio", "201401", "Mês inicial (AAAAMM)")
dbutils.widgets.text("mes_fim", "", "Mês final (AAAAMM, vazio = último mês fechado)")
dbutils.widgets.dropdown("forcar_reprocesso", "nao", ["nao", "sim"], "Rebaixar meses já carregados")

# COMMAND ----------

import base64
import calendar
import gzip
import hashlib
import json
import os
import shutil
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import requests

SGAA_URL = "https://sgaa.cemaden.gov.br/SGAA/rest/controle-token/tokens"
PED_URL = "https://sws.cemaden.gov.br/PED/rest"
ENDPOINT = "pcds/pcds-dados-pg"
ENDPOINT_PAGINA = "controle-paginacao/pagina"
COD_IBGE = "3550308"  # município de São Paulo
REDE = "11"  # rede CEMADEN

CATALOGO, SCHEMA = "fiap_analytics", "bronze"
VOLUME = f"/Volumes/{CATALOGO}/{SCHEMA}/cemaden_rainfall_readings"

SECRET_SCOPE = "cemaden"
INTERVALO_MIN_S = 5.5  # limite da PED: 12 chamadas por minuto
MARGEM_TOKEN = timedelta(minutes=15)  # um mês leva poucos minutos; o token precisa durar o mês inteiro
TENTATIVAS_HTTP = 5
TENTATIVAS_MES = 3
CARENCIA = timedelta(days=3)  # o mês só é dado como completo se foi baixado 3 dias depois de terminar

BATCH_ID = str(uuid.uuid4())
MODO = dbutils.widgets.get("modo")
FORCAR = dbutils.widgets.get("forcar_reprocesso") == "sim"

def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC] {msg}", flush=True)

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.cemaden_rainfall_readings "
          "COMMENT 'Landing das leituras de chuva da API PED do CEMADEN (pcds-dados-pg), um JSON por mês'")

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
FONTE = "cemaden_rainfall_readings"
ETAPA = "landing"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Token SGAA, limitador e chamadas à PED

# COMMAND ----------

def jwt_expiracao(jwt: str) -> datetime:
    payload = jwt.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return datetime.fromtimestamp(json.loads(base64.urlsafe_b64decode(payload))["exp"], tz=timezone.utc)

class TokenRejeitado(Exception):
    pass

class GerenciadorToken:
    """Obtém o token na SGAA. A SGAA devolve o token ativo enquanto ele não expira,
    então, se faltar pouco, espera a expiração para receber um token novo."""

    def __init__(self):
        self.token = None

    def _pedir(self) -> str:
        r = requests.post(
            SGAA_URL,
            json={
                "email": dbutils.secrets.get(SECRET_SCOPE, "ped_email"),
                "password": dbutils.secrets.get(SECRET_SCOPE, "ped_password"),
            },
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=60,
        )
        if r.status_code not in (200, 201):
            raise RuntimeError(f"SGAA respondeu status {r.status_code}")
        return r.json()["token"]

    def restante(self) -> timedelta:
        return jwt_expiracao(self.token) - datetime.now(timezone.utc) if self.token else timedelta(0)

    def garantir_validade(self) -> None:
        if self.token and self.restante() > MARGEM_TOKEN:
            return
        self.token = self._pedir()
        if self.restante() <= MARGEM_TOKEN:
            espera = max(self.restante().total_seconds(), 0) + 30
            log(f"Token ativo expira em {self.restante()}. Aguardando {espera / 60:.1f} min para renovar.")
            time.sleep(espera)
            self.token = self._pedir()
        log(f"Token válido até {jwt_expiracao(self.token):%Y-%m-%d %H:%M} UTC")


class LimitadorTaxa:
    def __init__(self, intervalo_s: float):
        self.intervalo_s = intervalo_s
        self._ultimo = 0.0

    def aguardar(self) -> None:
        espera = self._ultimo + self.intervalo_s - time.monotonic()
        if espera > 0:
            time.sleep(espera)
        self._ultimo = time.monotonic()


tokens = GerenciadorToken()
limitador = LimitadorTaxa(INTERVALO_MIN_S)
sessao = requests.Session()


def get_ped(path: str, params: dict) -> requests.Response:
    ultimo_erro = None
    for tentativa in range(1, TENTATIVAS_HTTP + 1):
        limitador.aguardar()
        try:
            r = sessao.get(f"{PED_URL}/{path}", params=params, headers={"token": tokens.token}, timeout=120)
        except requests.RequestException as e:
            ultimo_erro = type(e).__name__
        else:
            if r.status_code in (200, 202):
                return r
            if r.status_code in (401, 403):
                raise TokenRejeitado(f"{path} status {r.status_code}")
            if r.status_code not in (429, 500, 502, 503, 504):
                raise RuntimeError(f"{path} status {r.status_code}: {r.text[:200]}")
            ultimo_erro = f"status {r.status_code}"
        espera = min(60, 5 * 2 ** tentativa)
        log(f"   {path}: {ultimo_erro}. Nova tentativa em {espera}s ({tentativa}/{TENTATIVAS_HTTP})")
        time.sleep(espera)
    raise RuntimeError(f"{path} falhou após {TENTATIVAS_HTTP} tentativas: {ultimo_erro}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coleta e gravação de um mês

# COMMAND ----------

def limites_mes(ano: int, mes: int) -> tuple:
    ultimo_dia = calendar.monthrange(ano, mes)[1]
    return f"{ano}{mes:02d}010000", f"{ano}{mes:02d}{ultimo_dia:02d}2359"


def buscar_mes(ano: int, mes: int) -> tuple:
    datainicio, datafim = limites_mes(ano, mes)
    params = {"codibge": COD_IBGE, "rede": REDE, "datainicio": datainicio, "datafim": datafim}
    registros, paginas = [], 0
    r = get_ped(ENDPOINT, params)
    while True:
        try:
            corpo = r.json()
        except ValueError:
            raise RuntimeError(f"Resposta não é JSON (status {r.status_code})")
        lote = corpo.get("data") or []
        if not lote:  # status 202 com lista vazia: fim da paginação ou mês sem dados
            break
        paginas += 1
        registros.extend(lote)
        scroll = corpo.get("scroll")
        if not scroll:
            break
        r = get_ped(ENDPOINT_PAGINA, {"scroll": scroll})
    return registros, paginas


def gravar_mes(ano: int, mes: int, registros: list) -> tuple:
    pasta = f"{VOLUME}/ano={ano}/mes={mes:02d}"
    destino = f"{pasta}/readings_{ano}{mes:02d}.json.gz"
    temporario = f"/tmp/readings_{ano}{mes:02d}_{BATCH_ID}.json.gz"
    with gzip.open(temporario, "wt", encoding="utf-8") as f:
        for reg in registros:  # JSON por linha, com o conteúdo exatamente como veio da API
            f.write(json.dumps(reg, ensure_ascii=False))
            f.write("\n")
    with open(temporario, "rb") as f:
        sha256 = hashlib.sha256(f.read()).hexdigest()
    os.makedirs(pasta, exist_ok=True)
    shutil.copyfile(temporario, destino)
    os.remove(temporario)
    return destino, sha256


def contar_duplicados(registros: list) -> int:
    chaves = [(r.get("codestacao"), r.get("datahora"), r.get("id_sensor")) for r in registros]
    return len(chaves) - len(set(chaves))

def proximo_mes(ano: int, mes: int) -> tuple:
    return (ano + 1, 1) if mes == 12 else (ano, mes + 1)


def mes_fechado(ano: int, mes: int, coletado_em: datetime) -> bool:
    """O mês está completo se a coleta começou depois do fim do mês mais a carência (leituras atrasadas)."""
    ano_seguinte, mes_seguinte = proximo_mes(ano, mes)
    return coletado_em >= datetime(ano_seguinte, mes_seguinte, 1, tzinfo=timezone.utc) + CARENCIA


def lista_meses(inicio: str, fim: str) -> list:
    ano, mes = int(inicio[:4]), int(inicio[4:])
    ano_fim, mes_fim = int(fim[:4]), int(fim[4:])
    meses = []
    while (ano, mes) <= (ano_fim, mes_fim):
        meses.append((ano, mes))
        ano, mes = proximo_mes(ano, mes)
    return meses

# COMMAND ----------

# MAGIC %md
# MAGIC ## Meses a processar

# COMMAND ----------

hoje = datetime.now(timezone.utc).date()
mes_atual = (hoje.year, hoje.month)
mes_anterior = (hoje.year - 1, 12) if hoje.month == 1 else (hoje.year, hoje.month - 1)

# Concluído: já teve SUCESSO ou SEM_DADOS com o mês fechado. Parcial: só foi baixado antes de fechar.
situacao = spark.sql(f"""
    SELECT janela,
           max(CASE WHEN status IN ('SUCESSO', 'SEM_DADOS') THEN 1 ELSE 0 END) AS concluido,
           max(CASE WHEN status = 'PARCIAL' THEN 1 ELSE 0 END) AS parcial
    FROM {LOG_TABLE}
    WHERE fonte = '{FONTE}' AND etapa = 'landing'
    GROUP BY janela
""").collect()
concluidos = {row.janela for row in situacao if row.concluido}
parciais = {row.janela for row in situacao if row.parcial and not row.concluido}

if MODO == "incremental":
    # Mês atual, mês anterior enquanto não estiver concluído e meses baixados antes de fechar
    # que ainda não foram completados (por exemplo, se o job ficou dias sem rodar).
    candidatos = {mes_atual, mes_anterior} | {(int(j[:4]), int(j[4:])) for j in parciais}
    meses = sorted(m for m in candidatos if f"{m[0]}{m[1]:02d}" not in concluidos)
else:
    fim = dbutils.widgets.get("mes_fim").strip() or f"{mes_anterior[0]}{mes_anterior[1]:02d}"
    meses = lista_meses(dbutils.widgets.get("mes_inicio").strip(), fim)
    if not FORCAR:
        meses = [m for m in meses if f"{m[0]}{m[1]:02d}" not in concluidos]

log(f"Modo {MODO} | batch {BATCH_ID} | {len(meses)} meses a processar")
print([f"{a}{m:02d}" for a, m in meses])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coleta

# COMMAND ----------

resumo = []
for ano, mes in meses:
    janela = f"{ano}{mes:02d}"
    iniciado_em = datetime.now(timezone.utc)
    for tentativa in range(1, TENTATIVAS_MES + 1):
        try:
            tokens.garantir_validade()
            registros, paginas = buscar_mes(ano, mes)
            fechado = mes_fechado(ano, mes, iniciado_em)
            if not registros:
                status = "SEM_DADOS" if fechado else "PARCIAL"
                registrar(janela, status, iniciado_em, registros=0, paginas=0)
                log(f"{janela}: {status}, sem dados")
                resumo.append((janela, status, 0))
                break
            duplicados = contar_duplicados(registros)
            arquivo, sha256 = gravar_mes(ano, mes, registros)
            status = "SUCESSO" if fechado else "PARCIAL"
            registrar(janela, status, iniciado_em, registros=len(registros), paginas=paginas,
                      duplicados=duplicados, arquivo=arquivo, sha256=sha256)
            log(f"{janela}: {status}, {len(registros)} registros, {paginas} páginas, {duplicados} duplicados")
            resumo.append((janela, status, len(registros)))
            break
        except TokenRejeitado as e:
            log(f"{janela}: token rejeitado ({e}). Renovando e reiniciando o mês.")
            tokens.token = None
        except Exception as e:  # o scroll expira em 30 segundos; uma falha no meio reinicia o mês inteiro
            log(f"{janela}: falha na tentativa {tentativa}/{TENTATIVAS_MES}: {e}")
            if tentativa == TENTATIVAS_MES:
                registrar(janela, "FALHA", iniciado_em, mensagem=str(e))
                resumo.append((janela, "FALHA", 0))
            else:
                time.sleep(30)

falhas = [j for j, s, _ in resumo if s == "FALHA"]
log(f"Concluído: {len(resumo)} meses, {sum(n for _, _, n in resumo)} registros, {len(falhas)} falhas")
if falhas:
    raise RuntimeError(f"Meses com falha: {falhas}. Rode de novo: o notebook continua de onde parou.")
