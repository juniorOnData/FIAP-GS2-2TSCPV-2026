# Databricks notebook source
# MAGIC %md
# MAGIC # CEM_LND_002 01_cemaden_rainfall_readings_landing
# MAGIC
# MAGIC ## 01. Identificação
# MAGIC
# MAGIC | Campo | Valor |
# MAGIC |---|---|
# MAGIC | Identificador | CEM_LND_002 |
# MAGIC | Nome | 01_cemaden_rainfall_readings_landing |
# MAGIC | Projeto | ChuvaSegura |
# MAGIC | Domínio de negócio | Pluviometria (CEMADEN) |
# MAGIC | Camada | Landing |
# MAGIC | Linguagem | Python |
# MAGIC | Responsável técnico | juniorOnData |
# MAGIC | Responsável funcional | A definir |
# MAGIC | Status | Homologação |
# MAGIC | Versão | 1.3 |
# MAGIC | Criticidade | Média |
# MAGIC | Caminho no repositório | `src/cemaden/ingestion/01_cemaden_rainfall_readings_landing.py` |
# MAGIC
# MAGIC ## 02. Objetivo
# MAGIC
# MAGIC Baixar as leituras de chuva da rede CEMADEN no município pela API PED e gravar um arquivo por mês no volume, usando o log como checkpoint para continuar de onde parou.
# MAGIC
# MAGIC ## 03. Contexto funcional
# MAGIC
# MAGIC As leituras de chuva são a medida que será cruzada com as ocorrências de alagamento.
# MAGIC
# MAGIC ## 04. Arquitetura e fluxo
# MAGIC
# MAGIC | Posição | Item |
# MAGIC |---|---|
# MAGIC | Etapa anterior | Nenhuma |
# MAGIC | Etapa atual | Grava `ano=AAAA/mes=MM/` no volume `cemaden_rainfall_readings` |
# MAGIC | Etapa posterior | `02_cemaden_rainfall_readings_bronze` |
# MAGIC
# MAGIC ## 05. Dependências
# MAGIC
# MAGIC | Tipo | Item | Uso |
# MAGIC |---|---|---|
# MAGIC | Conexão | SGAA e PED `pcds/pcds-dados-pg` e `controle-paginacao/pagina` | Token e leituras paginadas |
# MAGIC | Secret | Scope `cemaden`, chaves `ped_email` e `ped_password` | Credenciais da conta PED |
# MAGIC | Tabela | `fiap_analytics.bronze.cemaden_ingestion_log` | Checkpoint dos meses concluídos e parciais |
# MAGIC | Módulo | `src/common/ingestion_log.py` | Registro no log |
# MAGIC | Biblioteca | `requests` | Chamadas HTTP |
# MAGIC | Permissão | USE CATALOG em `fiap_analytics`; USE SCHEMA, CREATE VOLUME, CREATE TABLE e MODIFY em `fiap_analytics.bronze`; READ VOLUME e WRITE VOLUME no volume de landing; acesso de saída à internet no compute | Criar o volume, gravar arquivos e o log |
# MAGIC
# MAGIC ## 06. Parâmetros de entrada
# MAGIC
# MAGIC | Nome | Tipo | Obrigatório | Descrição | Valor padrão | Exemplo |
# MAGIC |---|---|---|---|---|---|
# MAGIC | `modo` | dropdown (texto) | Sim | `backfill` baixa um intervalo de meses; `incremental` baixa o mês atual e os pendentes | backfill | incremental |
# MAGIC | `mes_inicio` | text (AAAAMM) | No backfill | Primeiro mês do backfill | 201401 | 202001 |
# MAGIC | `mes_fim` | text (AAAAMM) | Não | Último mês do backfill; vazio usa o último mês fechado | (vazio) | 202610 |
# MAGIC | `forcar_reprocesso` | dropdown (nao/sim) | Não | No backfill, baixa de novo os meses já concluídos | nao | sim |
# MAGIC
# MAGIC ## 07. Fontes de dados
# MAGIC
# MAGIC | Fonte | Formato | Origem | Tipo de leitura | Frequência | Chave | Filtros |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | PED `pcds/pcds-dados-pg` | JSON paginado (scroll) | CEMADEN | Por janela mensal | Diária no incremental | `codestacao` + `datahora` + `id_sensor` | `codibge = 3550308`, `rede = 11`, `datainicio` e `datafim` do mês |
# MAGIC
# MAGIC ## 08. Processamento técnico
# MAGIC
# MAGIC | Etapa | Linguagem | Célula | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | E01 | PY | Parâmetros | Widgets `modo`, `mes_inicio`, `mes_fim` e `forcar_reprocesso`. |
# MAGIC | E02 | PY | Configuração | Imports, constantes da API, do volume e de retentativa, carência de 3 dias, `BATCH_ID` e criação do volume. |
# MAGIC | E03 | PY | Log de ingestão | Importa o `IngestionLog` e define `registrar` (etapa `landing`). |
# MAGIC | E04 | PY | Token e chamadas à PED | Gerenciador de token da SGAA, limitador de 5,5 s entre chamadas e `get_ped` com retentativa. |
# MAGIC | E05 | PY | Coleta e gravação de um mês | Limites do mês, busca paginada, gravação gzip com SHA256, contagem de duplicados, regra de mês fechado e lista de meses. |
# MAGIC | E06 | PY | Meses a processar | Lê no log os meses concluídos e parciais e monta a lista conforme o modo. |
# MAGIC | E07 | PY | Coleta | Para cada mês, até 3 tentativas; grava o arquivo e registra `SUCESSO`, `PARCIAL`, `SEM_DADOS` ou `FALHA`; ao final, erro se algum mês falhou. |
# MAGIC
# MAGIC ## 09. Regras de negócio
# MAGIC
# MAGIC | ID | Regra | Lógica | Comportamento esperado |
# MAGIC |---|---|---|---|
# MAGIC | RN_CEM_001 | Recorte territorial e de rede | Município 3550308 (São Paulo) e rede 11 (CEMADEN). | Só leituras da capital. |
# MAGIC | RN_CEM_002 | Mês completo | Um mês é completo se a coleta começou pelo menos 3 dias depois do fim do mês (carência para leituras atrasadas). | Antes disso o mês fica `PARCIAL` e volta a ser baixado. |
# MAGIC | RN_CEM_003 | Meses do incremental | Mês atual, mês anterior enquanto não estiver concluído e meses parciais ainda não concluídos. | Nenhum mês fica incompleto para sempre, mesmo se o job passar dias sem rodar. |
# MAGIC | RN_CEM_004 | Duplicidade de leitura | Chave `codestacao` + `datahora` + `id_sensor`. | Duplicados são contados e registrados em `duplicados`, mas não removidos na landing. |
# MAGIC
# MAGIC ## 10. Saídas de dados
# MAGIC
# MAGIC | Destino | Formato | Granularidade | Chave | Modo de escrita |
# MAGIC |---|---|---|---|---|
# MAGIC | Volume `cemaden_rainfall_readings`: `ano=AAAA/mes=MM/readings_AAAAMM.json.gz` | JSON por linha (gzip) | Um arquivo por mês | `codestacao` + `datahora` + `id_sensor` | Sobrescreve o arquivo do mês |
# MAGIC
# MAGIC As colunas de cada tabela estão no dicionário de dados (`docs/02_catalogos/dicionario_de_dados.csv`).
# MAGIC
# MAGIC ## 11. Qualidade dos dados
# MAGIC
# MAGIC Contagem de duplicados por mês registrada no log. A conferência de linhas entre arquivo e log é feita na Bronze.
# MAGIC
# MAGIC ## 12. Tratamento de erros
# MAGIC
# MAGIC | Situação | Comportamento | Registro | Recuperação |
# MAGIC |---|---|---|---|
# MAGIC | Token recusado | Renova o token e reinicia o mês | Saída do notebook | Automática |
# MAGIC | Erro no meio da paginação (o scroll expira em 30 s) | Reinicia o mês; até 3 tentativas com 30 s de espera | `FALHA` no log após a terceira | Rodar de novo: o notebook continua de onde parou |
# MAGIC | Meses com falha ao final | A tarefa termina com erro listando os meses | `FALHA` no log | Rodar de novo |
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
# MAGIC | Job e tarefa | `ChuvaSegura - CEMADEN` (`jobs/chuvasegura_cemaden.job.yml`), tarefa `cemaden_rainfall_readings_landing` |
# MAGIC | Frequência | Diária, 07:00 (America/Sao_Paulo); agenda pausada até o backfill |
# MAGIC | SLA | A definir |
# MAGIC | Métricas | Meses por status, registros, páginas e duplicados |
# MAGIC | Logs | `fiap_analytics.bronze.cemaden_ingestion_log` (etapa `landing`, janela = AAAAMM) |
# MAGIC | Alertas | Não configurado (recomendação: notificação de falha no job) |
# MAGIC | Reprocessamento | Backfill com `forcar_reprocesso = sim` para o intervalo desejado |
# MAGIC
# MAGIC ## 15. Histórico de alterações
# MAGIC
# MAGIC | Versão | Data | Responsável | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | 1.0 | 2026-11-10 | juniorOnData | Criação da coleta de leituras de chuva (PR #6) |
# MAGIC | 1.1 | 2026-11-16 | juniorOnData | Log de ingestão próprio da origem e meses parciais (PR #8) |
# MAGIC | 1.2 | 2026-11-16 | juniorOnData | Formato de código fonte, módulo de log compartilhado e novo nome do arquivo (PR #9) |
# MAGIC | 1.3 | 2026-11-16 | juniorOnData | Documentação no padrão oficial (PR #15) |

# COMMAND ----------

# [PY] E01 Parâmetros
dbutils.widgets.dropdown("modo", "backfill", ["backfill", "incremental"], "Modo")
dbutils.widgets.text("mes_inicio", "201401", "Mês inicial (AAAAMM)")
dbutils.widgets.text("mes_fim", "", "Mês final (AAAAMM, vazio = último mês fechado)")
dbutils.widgets.dropdown("forcar_reprocesso", "nao", ["nao", "sim"], "Rebaixar meses já carregados")

# COMMAND ----------

# [PY] E02 Configuração
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
FONTE = "cemaden_rainfall_readings"
ETAPA = "landing"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Token SGAA, limitador e chamadas à PED

# COMMAND ----------

# [PY] E04 Token e chamadas à PED
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

# [PY] E05 Coleta e gravação de um mês
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

# [PY] E06 Meses a processar
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

# [PY] E07 Coleta
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
