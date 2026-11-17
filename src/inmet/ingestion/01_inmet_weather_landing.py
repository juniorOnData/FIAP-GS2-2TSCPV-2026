# Databricks notebook source
# MAGIC %md
# MAGIC # 01_inmet_weather_landing
# MAGIC
# MAGIC Baixa do portal do INMET os dados meteorológicos horários das estações automáticas do município de São Paulo e grava um arquivo por ano no volume `fiap_analytics.bronze.inmet_weather`.
# MAGIC
# MAGIC Fonte: dados históricos anuais do BDMEP (`https://portal.inmet.gov.br/uploads/dadoshistoricos/AAAA.zip`). Cada zip do ano (cerca de 100 MB) tem um CSV por estação automática do Brasil. O do ano corrente é atualizado pelo INMET mais ou menos uma vez por mês.
# MAGIC
# MAGIC Por que o INMET entra: traz variáveis que o CEMADEN e o SIBH não têm (temperatura, umidade, pressão, vento e radiação), além de chuva medida por outra rede.
# MAGIC
# MAGIC Modos (widget `modo`):
# MAGIC
# MAGIC 1. `backfill` (padrão): baixa de `ano_inicio` (mínimo 2020, o início do período do projeto) até o ano corrente, pulando os anos já concluídos no log. Com `forcar = sim`, baixa todos de novo.
# MAGIC 2. `incremental`: baixa o ano corrente, o ano anterior enquanto não estiver concluído e os anos que ficaram parciais.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Só os CSVs das estações do widget `estacoes` (padrão `A701,A771`: São Paulo - Mirante e São Paulo - Interlagos), lidos direto do zip publicado por HTTP Range. Gravados sem alteração num zip `ano=AAAA/inmet_sp_AAAA.zip`, com um manifesto ao lado (arquivos, ETag e data de modificação do zip na fonte).
# MAGIC 2. Ano completo (`SUCESSO`) só quando baixado depois de 31 de janeiro do ano seguinte (`CARENCIA`), porque o INMET fecha o ano no arquivo publicado em janeiro. Antes disso entra como `PARCIAL`.
# MAGIC 3. Se o zip do ano não mudou na fonte desde a última coleta (mesmo ETag, tamanho e data), nada é baixado e o log registra `SEM_DADOS` com o motivo. Ano ainda não publicado (404) também é `SEM_DADOS`. Só `SUCESSO` conclui um ano.
# MAGIC 4. Uma linha por ano no `fiap_analytics.bronze.inmet_ingestion_log` (etapa `landing`); `registros` é o número de CSVs extraídos.

# COMMAND ----------

dbutils.widgets.dropdown("modo", "backfill", ["backfill", "incremental"], "Modo")
dbutils.widgets.text("ano_inicio", "2020", "Ano inicial (mínimo 2020)")
dbutils.widgets.text("estacoes", "A701,A771", "Códigos das estações, separados por vírgula")
dbutils.widgets.dropdown("forcar", "nao", ["nao", "sim"], "Baixar mesmo sem alteração na fonte")

# COMMAND ----------

import json
import os
import re
import sys
import tempfile
import uuid
from datetime import datetime, timezone

CATALOG, SCHEMA, VOLUME = "fiap_analytics", "bronze", "inmet_weather"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
DATASET = "inmet_weather"
URL_ANO = "https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip"
ANO_MINIMO = 2020  # o projeto usa 2020 em diante, como o CEMADEN e o SIBH
MES_CARENCIA = 2   # o ano só é dado como completo se foi baixado a partir de fevereiro do ano seguinte

MODO = dbutils.widgets.get("modo")
FORCAR = dbutils.widgets.get("forcar") == "sim"
ESTACOES = sorted({e.strip().upper() for e in dbutils.widgets.get("estacoes").split(",") if e.strip()})
if not ESTACOES or not all(re.fullmatch(r"[A-Z]\d{3}", e) for e in ESTACOES):
    raise ValueError(f"Códigos de estação inválidos: {ESTACOES}")
BATCH_ID = str(uuid.uuid4())

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME} "
          "COMMENT 'Landing dos dados meteorológicos horários do INMET (BDMEP, estações automáticas do município de "
          "São Paulo), um zip por ano com os CSVs originais.'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log de execução

# COMMAND ----------

# Módulos de src/common. Sobe pelas pastas até achar src/ para importar.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common.arquivos import copiar_para_volume, extrair_do_zip, manifestos, metadados_remotos  # noqa: E402
from common.ingestion_log import IngestionLog  # noqa: E402

LOG_TABLE = "fiap_analytics.bronze.inmet_ingestion_log"
FONTE = "inmet_weather"
ETAPA = "landing"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anos a processar

# COMMAND ----------

agora = datetime.now(timezone.utc)
ano_atual = agora.year

situacao = spark.sql(f"""
    SELECT janela,
           max(CASE WHEN status = 'SUCESSO' THEN 1 ELSE 0 END) AS concluido,
           max(CASE WHEN status = 'PARCIAL' THEN 1 ELSE 0 END) AS parcial
    FROM {LOG_TABLE}
    WHERE fonte = '{FONTE}' AND etapa = 'landing'
    GROUP BY janela
""").collect()
concluidos = {int(r.janela) for r in situacao if r.concluido}
parciais = {int(r.janela) for r in situacao if r.parcial and not r.concluido}

if MODO == "incremental":
    candidatos = {ano_atual, ano_atual - 1} | parciais
    anos = sorted(a for a in candidatos if a >= ANO_MINIMO and a not in concluidos)
else:
    inicio = max(int(dbutils.widgets.get("ano_inicio").strip() or ANO_MINIMO), ANO_MINIMO)
    anos = [a for a in range(inicio, ano_atual + 1) if FORCAR or a not in concluidos]

# Última coleta de cada ano no volume, para saber se o zip mudou na fonte.
ultima_coleta = {}
for m in manifestos(VOLUME_PATH):
    ultima_coleta[int(m["snapshot_date"])] = m  # manifestos() vem do mais antigo para o mais recente

print(f"Modo {MODO} | estações {ESTACOES} | anos: {anos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coleta

# COMMAND ----------

padrao = rf"(?i)_({'|'.join(ESTACOES)})_[^/]*\.csv$"  # os arquivos do INMET terminam em .CSV
falhas = []
for ano in anos:
    janela, iniciado_em = str(ano), datetime.now(timezone.utc)
    try:
        url = URL_ANO.format(ano=ano)
        remoto = metadados_remotos(url, ausente_ok=True)
        if remoto is None:
            registrar(janela, "SEM_DADOS", iniciado_em, registros=0, mensagem="arquivo do ano ainda não publicado (404)")
            print(f"{ano}: ainda não publicado")
            continue
        anterior = ultima_coleta.get(ano)
        chave = ("etag", "tamanho", "ultima_modificacao")
        if anterior and not FORCAR and all(anterior["remote"].get(k) == remoto.get(k) for k in chave):
            registrar(janela, "SEM_DADOS", iniciado_em, registros=0,
                      mensagem=f"zip sem alteração na fonte desde a coleta de {anterior['collected_at_utc'][:10]}")
            print(f"{ano}: sem alteração na fonte")
            continue

        base_name = f"inmet_sp_{ano}"
        destino = f"{VOLUME_PATH}/ano={ano}/{base_name}.zip"
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, f"{base_name}.zip")
            resultado = extrair_do_zip(url, padrao, local)
            copiar_para_volume(local, destino)
        encontradas = sorted({re.search(padrao, n).group(1).upper() for n, _ in resultado["membros"]})
        faltando = sorted(set(ESTACOES) - set(encontradas))

        manifesto = {
            "dataset": DATASET,
            "file_name": f"{base_name}.zip",
            "snapshot_date": janela,
            "collected_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_url": url,
            "remote": resultado["metadados"],
            "stations": encontradas,
            "missing_stations": faltando,
            "members": [{"name": n, "bytes": b} for n, b in resultado["membros"]],
            "sha256": resultado["sha256"],
            "landing_batch_id": BATCH_ID,
        }
        with open(f"{VOLUME_PATH}/ano={ano}/{base_name}.manifest.json", "w", encoding="utf-8") as fh:
            json.dump(manifesto, fh, ensure_ascii=False, indent=2)

        fechado = iniciado_em >= datetime(ano + 1, MES_CARENCIA, 1, tzinfo=timezone.utc)
        status = "SUCESSO" if fechado else "PARCIAL"
        mensagem = f"estações: {', '.join(encontradas)}" + (f"; sem arquivo no ano: {', '.join(faltando)}" if faltando else "")
        registrar(janela, status, iniciado_em, registros=len(resultado["membros"]), paginas=1,
                  arquivo=destino, sha256=resultado["sha256"], mensagem=mensagem)
        print(f"{ano}: {status}, {mensagem}")
    except Exception as exc:
        registrar(janela, "FALHA", iniciado_em, mensagem=str(exc))
        falhas.append(ano)
        print(f"{ano}: FALHA {exc}")

if falhas:
    raise RuntimeError(f"Anos com falha: {falhas}. Rode de novo: o notebook continua de onde parou.")
