# Databricks notebook source
# MAGIC %md
# MAGIC # 01_sgb_susceptibility_landing
# MAGIC
# MAGIC Baixa do RIGEO (repositório do Serviço Geológico do Brasil, SGB/CPRM) as camadas vetoriais da *Carta de suscetibilidade a movimentos gravitacionais de massa e inundações: município de São Paulo - SP* (IPT/CPRM, 2015, escala 1:75.000) e grava no volume `fiap_analytics.bronze.sgb_susceptibility`.
# MAGIC
# MAGIC Fonte: pacote `sig_saopaulo_sp_suscet.zip` do registro https://rigeo.sgb.gov.br/handle/doc/15216 (cerca de 970 MB, quase tudo fotos de campo).
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Só as camadas usadas pelo projeto: a pasta `12.Suscetibilidade` (inundação, bacias contribuintes, enxurrada, movimento de massa, corrida de massa e os quadros-síntese) e a pasta `04.PadrõesDeRelevo`. Os membros são lidos direto do zip publicado, por requisições HTTP Range, sem baixar o pacote inteiro.
# MAGIC 2. O conteúdo de cada arquivo é gravado exatamente como veio, num zip `versao=AAAA-MM-DD/sgb_suscetibilidade_saopaulo_AAAAMMDD.zip`, com um manifesto ao lado (membros, tamanho, SHA256, ETag e data de modificação do pacote na fonte).
# MAGIC 3. A carta é estática: se o pacote na fonte não mudou desde a última coleta (mesmo ETag, tamanho e data de modificação), nada é baixado e o log registra `SEM_DADOS`. Com `forcar = sim`, baixa de novo.
# MAGIC 4. Uma linha no `fiap_analytics.bronze.sgb_ingestion_log` (etapa `landing`); `registros` é o número de arquivos extraídos.

# COMMAND ----------

dbutils.widgets.text(
    "url", "https://rigeo.sgb.gov.br/server/api/core/bitstreams/a74255cb-fed8-45a5-8865-1626e84f0dd0/content",
    "URL do pacote SIG da carta (RIGEO)")
dbutils.widgets.dropdown("forcar", "nao", ["nao", "sim"], "Baixar mesmo sem alteração na fonte")

# COMMAND ----------

import json
import os
import sys
import tempfile
import uuid
from datetime import datetime, timezone

CATALOG, SCHEMA, VOLUME = "fiap_analytics", "bronze", "sgb_susceptibility"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
DATASET = "sgb_susceptibility"
REGISTRO_FONTE = "https://rigeo.sgb.gov.br/handle/doc/15216"
# Membros do pacote usados pelo projeto: camadas de suscetibilidade, quadros-síntese e padrões de relevo.
PADRAO_MEMBROS = r"^(12\.Suscetibilidade|04\.Padr[^/]*Relevo)/[^/]+\.(shp|shx|dbf|prj|cpg|docx)$"

URL = dbutils.widgets.get("url").strip()
FORCAR = dbutils.widgets.get("forcar") == "sim"
BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc)

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME} "
          "COMMENT 'Landing da carta de suscetibilidade a movimentos de massa e inundações do município de São Paulo "
          "(SGB/CPRM e IPT, 2015): camadas vetoriais em shapefile, um zip por versão coletada.'")

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
from common.arquivos import copiar_para_volume, extrair_do_zip, metadados_remotos, ultimo_manifesto  # noqa: E402
from common.ingestion_log import IngestionLog  # noqa: E402

LOG_TABLE = "fiap_analytics.bronze.sgb_ingestion_log"
FONTE = "sgb_susceptibility"
ETAPA = "landing"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coleta

# COMMAND ----------


janela = STARTED_AT.strftime("%Y-%m-%d")
try:
    remoto = metadados_remotos(URL)
    anterior = ultimo_manifesto(VOLUME_PATH)
    chave = ("etag", "tamanho", "ultima_modificacao")
    if anterior and not FORCAR and all(anterior["remote"].get(k) == remoto.get(k) for k in chave):
        msg = f"pacote sem alteração na fonte desde a coleta de {anterior['snapshot_date']}"
        registrar(janela, "SEM_DADOS", STARTED_AT, registros=0, mensagem=msg)
        print(f"Nada a baixar: {msg}")
    else:
        base_name = f"sgb_suscetibilidade_saopaulo_{STARTED_AT:%Y%m%d}"
        destino = f"{VOLUME_PATH}/versao={janela}/{base_name}.zip"
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, f"{base_name}.zip")
            resultado = extrair_do_zip(URL, PADRAO_MEMBROS, local)
            tamanho = os.path.getsize(local)
            copiar_para_volume(local, destino)

        camadas = sorted(os.path.splitext(os.path.basename(n))[0] for n, _ in resultado["membros"] if n.endswith(".shp"))
        manifesto = {
            "dataset": DATASET,
            "file_name": f"{base_name}.zip",
            "snapshot_date": janela,
            "collected_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_url": URL,
            "source_record": REGISTRO_FONTE,
            "remote": resultado["metadados"],
            "members": [{"name": n, "bytes": b} for n, b in resultado["membros"]],
            "layers": camadas,
            "sha256": resultado["sha256"],
            "size_bytes": tamanho,
            "landing_batch_id": BATCH_ID,
        }
        with open(f"{VOLUME_PATH}/versao={janela}/{base_name}.manifest.json", "w", encoding="utf-8") as fh:
            json.dump(manifesto, fh, ensure_ascii=False, indent=2)

        registrar(janela, "SUCESSO", STARTED_AT, registros=len(resultado["membros"]), paginas=1,
                  arquivo=destino, sha256=resultado["sha256"], mensagem=f"camadas: {', '.join(camadas)}")
        print(f"OK: {len(resultado['membros'])} arquivos ({len(camadas)} camadas) gravados em {destino}")
except Exception as exc:
    registrar(janela, "FALHA", STARTED_AT, mensagem=str(exc))
    raise
