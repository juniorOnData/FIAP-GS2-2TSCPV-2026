# Databricks notebook source
# MAGIC %md
# MAGIC # 01_ibge_census_tracts_landing
# MAGIC
# MAGIC Baixa do IBGE a malha de setores censitários do Censo Demográfico 2022 com os atributos básicos do universo (pessoas e domicílios por setor) do estado de São Paulo e grava no volume `fiap_analytics.bronze.ibge_census_tracts`.
# MAGIC
# MAGIC Fonte: `SP_setores_CD2022.gpkg` da pasta *Agregados por Setores Censitários / malha com atributos* do FTP do IBGE (GeoPackage, cerca de 180 MB). Um só arquivo traz o polígono do setor e as variáveis V0001 a V0007 (total de pessoas, domicílios, domicílios particulares e coletivos, média de moradores, percentual de domicílios imputados e domicílios particulares ocupados).
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Arquivo gravado exatamente como veio, em `versao=AAAA-MM-DD/SP_setores_CD2022.gpkg`, com um manifesto ao lado (setores no arquivo e no município, SHA256, ETag e data de modificação na fonte).
# MAGIC 2. O Censo é um retrato de 2022: se o arquivo não mudou na fonte desde a última coleta (mesmo ETag, tamanho e data), nada é baixado e o log registra `SEM_DADOS`. Com `forcar = sim`, baixa de novo. O IBGE já republicou arquivos do Censo com correções, por isso a conferência é periódica.
# MAGIC 3. O arquivo precisa ter setores do município `cod_ibge` (3550308), senão a coleta falha.
# MAGIC 4. Uma linha no `fiap_analytics.bronze.ibge_ingestion_log` (etapa `landing`); `registros` é o número de setores do estado no arquivo.

# COMMAND ----------

dbutils.widgets.text(
    "url",
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/malha_com_atributos/setores/gpkg/UF/SP/SP_setores_CD2022.gpkg",
    "URL da malha de setores com atributos (IBGE)")
dbutils.widgets.text("cod_ibge", "3550308", "Município usado na conferência")
dbutils.widgets.dropdown("forcar", "nao", ["nao", "sim"], "Baixar mesmo sem alteração na fonte")

# COMMAND ----------

import json
import os
import sqlite3
import sys
import tempfile
import uuid
from datetime import datetime, timezone

CATALOG, SCHEMA, VOLUME = "fiap_analytics", "bronze", "ibge_census_tracts"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
DATASET = "ibge_census_tracts"

URL = dbutils.widgets.get("url").strip()
COD_IBGE = dbutils.widgets.get("cod_ibge").strip()
FORCAR = dbutils.widgets.get("forcar") == "sim"
BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc)

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME} "
          "COMMENT 'Landing da malha de setores censitários do Censo 2022 com atributos (IBGE), estado de São Paulo, "
          "um GeoPackage por versão coletada.'")

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
from common import geo  # noqa: E402
from common.arquivos import baixar, copiar_para_volume, metadados_remotos, ultimo_manifesto  # noqa: E402
from common.ingestion_log import IngestionLog  # noqa: E402

LOG_TABLE = "fiap_analytics.bronze.ibge_ingestion_log"
FONTE = "ibge_census_tracts"
ETAPA = "landing"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coleta

# COMMAND ----------

def contar_setores(caminho):
    """(camada, setores no arquivo, setores do município COD_IBGE)."""
    camadas = geo.camadas_gpkg(caminho)
    if len(camadas) != 1:
        raise RuntimeError(f"Esperava uma camada de feições, o arquivo tem {[c[0] for c in camadas]}")
    camada, _, _, atributos = camadas[0]
    if "CD_MUN" not in atributos or "CD_SETOR" not in atributos:
        raise RuntimeError(f"Colunas CD_MUN e CD_SETOR não encontradas: {atributos}")
    conn = sqlite3.connect(f"file:{caminho}?mode=ro", uri=True)
    try:
        total = conn.execute(f'SELECT COUNT(*) FROM "{camada}"').fetchone()[0]
        municipio = conn.execute(f'SELECT COUNT(*) FROM "{camada}" WHERE CD_MUN = ?', (COD_IBGE,)).fetchone()[0]
    finally:
        conn.close()
    return camada, total, municipio, atributos


janela = STARTED_AT.strftime("%Y-%m-%d")
try:
    remoto = metadados_remotos(URL)
    anterior = ultimo_manifesto(VOLUME_PATH)
    chave = ("etag", "tamanho", "ultima_modificacao")
    if anterior and not FORCAR and all(anterior["remote"].get(k) == remoto.get(k) for k in chave):
        msg = f"arquivo sem alteração na fonte desde a coleta de {anterior['snapshot_date']}"
        registrar(janela, "SEM_DADOS", STARTED_AT, registros=0, mensagem=msg)
        print(f"Nada a baixar: {msg}")
    else:
        nome = os.path.basename(URL)
        destino = f"{VOLUME_PATH}/versao={janela}/{nome}"
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, nome)
            sha256, tamanho = baixar(URL, local)
            camada, total, municipio, atributos = contar_setores(local)
            if municipio == 0:
                raise RuntimeError(f"Nenhum setor do município {COD_IBGE} no arquivo")
            copiar_para_volume(local, destino)

        manifesto = {
            "dataset": DATASET,
            "file_name": nome,
            "snapshot_date": janela,
            "collected_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_url": URL,
            "remote": remoto,
            "layer": camada,
            "fields": atributos,
            "record_count": total,
            "municipality_code": COD_IBGE,
            "municipality_count": municipio,
            "sha256": sha256,
            "size_bytes": tamanho,
            "landing_batch_id": BATCH_ID,
        }
        with open(f"{VOLUME_PATH}/versao={janela}/{os.path.splitext(nome)[0]}.manifest.json", "w", encoding="utf-8") as fh:
            json.dump(manifesto, fh, ensure_ascii=False, indent=2)

        registrar(janela, "SUCESSO", STARTED_AT, registros=total, paginas=1, arquivo=destino, sha256=sha256,
                  mensagem=f"setores no município {COD_IBGE}: {municipio}")
        print(f"OK: {total} setores do estado ({municipio} no município) gravados em {destino}")
except Exception as exc:
    registrar(janela, "FALHA", STARTED_AT, mensagem=str(exc))
    raise
