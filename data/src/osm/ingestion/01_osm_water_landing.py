# Databricks notebook source
# MAGIC %md
# MAGIC # 01_osm_water_landing
# MAGIC
# MAGIC Baixa da Geofabrik a hidrografia do OpenStreetMap da região Sudeste: cursos d'água (`gis_osm_waterways_free_1`: rios, córregos, canais e drenos) e corpos d'água (`gis_osm_water_a_free_1`: represas, lagos e áreas de rio). Grava no volume `fiap_analytics.bronze.osm_water`.
# MAGIC
# MAGIC É o mesmo extrato *free shapefile* das vias (`01_osm_roads_landing`): só as duas camadas são lidas do zip publicado, por requisições HTTP Range, e gravadas num zip por versão com um manifesto ao lado. Se o extrato não mudou desde a última coleta, não baixa de novo.
# MAGIC
# MAGIC Dados © colaboradores do OpenStreetMap, ODbL 1.0.

# COMMAND ----------

dbutils.widgets.text("url", "https://download.geofabrik.de/south-america/brazil/sudeste-latest-free.shp.zip",
                     "URL do extrato da Geofabrik")
dbutils.widgets.dropdown("forcar", "nao", ["nao", "sim"], "Baixar mesmo sem alteração na fonte")

# COMMAND ----------

import json
import os
import re
import sys
import tempfile
import uuid
from datetime import datetime, timezone

CATALOG, SCHEMA, VOLUME = "fiap_analytics", "bronze", "osm_water"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
DATASET = "osm_water"
CAMADAS = ("gis_osm_waterways_free_1", "gis_osm_water_a_free_1")
ATRIBUICAO = "© OpenStreetMap contributors, ODbL 1.0 (https://www.openstreetmap.org/copyright), extrato da Geofabrik"

URL = dbutils.widgets.get("url").strip()
FORCAR = dbutils.widgets.get("forcar") == "sim"
BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc)

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME} "
          "COMMENT 'Landing da hidrografia do OpenStreetMap (Geofabrik, região Sudeste, cursos e corpos d''água), "
          "um zip por versão coletada. © OpenStreetMap contributors, ODbL 1.0.'")

_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common.arquivos import copiar_para_volume, extrair_do_zip, metadados_remotos, ultimo_manifesto  # noqa: E402
from common.ingestion_log import IngestionLog  # noqa: E402

registrar = IngestionLog(spark, "fiap_analytics.bronze.osm_ingestion_log", "osm_water", "landing", BATCH_ID).registrar

# COMMAND ----------


def data_do_extrato(url_final):
    """Data do extrato da Geofabrik a partir do nome do arquivo (sudeste-261116-free.shp.zip -> 2026-11-16)."""
    achado = re.search(r"-(\d{2})(\d{2})(\d{2})-free", url_final)
    return f"20{achado.group(1)}-{achado.group(2)}-{achado.group(3)}" if achado else None


janela = STARTED_AT.strftime("%Y-%m-%d")
try:
    remoto = metadados_remotos(URL)
    extrato = data_do_extrato(remoto["url_final"]) or janela
    janela = extrato
    anterior = ultimo_manifesto(VOLUME_PATH)
    chave = ("etag", "tamanho", "ultima_modificacao")
    if anterior and not FORCAR and all(anterior["remote"].get(k) == remoto.get(k) for k in chave):
        msg = f"extrato sem alteração na fonte desde a coleta de {anterior['snapshot_date']}"
        registrar(janela, "SEM_DADOS", STARTED_AT, registros=0, mensagem=msg)
        print(f"Nada a baixar: {msg}")
    else:
        base_name = f"osm_sudeste_water_{extrato.replace('-', '')[2:]}"
        destino = f"{VOLUME_PATH}/versao={extrato}/{base_name}.zip"
        padrao = rf"^({'|'.join(re.escape(c) for c in CAMADAS)})\.(shp|shx|dbf|prj|cpg)$"
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, f"{base_name}.zip")
            resultado = extrair_do_zip(remoto["url_final"], padrao, local)
            tamanho = os.path.getsize(local)
            copiar_para_volume(local, destino)
        manifesto = {
            "dataset": DATASET, "file_name": f"{base_name}.zip", "snapshot_date": extrato,
            "collected_at_utc": datetime.now(timezone.utc).isoformat(), "source_url": URL, "layers": list(CAMADAS),
            "remote": remoto, "members": [{"name": n, "bytes": b} for n, b in resultado["membros"]],
            "sha256": resultado["sha256"], "size_bytes": tamanho, "license": ATRIBUICAO, "landing_batch_id": BATCH_ID,
        }
        with open(f"{VOLUME_PATH}/versao={extrato}/{base_name}.manifest.json", "w", encoding="utf-8") as fh:
            json.dump(manifesto, fh, ensure_ascii=False, indent=2)
        registrar(janela, "SUCESSO", STARTED_AT, registros=len(resultado["membros"]), paginas=1,
                  arquivo=destino, sha256=resultado["sha256"], mensagem=f"extrato {extrato}, camadas {', '.join(CAMADAS)}")
        print(f"OK: extrato {extrato}, {len(resultado['membros'])} arquivos gravados em {destino}")
except Exception as exc:
    registrar(janela, "FALHA", STARTED_AT, mensagem=str(exc))
    raise
