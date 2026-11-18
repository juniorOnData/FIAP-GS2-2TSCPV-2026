# Databricks notebook source
# MAGIC %md
# MAGIC # 01_osm_roads_landing
# MAGIC
# MAGIC ## Função de versão
# MAGIC
# MAGIC `data_do_extrato` extrai a data do nome final do arquivo após o redirecionamento de `latest`. Essa versão é registrada no manifesto para identificar o extrato utilizado e evitar uma nova ingestão quando o arquivo não mudou.
# MAGIC
# MAGIC Baixa da Geofabrik a rede viária do OpenStreetMap da região Sudeste (camada `gis_osm_roads_free_1` do extrato *free shapefile*) e grava no volume `fiap_analytics.bronze.osm_roads`.
# MAGIC
# MAGIC Fonte: `https://download.geofabrik.de/south-america/brazil/sudeste-latest-free.shp.zip`. O endereço `latest` redireciona para o extrato do dia (`sudeste-AAMMDD-free.shp.zip`, cerca de 2,2 GB com todas as camadas).
# MAGIC
# MAGIC **Licença:** dados © colaboradores do OpenStreetMap, sob a Open Database License (ODbL 1.0). Todo produto feito com estes dados precisa trazer a atribuição "© OpenStreetMap contributors" e, se a base derivada for distribuída, mantê-la sob a ODbL.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Só a camada de vias (`.shp`, `.shx`, `.dbf`, `.prj` e `.cpg`, cerca de 530 MB comprimidos), lida direto do zip publicado por requisições HTTP Range. O conteúdo é gravado sem alteração num zip `versao=AAAA-MM-DD/osm_sudeste_roads_AAMMDD.zip`, com um manifesto ao lado (data do extrato, membros, SHA256, ETag e data de modificação na fonte).
# MAGIC 2. Se o extrato na fonte não mudou desde a última coleta (mesmo ETag, tamanho e data), nada é baixado e o log registra `SEM_DADOS`. Com `forcar = sim`, baixa de novo.
# MAGIC 3. Uma linha no `fiap_analytics.bronze.osm_ingestion_log` (etapa `landing`); `registros` é o número de arquivos extraídos e `janela` é a data do extrato da Geofabrik.

# COMMAND ----------

dbutils.widgets.text("url", "https://download.geofabrik.de/south-america/brazil/sudeste-latest-free.shp.zip",
                     "URL do extrato da Geofabrik")
dbutils.widgets.text("camada", "gis_osm_roads_free_1", "Camada do extrato")
dbutils.widgets.dropdown("forcar", "nao", ["nao", "sim"], "Baixar mesmo sem alteração na fonte")

# COMMAND ----------

import json
import os
import re
import sys
import tempfile
import uuid
from datetime import datetime, timezone

CATALOG, SCHEMA, VOLUME = "fiap_analytics", "bronze", "osm_roads"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
DATASET = "osm_roads"
ATRIBUICAO = "© OpenStreetMap contributors, ODbL 1.0 (https://www.openstreetmap.org/copyright), extrato da Geofabrik"

URL = dbutils.widgets.get("url").strip()
CAMADA = dbutils.widgets.get("camada").strip()
FORCAR = dbutils.widgets.get("forcar") == "sim"
BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc)

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME} "
          "COMMENT 'Landing da rede viária do OpenStreetMap (Geofabrik, região Sudeste, camada gis_osm_roads_free_1), "
          "um zip por versão coletada. © OpenStreetMap contributors, ODbL 1.0.'")

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

LOG_TABLE = "fiap_analytics.bronze.osm_ingestion_log"
FONTE = "osm_roads"
ETAPA = "landing"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coleta

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
        base_name = f"osm_sudeste_roads_{extrato.replace('-', '')[2:]}"
        destino = f"{VOLUME_PATH}/versao={extrato}/{base_name}.zip"
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, f"{base_name}.zip")
            # A URL final (com a data) garante que todos os pedaços venham do mesmo extrato.
            resultado = extrair_do_zip(remoto["url_final"], rf"^{re.escape(CAMADA)}\.(shp|shx|dbf|prj|cpg)$", local)
            tamanho = os.path.getsize(local)
            copiar_para_volume(local, destino)

        manifesto = {
            "dataset": DATASET,
            "file_name": f"{base_name}.zip",
            "snapshot_date": extrato,
            "collected_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_url": URL,
            "layer": CAMADA,
            "remote": remoto,
            "members": [{"name": n, "bytes": b} for n, b in resultado["membros"]],
            "sha256": resultado["sha256"],
            "size_bytes": tamanho,
            "license": ATRIBUICAO,
            "landing_batch_id": BATCH_ID,
        }
        with open(f"{VOLUME_PATH}/versao={extrato}/{base_name}.manifest.json", "w", encoding="utf-8") as fh:
            json.dump(manifesto, fh, ensure_ascii=False, indent=2)

        registrar(janela, "SUCESSO", STARTED_AT, registros=len(resultado["membros"]), paginas=1,
                  arquivo=destino, sha256=resultado["sha256"], mensagem=f"extrato {extrato}, camada {CAMADA}")
        print(f"OK: extrato {extrato}, {len(resultado['membros'])} arquivos gravados em {destino}")
except Exception as exc:
    registrar(janela, "FALHA", STARTED_AT, mensagem=str(exc))
    raise
