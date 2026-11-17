# Databricks notebook source
# MAGIC %md
# MAGIC # 02_sgb_susceptibility_bronze
# MAGIC
# MAGIC Carrega na Bronze (`fiap_analytics.bronze.sgb_susceptibility_areas`) as camadas da carta de suscetibilidade do SGB gravadas no volume `fiap_analytics.bronze.sgb_susceptibility`.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Lê a coleta mais recente do volume (manifesto). Se ela já está na tabela (mesmo SHA256), não faz nada. Com `recarregar = sim`, carrega de novo.
# MAGIC 2. Cada shapefile do zip é uma camada (`_source_layer`). Cada feição vira uma linha: atributos do `.dbf` em texto, com o nome em minúsculas (as camadas grafam `Classe` e `CLASSE`), e a geometria em WKB (`geom_wkb`), no sistema do arquivo (`_source_srs_id`, lido do `.prj`; SIRGAS 2000 / UTM 23S, EPSG 31983).
# MAGIC 3. `_source_feature_id` é a posição da feição no shapefile e, com a camada, identifica a feição dentro da versão.
# MAGIC 4. Uma versão por vez: a tabela é substituída pela versão nova (o histórico das versões fica no volume).
# MAGIC 5. Conferência por camada: feições lidas precisam bater com as gravadas. Uma linha no `sgb_ingestion_log`.

# COMMAND ----------

dbutils.widgets.dropdown("recarregar", "nao", ["nao", "sim"], "Recarregar a versão já carregada")

# COMMAND ----------

import os
import sys
import tempfile
import uuid
import zipfile
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG, SCHEMA, VOLUME = "fiap_analytics", "bronze", "sgb_susceptibility"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
TABLE = f"{CATALOG}.{SCHEMA}.sgb_susceptibility_areas"
RECARREGAR = dbutils.widgets.get("recarregar") == "sim"
BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc)

# Módulos de src/common. Sobe pelas pastas até achar src/ para importar.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common import geo  # noqa: E402
from common.arquivos import ultimo_manifesto  # noqa: E402
from common.ingestion_log import IngestionLog  # noqa: E402

LOG_TABLE = "fiap_analytics.bronze.sgb_ingestion_log"
FONTE = "sgb_susceptibility"
ETAPA = "bronze"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

manifesto = ultimo_manifesto(VOLUME_PATH)
if manifesto is None:
    raise RuntimeError(f"Nenhuma coleta em {VOLUME_PATH}. Rode antes o 01_sgb_susceptibility_landing.")

carregada = (spark.catalog.tableExists(TABLE)
             and not spark.table(TABLE).filter(F.col("_source_sha256") == manifesto["sha256"]).isEmpty())
print(f"Versão mais recente: {manifesto['snapshot_date']} ({manifesto['sha256'][:12]}) | já carregada: {carregada}")

# COMMAND ----------


def ler_camadas(zip_path):
    """[(camada, membro, srid, n_feicoes, DataFrame)] de cada shapefile do zip."""
    saida = []
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(zip_path) as z:
            for nome in z.namelist():
                alvo = os.path.realpath(os.path.join(tmp, nome))
                if not alvo.startswith(os.path.realpath(tmp) + os.sep):
                    raise ValueError(f"Caminho suspeito no zip: {nome}")
            z.extractall(tmp)
            membros = sorted(n for n in z.namelist() if n.lower().endswith(".shp"))
        for membro in membros:
            caminho = os.path.join(tmp, membro)
            camada = os.path.splitext(os.path.basename(membro))[0]
            srid = geo.srid_shapefile(caminho)
            if srid is None:
                raise RuntimeError(f"Sistema de coordenadas não reconhecido no .prj de {membro}")
            registros = list(geo.ler_shapefile(caminho))
            campos = sorted({k for _, attrs, _ in registros for k in attrs}, key=str.lower)
            colunas = [c.lower() for c in campos]
            if len(set(colunas)) != len(colunas):
                raise RuntimeError(f"{membro}: campos que só diferem em maiúsculas: {campos}")
            schema = T.StructType(
                [T.StructField("_source_feature_id", T.IntegerType())]
                + [T.StructField(c, T.StringType()) for c in colunas]
                + [T.StructField("geom_wkb", T.BinaryType())]
            )
            linhas = [(i,) + tuple(attrs.get(c) for c in campos) + (wkb,) for i, attrs, wkb in registros]
            df = (
                spark.createDataFrame(linhas, schema)
                .withColumn("_source_layer", F.lit(camada))
                .withColumn("_source_member", F.lit(membro))
                .withColumn("_source_srs_id", F.lit(srid).cast("int"))
            )
            saida.append((camada, membro, srid, len(linhas), df))
            print(f"LIDO: {membro}: {len(linhas)} feições (EPSG {srid})")
    return saida


if carregada and not RECARREGAR:
    print("Nada novo para carregar.")
else:
    try:
        camadas = ler_camadas(manifesto["data_path"])
        if not camadas:
            raise RuntimeError(f"Nenhum shapefile em {manifesto['data_path']}")
        bronze = (
            reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [df for *_, df in camadas])
            .withColumn("_source_file", F.lit(manifesto["data_path"]))
            .withColumn("_source_sha256", F.lit(manifesto["sha256"]))
            .withColumn("_source_version", F.lit(manifesto["snapshot_date"]))
            .withColumn("_batch_id", F.lit(BATCH_ID))
            .withColumn("_ingested_at", F.current_timestamp())
        )
        (bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE))
        spark.sql(f"COMMENT ON TABLE {TABLE} IS 'Feições brutas da carta de suscetibilidade a movimentos de massa e "
                  "inundações do município de São Paulo (SGB/CPRM e IPT, 2015), uma linha por feição de cada camada, "
                  "atributos em texto e geometria em WKB (EPSG 31983).'")

        gravadas = {r["_source_layer"]: r["n"] for r in
                    spark.table(TABLE).groupBy("_source_layer").agg(F.count("*").alias("n")).collect()}
        for camada, membro, _, n, _ in camadas:
            assert gravadas.get(camada) == n, f"{membro}: lidas {n}, gravadas {gravadas.get(camada)}"
    except Exception as exc:
        registrar(manifesto["snapshot_date"], "FALHA", STARTED_AT, arquivo=manifesto["data_path"],
                  sha256=manifesto["sha256"], mensagem=str(exc))
        raise

    total = sum(n for _, _, _, n, _ in camadas)
    resumo = "; ".join(f"{c}={n}" for c, _, _, n, _ in camadas)
    registrar(manifesto["snapshot_date"], "SUCESSO", STARTED_AT, registros=total, arquivo=manifesto["data_path"],
              sha256=manifesto["sha256"], mensagem=resumo)
    print(f"OK: {total} feições gravadas e conferidas em {TABLE} ({resumo})")

display(spark.table(TABLE).groupBy("_source_layer", "_source_srs_id", "_source_version")
        .agg(F.count("*").alias("feicoes")).orderBy("_source_layer"))
