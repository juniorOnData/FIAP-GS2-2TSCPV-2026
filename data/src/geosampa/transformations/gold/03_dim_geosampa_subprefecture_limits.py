# Databricks notebook source
# MAGIC %md
# MAGIC # 03_dim_geosampa_subprefecture_limits
# MAGIC
# MAGIC Geometria das subprefeituras na Gold: uma linha por polígono do shapefile do GeoSampa, ligada à `dim_geosampa_subprefecture_area` pela sigla.
# MAGIC
# MAGIC 1. Sem a Bronze dos limites (o shapefile ainda não foi enviado ao Volume), termina sem erro e não cria a tabela.
# MAGIC 2. O campo do shapefile que traz o nome ou a sigla é descoberto comparando os valores com o cadastro (nome, sigla do Anexo IX e sigla usada nas ocorrências), sem acento, caixa ou pontuação (`src/common/nomes.py`). Vale o campo que casa com mais subprefeituras.
# MAGIC 3. Polígono que não casa com nenhuma subprefeitura, ou subprefeitura do cadastro sem polígono, falha a tarefa: limite errado no mapa é pior que limite ausente.
# MAGIC 4. A tabela é recriada a cada carga (são 32 subprefeituras).

# COMMAND ----------

import json
import os
import sys

from pyspark.sql import functions as F

_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if _src not in sys.path:
    sys.path.append(_src)
from common.nomes import campo_que_melhor_casa, normalizar_nome  # noqa: E402

BRONZE = "fiap_analytics.bronze.geosampa_subprefecture_limits"
DIM = "fiap_analytics.gold.dim_geosampa_subprefecture_area"
TABLE = "fiap_analytics.gold.dim_geosampa_subprefecture_limits"

if not spark.catalog.tableExists(BRONZE):
    dbutils.notebook.exit("Sem limites na Bronze: envie o shapefile das subprefeituras ao Volume.")

# COMMAND ----------

subs = (spark.table(DIM).where((F.col("COD_DIM_SUBPREFEITURA") != -1) & (F.col("TIP_UNIDADE") == "SUBPREFEITURA"))
        .select("SIG_SUBPREFEITURA", "NOM_SUBPREFEITURA", "SIG_SUBPREFEITURA_GEOSAMPA").collect())
chave_para_sigla = {}
for s in subs:
    for valor in (s["NOM_SUBPREFEITURA"], s["SIG_SUBPREFEITURA"], s["SIG_SUBPREFEITURA_GEOSAMPA"]):
        if valor:
            chave_para_sigla[normalizar_nome(valor)] = s["SIG_SUBPREFEITURA"]
nome_por_sigla = {s["SIG_SUBPREFEITURA"]: s["NOM_SUBPREFEITURA"] for s in subs}

bronze = spark.table(BRONZE).collect()
atributos = [json.loads(r["ds_atributos_json"]) for r in bronze]
campo, casados = campo_que_melhor_casa(atributos, set(chave_para_sigla))
if campo is None:
    raise RuntimeError(f"Nenhum campo do shapefile casa com as subprefeituras do cadastro. Campos: {sorted({k for a in atributos for k in a})}")
print(f"Campo usado para casar: {campo} ({casados} de {len(bronze)} polígonos)")

linhas, sem_par = [], []
for r, a in zip(bronze, atributos):
    sigla = chave_para_sigla.get(normalizar_nome(a.get(campo)))
    if sigla is None:
        sem_par.append(a.get(campo))
        continue
    linhas.append((sigla, nome_por_sigla[sigla], r["nr_id_feicao"], r["nr_area_km2"], r["nr_centroide_latitude"], r["nr_centroide_longitude"],
                   r["nr_lat_min"], r["nr_lat_max"], r["nr_lon_min"], r["nr_lon_max"], r["geom_wkb"], f"{r['_source_file']} ({r['_source_sha256'][:12]})"))
faltando = sorted(set(nome_por_sigla) - {l[0] for l in linhas})
if sem_par or faltando:
    raise RuntimeError(f"Limites inconsistentes com o cadastro. Polígonos sem subprefeitura: {sem_par}. Subprefeituras sem polígono: {faltando}.")
area = sum(l[3] or 0 for l in linhas)
if not 1400 <= area <= 1650:
    raise RuntimeError(f"Área total dos limites fora do esperado: {area:.1f} km² (o município tem cerca de 1.521 km²).")

# COMMAND ----------

colunas = ["SIG_SUBPREFEITURA", "NOM_SUBPREFEITURA", "NUM_ID_FEICAO", "NUM_AREA_KM2", "NUM_CENTROIDE_LATITUDE", "NUM_CENTROIDE_LONGITUDE",
           "NUM_LAT_MIN", "NUM_LAT_MAX", "NUM_LON_MIN", "NUM_LON_MAX", "GEO_WKB", "DSC_VERSAO_FONTE"]
(spark.createDataFrame(linhas, f"{colunas[0]} STRING, {colunas[1]} STRING, {colunas[2]} INT, {colunas[3]} DOUBLE, {colunas[4]} DOUBLE, "
                              f"{colunas[5]} DOUBLE, {colunas[6]} DOUBLE, {colunas[7]} DOUBLE, {colunas[8]} DOUBLE, {colunas[9]} DOUBLE, "
                              f"{colunas[10]} BINARY, {colunas[11]} STRING")
 .withColumn("DAT_CARGA_DIMENSAO", F.current_timestamp())
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE))
spark.sql(f"COMMENT ON TABLE {TABLE} IS 'Limites das subprefeituras de Sao Paulo (GeoSampa), um poligono por linha, ligados a dim_geosampa_subprefecture_area pela sigla; GEO_WKB em EPSG 4674'")
print(f"{len(linhas)} polígonos de {len(nome_por_sigla)} subprefeituras; {area:.1f} km²")
