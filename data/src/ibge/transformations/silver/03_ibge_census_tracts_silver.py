# Databricks notebook source
# MAGIC %md
# MAGIC # 03_ibge_census_tracts_silver
# MAGIC
# MAGIC ## Funções de conversão
# MAGIC
# MAGIC `codigo` normaliza códigos textuais e marcadores de ausência; `inteiro` e `decimal` convertem contagens e medidas; `arredondado` aplica a precisão definida para os campos numéricos publicados na Silver.
# MAGIC
# MAGIC Recria a Silver dos setores censitários do Censo 2022 no município de São Paulo (`fiap_analytics.silver.ibge_census_tracts`) a partir da Bronze: uma linha por setor, com códigos e nomes padronizados, contagens tipadas e geometria padronizada.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Códigos `.` (sem bairro, sem favela...) viram nulo. A situação detalhada (`CD_SIT`) e o tipo do setor (`CD_TIPO`) ganham a descrição do dicionário de dados do IBGE. `fl_favela` marca os setores do tipo 1 (Favela e Comunidade Urbana).
# MAGIC 2. Variáveis do universo: V0001 (pessoas), V0002 (domicílios), V0003 (domicílios particulares), V0004 (domicílios coletivos), V0005 (média de moradores em domicílios particulares ocupados, 2 casas), V0006 (proporção de domicílios particulares ocupados imputados, de 0 a 1, 4 casas) e V0007 (domicílios particulares ocupados).
# MAGIC 3. Geometria: a Bronze está em SIRGAS 2000 geográfico (EPSG 4674), o padrão das fontes geoespaciais do projeto, e `geom_wkb` é mantido assim. Área, centroide e envelope são calculados com `src/common/geo.py` (área em UTM 23S). `nr_area_ibge_km2` é a área publicada pelo IBGE; a densidade (`nr_densidade_hab_km2`) usa essa área.
# MAGIC 4. A conversão roda no driver (cerca de 27 mil setores).

# COMMAND ----------

import os
import sys

from pyspark.sql import types as T

# Módulos de src/common. Sobe pelas pastas até achar src/ para importar.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common import geo  # noqa: E402

BRONZE = "fiap_analytics.bronze.ibge_census_tracts"
SILVER = "fiap_analytics.silver.ibge_census_tracts"

# Dicionário de dados da malha com atributos (Dicionario_de_dados_malha_agregados.xlsx).
SITUACAO_DETALHADA = {
    "1": "Área urbana de alta densidade de edificações de cidade ou vila",
    "2": "Área urbana de baixa densidade de edificações de cidade ou vila",
    "3": "Núcleo urbano",
    "5": "Aglomerado rural - Povoado",
    "6": "Aglomerado rural - Núcleo rural",
    "7": "Aglomerado rural - Lugarejo",
    "8": "Área rural (exclusive aglomerados)",
    "9": "Massas de água",
}
TIPO_SETOR = {
    "0": "Não especial",
    "1": "Favela e Comunidade Urbana",
    "2": "Quartel e base militar",
    "3": "Alojamento / acampamento",
    "4": "Setor com baixo patamar domiciliar",
    "5": "Agrupamento indígena",
    "6": "Unidade prisional",
    "7": "Convento / hospital / ILPI / IACA",
    "8": "Agrovila do PA",
    "9": "Agrupamento quilombola",
}

# COMMAND ----------


def codigo(valor):
    valor = (valor or "").strip()
    return None if valor in ("", ".") else valor


def inteiro(valor):
    try:
        return int(float(valor)) if valor not in (None, "") else None
    except ValueError:
        return None


def decimal(valor):
    try:
        return float(valor) if valor not in (None, "") else None
    except ValueError:
        return None


def arredondado(valor, casas):
    """V0005 e V0006 vêm em ponto flutuante de 32 bits (2.4000000953674316): arredonda para as casas publicadas."""
    numero = decimal(valor)
    return round(numero, casas) if numero is not None else None


linhas = spark.table(BRONZE).collect()
print(f"{len(linhas)} setores na Bronze")

saida = []
for r in linhas:
    g = geo.descrever(r["geom_wkb"], r["_source_srs_id"]) or {}
    cd_sit, cd_tipo = codigo(r["cd_sit"]), codigo(r["cd_tipo"])
    area_ibge, pessoas = decimal(r["area_km2"]), inteiro(r["v0001"])
    saida.append((
        codigo(r["cd_setor"]), r["situacao"], cd_sit, SITUACAO_DETALHADA.get(cd_sit), cd_tipo, TIPO_SETOR.get(cd_tipo),
        cd_tipo == "1",
        codigo(r["cd_mun"]), r["nm_mun"], codigo(r["cd_dist"]), r["nm_dist"], codigo(r["cd_subdist"]), r["nm_subdist"],
        codigo(r["cd_bairro"]), r["nm_bairro"], codigo(r["cd_fcu"]), r["nm_fcu"], codigo(r["cd_nu"]), r["nm_nu"],
        pessoas, inteiro(r["v0002"]), inteiro(r["v0003"]), inteiro(r["v0004"]), arredondado(r["v0005"], 2),
        arredondado(r["v0006"], 4), inteiro(r["v0007"]),
        area_ibge, g.get("area_km2"),
        pessoas / area_ibge if pessoas is not None and area_ibge else None,
        g.get("geom_wkb"), g.get("tipo"), g.get("pontos"),
        g.get("centroide_lat"), g.get("centroide_lon"),
        g.get("lat_min"), g.get("lat_max"), g.get("lon_min"), g.get("lon_max"),
        r["_source_srs_id"], r["_source_sha256"], r["_source_version"], r["_ingested_at"],
    ))

SCHEMA_SILVER = T.StructType([
    T.StructField("cd_setor", T.StringType()),
    T.StructField("dc_situacao", T.StringType()),
    T.StructField("cd_situacao_detalhada", T.StringType()),
    T.StructField("dc_situacao_detalhada", T.StringType()),
    T.StructField("cd_tipo_setor", T.StringType()),
    T.StructField("dc_tipo_setor", T.StringType()),
    T.StructField("fl_favela", T.BooleanType()),
    T.StructField("cd_municipio", T.StringType()),
    T.StructField("nm_municipio", T.StringType()),
    T.StructField("cd_distrito", T.StringType()),
    T.StructField("nm_distrito", T.StringType()),
    T.StructField("cd_subdistrito", T.StringType()),
    T.StructField("nm_subdistrito", T.StringType()),
    T.StructField("cd_bairro", T.StringType()),
    T.StructField("nm_bairro", T.StringType()),
    T.StructField("cd_favela", T.StringType()),
    T.StructField("nm_favela", T.StringType()),
    T.StructField("cd_nucleo_urbano", T.StringType()),
    T.StructField("nm_nucleo_urbano", T.StringType()),
    T.StructField("nr_pessoas", T.LongType()),
    T.StructField("nr_domicilios", T.LongType()),
    T.StructField("nr_domicilios_particulares", T.LongType()),
    T.StructField("nr_domicilios_coletivos", T.LongType()),
    T.StructField("nr_media_moradores", T.DoubleType()),
    T.StructField("nr_prop_domicilios_imputados", T.DoubleType()),
    T.StructField("nr_domicilios_ocupados", T.LongType()),
    T.StructField("nr_area_ibge_km2", T.DoubleType()),
    T.StructField("nr_area_km2", T.DoubleType()),
    T.StructField("nr_densidade_hab_km2", T.DoubleType()),
    T.StructField("geom_wkb", T.BinaryType()),
    T.StructField("dc_tipo_geometria", T.StringType()),
    T.StructField("nr_vertices", T.IntegerType()),
    T.StructField("nr_centroide_latitude", T.DoubleType()),
    T.StructField("nr_centroide_longitude", T.DoubleType()),
    T.StructField("nr_lat_min", T.DoubleType()),
    T.StructField("nr_lat_max", T.DoubleType()),
    T.StructField("nr_lon_min", T.DoubleType()),
    T.StructField("nr_lon_max", T.DoubleType()),
    T.StructField("_source_srs_id", T.IntegerType()),
    T.StructField("_source_sha256", T.StringType()),
    T.StructField("_source_version", T.StringType()),
    T.StructField("_ingested_at", T.TimestampType()),
])

(spark.createDataFrame(saida, SCHEMA_SILVER)
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER))
spark.sql(f"""
    COMMENT ON TABLE {SILVER} IS
    'Silver dos setores censitários do Censo 2022 (IBGE) no município de São Paulo: uma linha por setor, com situação, tipo, distrito, favela, pessoas e domicílios, geometria em EPSG 4674, área, densidade, centroide e envelope.'
""")

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*) AS setores, SUM(nr_pessoas) AS pessoas, SUM(nr_domicilios) AS domicilios,
           COUNT_IF(fl_favela) AS setores_favela, SUM(IF(fl_favela, nr_pessoas, 0)) AS pessoas_favela,
           ROUND(SUM(nr_area_ibge_km2), 1) AS area_ibge_km2, ROUND(SUM(nr_area_km2), 1) AS area_calculada_km2,
           COUNT(DISTINCT cd_distrito) AS distritos, COUNT_IF(geom_wkb IS NULL) AS sem_geometria
    FROM {SILVER}
"""))
