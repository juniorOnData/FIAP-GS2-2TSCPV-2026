# Databricks notebook source
# MAGIC %md
# MAGIC # 03_sgb_silver_quality
# MAGIC
# MAGIC Regras de qualidade da Silver do SGB (`fiap_analytics.silver.sgb_susceptibility_areas`). Roda depois de `03_sgb_susceptibility_areas_silver` e antes da Gold.
# MAGIC
# MAGIC 1. Regras `ERRO` que falham param a tarefa e o job: a Gold não é carregada com dados ruins.
# MAGIC 2. Regras `AVISO` só registram.
# MAGIC 3. Resultados em `fiap_analytics.bronze.sgb_data_quality_log`, uma linha por regra e execução.
# MAGIC
# MAGIC A regra `area_igual_fonte` confere a leitura do shapefile e a conversão de coordenadas: a área calculada precisa bater com a `Shape_Area` gravada pela própria fonte. A regra `area_movimento_massa_igual_quadro` compara a área de cada classe com o quadro-síntese da carta. No quadro de inundação a soma dos polígonos não bate com a área publicada, por isso essa camada fica de fora.

# COMMAND ----------

import os
import sys
import uuid

# Módulos de src/common. Sobe pelas pastas até achar src/ para importar.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common.data_quality import DataQuality, condicao, consulta, nao_nulo, nao_vazia, unico  # noqa: E402

DQ_TABLE = "fiap_analytics.bronze.sgb_data_quality_log"
FONTE = "sgb"
ETAPA = "silver"
BATCH_ID = str(uuid.uuid4())

SILVER = "fiap_analytics.silver.sgb_susceptibility_areas"
BRONZE = "fiap_analytics.bronze.sgb_susceptibility_areas"

CAMADAS_ESPERADAS = ["inundacao", "bacia_inundacao", "enxurrada", "movimento_massa", "corrida_massa", "relevo"]
CAMADAS_GRADUADAS = ["inundacao", "bacia_inundacao", "movimento_massa"]
# Área por classe no quadro-síntese de movimentos de massa da carta (km2).
QUADRO_MOVIMENTO_MASSA = {"Alta": 100.1, "Média": 231.2, "Baixa": 1196.2}


def lista_sql(valores):
    return ", ".join(f"'{v}'" for v in valores)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras

# COMMAND ----------

quadro = " UNION ALL ".join(f"SELECT '{c}' AS classe, {a} AS area" for c, a in QUADRO_MOVIMENTO_MASSA.items())

regras = [
    # Volume e completude
    nao_vazia(SILVER),
    consulta(
        "linhas_iguais_bronze", SILVER, "A Silver tem as mesmas feições da Bronze",
        f"""SELECT ABS((SELECT COUNT(*) FROM {SILVER}) - (SELECT COUNT(*) FROM {BRONZE})) AS falhas,
                   (SELECT COUNT(*) FROM {BRONZE}) AS total""",
    ),
    consulta(
        "camadas_esperadas", SILVER, f"As camadas {', '.join(CAMADAS_ESPERADAS)} estão presentes",
        f"""SELECT {len(CAMADAS_ESPERADAS)} - COUNT(DISTINCT cd_camada) AS falhas, {len(CAMADAS_ESPERADAS)} AS total
            FROM {SILVER} WHERE cd_camada IN ({lista_sql(CAMADAS_ESPERADAS)})""",
    ),
    condicao("camada_conhecida", SILVER, "Feições de camadas que a Silver não conhece (shapefile novo na carta)",
             f"cd_camada NOT IN ({lista_sql(CAMADAS_ESPERADAS)})", severidade="AVISO"),
    unico(SILVER, ["cd_camada", "nr_id_feicao"], nome="feicao_unica_na_camada"),

    # Classe
    condicao("classe_valida", SILVER, "Classe Alta, Média ou Baixa nas camadas graduadas",
             "dc_classe IS NULL OR dc_classe NOT IN ('Alta', 'Média', 'Baixa')",
             filtro=f"cd_camada IN ({lista_sql(CAMADAS_GRADUADAS)})"),
    nao_nulo(SILVER, "dc_classe"),

    # Geometria
    condicao("srid_31983", SILVER, "Geometria de origem em SIRGAS 2000 / UTM 23S (EPSG 31983)", "_source_srs_id <> 31983"),
    nao_nulo(SILVER, "geom_wkb"),
    condicao("area_positiva", SILVER, "Área maior que zero", "nr_area_km2 IS NULL OR nr_area_km2 <= 0"),
    condicao("area_igual_fonte", SILVER, "Área calculada igual à Shape_Area da fonte (tolerância de 0,01%)",
             "ABS(nr_area_km2 - nr_area_fonte_km2) > 0.0001 * nr_area_fonte_km2",
             filtro="nr_area_fonte_km2 IS NOT NULL"),
    condicao("envelope_na_area_de_sao_paulo", SILVER, "Feição dentro da área de São Paulo e entorno (exceto as bacias contribuintes)",
             "nr_lat_min < -24.1 OR nr_lat_max > -23.3 OR nr_lon_min < -47.0 OR nr_lon_max > -46.3",
             filtro="cd_camada <> 'bacia_inundacao'"),
    consulta(
        "area_municipio_coberta", SILVER,
        "Movimento de massa e relevo cobrem a área do município (1.521 km², tolerância de 2%)",
        f"""SELECT COUNT_IF(ABS(area - 1521) > 0.02 * 1521) AS falhas, COUNT(*) AS total
            FROM (SELECT cd_camada, SUM(nr_area_km2) AS area FROM {SILVER}
                  WHERE cd_camada IN ('movimento_massa', 'relevo') GROUP BY cd_camada)""",
    ),
    consulta(
        "area_movimento_massa_igual_quadro", SILVER,
        "Área de cada classe de movimento de massa igual à do quadro-síntese da carta (tolerância de 1%)",
        f"""SELECT COUNT_IF(s.area IS NULL OR ABS(s.area - q.area) > 0.01 * q.area) AS falhas, COUNT(*) AS total
            FROM ({quadro}) q
            LEFT JOIN (SELECT dc_classe, SUM(nr_area_km2) AS area FROM {SILVER}
                       WHERE cd_camada = 'movimento_massa' GROUP BY dc_classe) s ON q.classe = s.dc_classe""",
        severidade="AVISO",
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução

# COMMAND ----------

dq = DataQuality(spark, DQ_TABLE, FONTE, ETAPA, BATCH_ID)
resultados = dq.executar(regras)
display(dq.resultados_df(resultados))

# COMMAND ----------

dq.validar(resultados)
