# Databricks notebook source
# MAGIC %md
# MAGIC # 03_ibge_silver_quality
# MAGIC
# MAGIC Regras de qualidade da Silver do IBGE (`fiap_analytics.silver.ibge_census_tracts`). Roda depois de `03_ibge_census_tracts_silver` e antes da Gold.
# MAGIC
# MAGIC 1. Regras `ERRO` que falham param a tarefa e o job: a Gold não é carregada com dados ruins.
# MAGIC 2. Regras `AVISO` só registram.
# MAGIC 3. Resultados em `fiap_analytics.bronze.ibge_data_quality_log`, uma linha por regra e execução.
# MAGIC
# MAGIC Referências do Censo 2022 para o município de São Paulo: 11.451.999 pessoas em 27.301 setores, 96 distritos e 1.521,2 km².

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

DQ_TABLE = "fiap_analytics.bronze.ibge_data_quality_log"
FONTE = "ibge"
ETAPA = "silver"
BATCH_ID = str(uuid.uuid4())

SILVER = "fiap_analytics.silver.ibge_census_tracts"
BRONZE = "fiap_analytics.bronze.ibge_census_tracts"

POPULACAO_CENSO = 11451999  # população do município no Censo 2022
DISTRITOS = 96

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras

# COMMAND ----------

regras = [
    # Volume e completude
    nao_vazia(SILVER),
    consulta(
        "linhas_iguais_bronze", SILVER, "A Silver tem os mesmos setores da Bronze",
        f"""SELECT ABS((SELECT COUNT(*) FROM {SILVER}) - (SELECT COUNT(*) FROM {BRONZE})) AS falhas,
                   (SELECT COUNT(*) FROM {BRONZE}) AS total""",
    ),

    # Identificação do setor
    nao_nulo(SILVER, "cd_setor"),
    unico(SILVER, ["cd_setor"]),
    condicao("cd_setor_15_digitos_do_municipio", SILVER, "Código do setor com 15 dígitos e começando pelo código do município",
             "NOT cd_setor RLIKE '^[0-9]{15}$' OR left(cd_setor, 7) <> cd_municipio"),
    condicao("municipio_sao_paulo", SILVER, "Setor do município de São Paulo (3550308)", "cd_municipio <> '3550308'"),
    consulta(
        "distritos_completos", SILVER, f"Os {DISTRITOS} distritos do município estão presentes",
        f"SELECT ABS({DISTRITOS} - COUNT(DISTINCT cd_distrito)) AS falhas, {DISTRITOS} AS total FROM {SILVER}",
    ),
    nao_nulo(SILVER, "dc_tipo_setor"),
    nao_nulo(SILVER, "dc_situacao_detalhada"),

    # Contagens do universo
    nao_nulo(SILVER, "nr_pessoas"),
    condicao("contagens_nao_negativas", SILVER, "Pessoas e domicílios maiores ou iguais a zero",
             "nr_pessoas < 0 OR nr_domicilios < 0 OR nr_domicilios_ocupados < 0"),
    condicao("domicilios_particulares_mais_coletivos", SILVER,
             "Total de domicílios igual a particulares mais coletivos (V0002 = V0003 + V0004)",
             "nr_domicilios <> nr_domicilios_particulares + nr_domicilios_coletivos"),
    condicao("ocupados_ate_particulares", SILVER, "Domicílios ocupados não passam dos particulares (V0007 <= V0003)",
             "nr_domicilios_ocupados > nr_domicilios_particulares"),
    condicao("proporcao_imputados_entre_0_e_1", SILVER, "Proporção de domicílios imputados entre 0 e 1",
             "nr_prop_domicilios_imputados NOT BETWEEN 0 AND 1"),
    consulta(
        "populacao_igual_censo", SILVER, f"Soma das pessoas igual à população do Censo 2022 ({POPULACAO_CENSO})",
        f"SELECT ABS(SUM(nr_pessoas) - {POPULACAO_CENSO}) AS falhas, {POPULACAO_CENSO} AS total FROM {SILVER}",
        severidade="AVISO",
    ),

    # Geometria
    condicao("srid_4674", SILVER, "Geometria de origem em SIRGAS 2000 geográfico (EPSG 4674)", "_source_srs_id <> 4674"),
    nao_nulo(SILVER, "geom_wkb"),
    condicao("area_positiva", SILVER, "Área maior que zero", "nr_area_km2 IS NULL OR nr_area_km2 <= 0"),
    condicao("area_igual_ibge", SILVER, "Área calculada igual à área do IBGE (tolerância de 0,1%: distorção do UTM)",
             "ABS(nr_area_km2 - nr_area_ibge_km2) > 0.001 * nr_area_ibge_km2", filtro="nr_area_ibge_km2 > 0"),
    condicao("envelope_na_area_de_sao_paulo", SILVER, "Setor dentro da área de São Paulo e entorno",
             "nr_lat_min < -24.1 OR nr_lat_max > -23.3 OR nr_lon_min < -47.0 OR nr_lon_max > -46.3"),
    consulta(
        "area_municipio", SILVER, "Soma das áreas dos setores igual à área do município (1.521 km², tolerância de 1%)",
        f"SELECT CASE WHEN ABS(SUM(nr_area_km2) - 1521) > 0.01 * 1521 THEN 1 ELSE 0 END AS falhas, 1 AS total FROM {SILVER}",
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
