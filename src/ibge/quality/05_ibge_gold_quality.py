# Databricks notebook source
# MAGIC %md
# MAGIC # 05_ibge_gold_quality
# MAGIC
# MAGIC Regras de qualidade da Gold do IBGE: a fato `fact_ibge_census_tract_population`, a dimensão `dim_ibge_census_tracts` e a chave na `dim_tempo`. Roda depois de `05_fact_ibge_census_tract_population`.
# MAGIC
# MAGIC 1. Regras `ERRO` que falham param a tarefa e o job.
# MAGIC 2. Regras `AVISO` só registram.
# MAGIC 3. Resultados em `fiap_analytics.bronze.ibge_data_quality_log`, uma linha por regra e execução.

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
from common.data_quality import DataQuality, condicao, consulta, existe_em, nao_nulo, nao_vazia, unico  # noqa: E402

DQ_TABLE = "fiap_analytics.bronze.ibge_data_quality_log"
FONTE = "ibge"
ETAPA = "gold"
BATCH_ID = str(uuid.uuid4())

SILVER = "fiap_analytics.silver.ibge_census_tracts"
FACT = "fiap_analytics.gold.fact_ibge_census_tract_population"
DIM_SETOR = "fiap_analytics.gold.dim_ibge_census_tracts"
DIM_TEMPO = "fiap_analytics.gold.dim_tempo"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras

# COMMAND ----------

regras = [
    # Fato: volume e conciliação com a Silver
    nao_vazia(FACT),
    consulta(
        "setores_e_pessoas_iguais_silver", FACT, "A fato do Censo 2022 tem os mesmos setores e as mesmas pessoas da Silver",
        f"""SELECT CASE WHEN s.n = g.n AND s.pessoas <=> g.pessoas AND s.domicilios <=> g.domicilios THEN 0 ELSE 1 END AS falhas,
                   1 AS total
            FROM (SELECT COUNT(*) AS n, SUM(nr_pessoas) AS pessoas, SUM(nr_domicilios) AS domicilios FROM {SILVER}) s
            CROSS JOIN (SELECT COUNT(*) AS n, SUM(NUM_PESSOAS) AS pessoas, SUM(NUM_DOMICILIOS) AS domicilios
                        FROM {FACT} WHERE COD_DIM_TEMPO = 20220731) g""",
    ),
    unico(FACT, ["COD_SETOR", "COD_DIM_TEMPO"], nome="setor_unico_por_data"),

    # Fato: integridade referencial (chave nula também conta como falha)
    existe_em(FACT, "COD_DIM_TEMPO", DIM_TEMPO),
    existe_em(FACT, "COD_DIM_SETOR", DIM_SETOR),
    condicao("setor_nao_informado", FACT, "Setores fora da dimensão (-1)", "COD_DIM_SETOR = -1"),

    # Fato: medidas
    condicao("pessoas_nao_negativas", FACT, "Pessoas e domicílios maiores ou iguais a zero",
             "NUM_PESSOAS < 0 OR NUM_DOMICILIOS < 0"),
    condicao("densidade_coerente", FACT, "Densidade igual a pessoas dividido pela área",
             "ABS(NUM_DENSIDADE_HAB_KM2 - NUM_PESSOAS / NUM_AREA_KM2) > 1e-6 * GREATEST(NUM_DENSIDADE_HAB_KM2, 1)",
             filtro="NUM_AREA_KM2 > 0"),

    # Dimensão de setores
    nao_vazia(DIM_SETOR),
    unico(DIM_SETOR, ["COD_DIM_SETOR"]),
    unico(DIM_SETOR, ["COD_SETOR"]),
    consulta(
        "membro_desconhecido", DIM_SETOR, "A dimensão tem exatamente uma linha -1 (não informado)",
        f"SELECT CASE WHEN COUNT_IF(COD_DIM_SETOR = -1) = 1 THEN 0 ELSE 1 END AS falhas, 1 AS total FROM {DIM_SETOR}",
    ),
    consulta(
        "setores_iguais_silver", DIM_SETOR, "A dimensão tem os mesmos setores da Silver",
        f"""SELECT COUNT(*) AS falhas, (SELECT COUNT(*) FROM {SILVER}) AS total
            FROM (SELECT cd_setor FROM {SILVER}
                  EXCEPT SELECT COD_SETOR FROM {DIM_SETOR} WHERE COD_DIM_SETOR <> -1
                  UNION ALL
                  (SELECT COD_SETOR FROM {DIM_SETOR} WHERE COD_DIM_SETOR <> -1 EXCEPT SELECT cd_setor FROM {SILVER}))""",
    ),
    nao_nulo(DIM_SETOR, "GEO_WKB", filtro="COD_DIM_SETOR <> -1"),
    condicao("centroide_no_envelope", DIM_SETOR, "Centroide dentro do envelope do setor",
             "NUM_CENTROIDE_LATITUDE NOT BETWEEN NUM_LAT_MIN AND NUM_LAT_MAX "
             "OR NUM_CENTROIDE_LONGITUDE NOT BETWEEN NUM_LON_MIN AND NUM_LON_MAX",
             filtro="COD_DIM_SETOR <> -1"),
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
