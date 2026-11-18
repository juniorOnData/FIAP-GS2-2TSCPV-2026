# Databricks notebook source
# MAGIC %md
# MAGIC # 05_sgb_gold_quality
# MAGIC
# MAGIC ## Função de validação
# MAGIC
# MAGIC `membro_desconhecido` verifica se cada dimensão mantém exatamente um registro com chave substituta `-1`, usado para representar membros desconhecidos ou sem correspondência.
# MAGIC
# MAGIC Regras de qualidade da Gold do SGB: `dim_sgb_susceptibility_class` e `dim_sgb_susceptibility_areas`. Roda depois de `05_dim_sgb_susceptibility_areas`.
# MAGIC
# MAGIC 1. Regras `ERRO` que falham param a tarefa e o job.
# MAGIC 2. Regras `AVISO` só registram.
# MAGIC 3. Resultados em `fiap_analytics.bronze.sgb_data_quality_log`, uma linha por regra e execução.

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

DQ_TABLE = "fiap_analytics.bronze.sgb_data_quality_log"
FONTE = "sgb"
ETAPA = "gold"
BATCH_ID = str(uuid.uuid4())

SILVER = "fiap_analytics.silver.sgb_susceptibility_areas"
DIM_CLASSE = "fiap_analytics.gold.dim_sgb_susceptibility_class"
DIM_AREA = "fiap_analytics.gold.dim_sgb_susceptibility_areas"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras

# COMMAND ----------


def membro_desconhecido(dim, chave):
    return consulta(
        "membro_desconhecido", dim, "A dimensão tem exatamente uma linha -1 (não informado)",
        f"SELECT CASE WHEN COUNT_IF({chave} = -1) = 1 THEN 0 ELSE 1 END AS falhas, 1 AS total FROM {dim}",
    )


regras = [
    # Dimensão de classes
    nao_vazia(DIM_CLASSE),
    unico(DIM_CLASSE, ["COD_DIM_CLASSE"]),
    unico(DIM_CLASSE, ["COD_CAMADA", "DSC_CLASSE"], nome="classe_unica_na_camada"),
    membro_desconhecido(DIM_CLASSE, "COD_DIM_CLASSE"),
    condicao("grau_coerente", DIM_CLASSE, "Grau 3, 2 e 1 para as classes Alta, Média e Baixa",
             "NOT (NUM_GRAU_SUSCETIBILIDADE <=> CASE DSC_CLASSE WHEN 'Alta' THEN 3 WHEN 'Média' THEN 2 WHEN 'Baixa' THEN 1 END)",
             filtro="COD_DIM_CLASSE <> -1"),
    nao_nulo(DIM_CLASSE, "DSC_CARACTERISTICAS", severidade="AVISO",
             filtro="COD_CAMADA IN ('inundacao', 'bacia_inundacao', 'movimento_massa')"),

    # Dimensão de áreas: volume e conciliação com a Silver
    nao_vazia(DIM_AREA),
    consulta(
        "areas_iguais_silver_por_camada", DIM_AREA,
        "Por camada, a dimensão tem as mesmas feições e a mesma área total da Silver",
        f"""SELECT COUNT_IF(NOT (s.n <=> g.n) OR ABS(COALESCE(s.area, 0) - COALESCE(g.area, 0)) > 1e-6) AS falhas,
                   COUNT(*) AS total
            FROM (SELECT cd_camada AS camada, COUNT(*) AS n, SUM(nr_area_km2) AS area FROM {SILVER} GROUP BY 1) s
            FULL OUTER JOIN (SELECT COD_CAMADA AS camada, COUNT(*) AS n, SUM(NUM_AREA_KM2) AS area
                             FROM {DIM_AREA} WHERE COD_DIM_AREA_SUSCETIBILIDADE <> -1 GROUP BY 1) g
              ON s.camada = g.camada""",
    ),
    unico(DIM_AREA, ["COD_DIM_AREA_SUSCETIBILIDADE"]),
    unico(DIM_AREA, ["COD_CAMADA", "NUM_ID_FEICAO"], nome="feicao_unica_na_camada"),
    membro_desconhecido(DIM_AREA, "COD_DIM_AREA_SUSCETIBILIDADE"),

    # Dimensão de áreas: integridade e geometria
    existe_em(DIM_AREA, "COD_DIM_CLASSE", DIM_CLASSE),
    condicao("classe_nao_informada", DIM_AREA, "Áreas sem classe identificada (-1)",
             "COD_DIM_CLASSE = -1", filtro="COD_DIM_AREA_SUSCETIBILIDADE <> -1"),
    nao_nulo(DIM_AREA, "GEO_WKB", filtro="COD_DIM_AREA_SUSCETIBILIDADE <> -1"),
    condicao("centroide_no_envelope", DIM_AREA, "Centroide dentro do envelope da área",
             "NUM_CENTROIDE_LATITUDE NOT BETWEEN NUM_LAT_MIN AND NUM_LAT_MAX "
             "OR NUM_CENTROIDE_LONGITUDE NOT BETWEEN NUM_LON_MIN AND NUM_LON_MAX",
             filtro="COD_DIM_AREA_SUSCETIBILIDADE <> -1"),
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
