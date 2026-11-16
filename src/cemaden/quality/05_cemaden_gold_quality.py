# Databricks notebook source
# MAGIC %md
# MAGIC # 05_cemaden_gold_quality
# MAGIC
# MAGIC Regras de qualidade da Gold do CEMADEN: a fato `fact_cemaden_rainfall_hourly` e a dimensão `dim_cemaden_stations`. Roda depois de `05_fact_cemaden_rainfall_hourly`.
# MAGIC
# MAGIC 1. Regras `ERRO` que falham param a tarefa e o job.
# MAGIC 2. Regras `AVISO` só registram.
# MAGIC 3. Resultados em `fiap_analytics.bronze.cemaden_data_quality_log`, uma linha por regra e execução.

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
from common.data_quality import DataQuality, condicao, consulta, existe_em, nao_vazia, unico  # noqa: E402

DQ_TABLE = "fiap_analytics.bronze.cemaden_data_quality_log"
FONTE = "cemaden"
ETAPA = "gold"
BATCH_ID = str(uuid.uuid4())

LEITURAS = "fiap_analytics.silver.cemaden_rainfall_readings"
FACT = "fiap_analytics.gold.fact_cemaden_rainfall_hourly"
DIM_TEMPO = "fiap_analytics.gold.dim_tempo"
DIM_ESTACAO = "fiap_analytics.gold.dim_cemaden_stations"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras

# COMMAND ----------

# Resumo mensal da Silver no mesmo critério da fato (mês da leitura no fuso de São Paulo).
SILVER_POR_MES = f"""
    SELECT CAST(date_format(dt_leitura, 'yyyyMM') AS INT) AS mes, COUNT(*) AS leituras, SUM(nr_chuva_mm) AS chuva
    FROM {LEITURAS} WHERE dt_hr_leitura_utc IS NOT NULL GROUP BY 1"""
FATO_POR_MES = f"""
    SELECT NUM_ANO_MES AS mes, SUM(NUM_LEITURAS) AS leituras, SUM(NUM_CHUVA_MM) AS chuva, COUNT(*) AS horas
    FROM {FACT} GROUP BY 1"""

regras = [
    # Fato: volume, chave e conciliação com a Silver
    nao_vazia(FACT),
    unico(FACT, ["COD_ESTACAO", "COD_SENSOR", "COD_DIM_TEMPO", "NUM_HORA"], nome="hora_unica_por_sensor"),
    consulta(
        "leituras_iguais_silver_por_mes", FACT, "Por mês, a soma de NUM_LEITURAS é igual às leituras da Silver",
        f"""SELECT COUNT_IF(NOT (s.leituras <=> g.leituras)) AS falhas, COUNT(*) AS total
            FROM ({SILVER_POR_MES}) s FULL OUTER JOIN ({FATO_POR_MES}) g ON s.mes = g.mes""",
    ),
    consulta(
        "chuva_igual_silver_por_mes", FACT,
        "Por mês, a chuva da fato é igual à da Silver (tolerância do arredondamento de 3 casas por hora)",
        f"""SELECT COUNT_IF(s.mes IS NULL OR g.mes IS NULL
                            OR ABS(COALESCE(s.chuva, 0) - COALESCE(g.chuva, 0)) > 0.0005 * g.horas + 0.01) AS falhas,
                   COUNT(*) AS total
            FROM ({SILVER_POR_MES}) s FULL OUTER JOIN ({FATO_POR_MES}) g ON s.mes = g.mes""",
    ),

    # Fato: integridade referencial (chave nula também conta como falha)
    existe_em(FACT, "COD_DIM_TEMPO", DIM_TEMPO),
    existe_em(FACT, "COD_DIM_ESTACAO", DIM_ESTACAO),
    condicao("estacao_nao_informada", FACT, "Horas de estações fora do cadastro (-1)",
             "COD_DIM_ESTACAO = -1", severidade="AVISO", limite=0.01),

    # Fato: valores
    condicao("chuva_horaria_nao_negativa", FACT, "Chuva da hora maior ou igual a zero", "NUM_CHUVA_MM < 0"),
    condicao("chuva_horaria_acima_de_120mm", FACT, "Horas com mais de 120 mm (valor fora do comum)",
             "NUM_CHUVA_MM > 120", severidade="AVISO"),

    # Dimensão de estações
    unico(DIM_ESTACAO, ["COD_DIM_ESTACAO"]),
    unico(DIM_ESTACAO, ["COD_ESTACAO"]),
    consulta(
        "membro_desconhecido", DIM_ESTACAO, "A dimensão tem exatamente uma linha -1 (não informado)",
        f"SELECT CASE WHEN COUNT_IF(COD_DIM_ESTACAO = -1) = 1 THEN 0 ELSE 1 END AS falhas, 1 AS total FROM {DIM_ESTACAO}",
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
