# Databricks notebook source
# MAGIC %md
# MAGIC # 05_inmet_gold_quality
# MAGIC
# MAGIC Regras de qualidade da Gold do INMET: a fato `fact_inmet_weather_hourly` e a dimensão `dim_inmet_stations`. Roda depois de `05_fact_inmet_weather_hourly`.
# MAGIC
# MAGIC 1. Regras `ERRO` que falham param a tarefa e o job.
# MAGIC 2. Regras `AVISO` só registram.
# MAGIC 3. Resultados em `fiap_analytics.bronze.inmet_data_quality_log`, uma linha por regra e execução.

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

DQ_TABLE = "fiap_analytics.bronze.inmet_data_quality_log"
FONTE = "inmet"
ETAPA = "gold"
BATCH_ID = str(uuid.uuid4())

MEDICOES = "fiap_analytics.silver.inmet_weather_readings"
FACT = "fiap_analytics.gold.fact_inmet_weather_hourly"
DIM_TEMPO = "fiap_analytics.gold.dim_tempo"
DIM_ESTACAO = "fiap_analytics.gold.dim_inmet_stations"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras

# COMMAND ----------

# Resumo mensal da Silver no mesmo critério da fato (mês no fuso de São Paulo, a partir do início da hora).
SILVER_POR_MES = f"""
    SELECT CAST(date_format(dt_medicao, 'yyyyMM') AS INT) AS mes, COUNT(*) AS horas, SUM(nr_chuva_mm) AS chuva
    FROM {MEDICOES} WHERE dt_hr_medicao_utc IS NOT NULL GROUP BY 1"""
FATO_POR_MES = f"""
    SELECT NUM_ANO_MES AS mes, COUNT(*) AS horas, SUM(NUM_CHUVA_MM) AS chuva FROM {FACT} GROUP BY 1"""

regras = [
    # Fato: volume, chave e conciliação com a Silver
    nao_vazia(FACT),
    unico(FACT, ["COD_ESTACAO", "COD_DIM_TEMPO", "NUM_HORA"], nome="hora_unica_por_estacao"),
    consulta(
        "horas_e_chuva_iguais_silver_por_mes", FACT, "Por mês, a fato tem as mesmas horas e a mesma chuva da Silver",
        f"""SELECT COUNT_IF(NOT (s.horas <=> g.horas) OR ABS(COALESCE(s.chuva, 0) - COALESCE(g.chuva, 0)) > 0.001) AS falhas,
                   COUNT(*) AS total
            FROM ({SILVER_POR_MES}) s FULL OUTER JOIN ({FATO_POR_MES}) g ON s.mes = g.mes""",
    ),

    # Fato: integridade referencial (chave nula também conta como falha)
    existe_em(FACT, "COD_DIM_TEMPO", DIM_TEMPO),
    existe_em(FACT, "COD_DIM_ESTACAO", DIM_ESTACAO),
    condicao("estacao_nao_informada", FACT, "Horas de estações fora do cadastro (-1)",
             "COD_DIM_ESTACAO = -1", severidade="AVISO"),

    # Fato: valores
    condicao("chuva_horaria_nao_negativa", FACT, "Chuva da hora maior ou igual a zero", "NUM_CHUVA_MM < 0"),
    condicao("temperatura_min_ate_max", FACT, "Temperatura mínima da hora não passa da máxima",
             "NUM_TEMPERATURA_MIN_C > NUM_TEMPERATURA_MAX_C", severidade="AVISO", limite=0.001),
    consulta(
        "horas_por_estacao_e_dia", FACT, "Nenhuma estação com mais de 24 horas num dia",
        f"""SELECT COUNT(*) AS falhas, (SELECT COUNT(*) FROM (SELECT 1 FROM {FACT} GROUP BY COD_ESTACAO, COD_DIM_TEMPO)) AS total
            FROM (SELECT 1 FROM {FACT} GROUP BY COD_ESTACAO, COD_DIM_TEMPO HAVING COUNT(*) > 24)""",
    ),

    # Dimensão de estações
    nao_vazia(DIM_ESTACAO),
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
