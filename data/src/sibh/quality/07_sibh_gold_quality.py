# Databricks notebook source
# MAGIC %md
# MAGIC # 07_sibh_gold_quality
# MAGIC
# MAGIC Regras de qualidade da Gold do SIBH: as fatos `fact_sibh_rainfall_hourly` e `fact_sibh_level_hourly` e a dimensão `dim_sibh_sensors`. Roda depois das duas fatos.
# MAGIC
# MAGIC 1. Regras `ERRO` que falham param a tarefa e o job.
# MAGIC 2. Regras `AVISO` só registram.
# MAGIC 3. Resultados em `fiap_analytics.bronze.sibh_data_quality_log`, uma linha por regra e execução.

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

DQ_TABLE = "fiap_analytics.bronze.sibh_data_quality_log"
FONTE = "sibh"
ETAPA = "gold"
BATCH_ID = str(uuid.uuid4())

MEDICOES = "fiap_analytics.silver.sibh_measurements"
FATO_CHUVA = "fiap_analytics.gold.fact_sibh_rainfall_hourly"
FATO_NIVEL = "fiap_analytics.gold.fact_sibh_level_hourly"
DIM_TEMPO = "fiap_analytics.gold.dim_tempo"
DIM_SENSOR = "fiap_analytics.gold.dim_sibh_sensors"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras

# COMMAND ----------


def silver_por_mes(grandeza):
    """Resumo mensal da Silver no mesmo critério das fatos (mês da medição no fuso de São Paulo)."""
    return f"""
        SELECT CAST(date_format(dt_medicao, 'yyyyMM') AS INT) AS mes, COUNT(*) AS medicoes, SUM(nr_valor) AS valor
        FROM {MEDICOES} WHERE dc_grandeza = '{grandeza}' AND dt_hr_medicao_utc IS NOT NULL GROUP BY 1"""


def regras_da_fato(fato):
    """Regras comuns às duas fatos: volume, chave, conciliação de medições e integridade."""
    grandeza = "Chuva" if fato == FATO_CHUVA else "Nível"
    return [
        nao_vazia(fato),
        unico(fato, ["COD_SENSOR", "COD_DIM_TEMPO", "NUM_HORA"], nome="hora_unica_por_sensor"),
        consulta(
            "medicoes_iguais_silver_por_mes", fato, f"Por mês, a soma de NUM_MEDICOES é igual às medições de {grandeza} da Silver",
            f"""SELECT COUNT_IF(NOT (s.medicoes <=> g.medicoes)) AS falhas, COUNT(*) AS total
                FROM ({silver_por_mes(grandeza)}) s
                FULL OUTER JOIN (SELECT NUM_ANO_MES AS mes, SUM(NUM_MEDICOES) AS medicoes FROM {fato} GROUP BY 1) g
                  ON s.mes = g.mes""",
        ),
        existe_em(fato, "COD_DIM_TEMPO", DIM_TEMPO),
        existe_em(fato, "COD_DIM_SENSOR", DIM_SENSOR),
        condicao("sensor_nao_informado", fato, "Horas de sensores fora do cadastro (-1)",
                 "COD_DIM_SENSOR = -1", severidade="AVISO", limite=0.01),
    ]


regras = [
    # Fato de chuva
    *regras_da_fato(FATO_CHUVA),
    consulta(
        "chuva_igual_silver_por_mes", FATO_CHUVA,
        "Por mês, a chuva da fato é igual à da Silver (tolerância do arredondamento de 3 casas por hora)",
        f"""SELECT COUNT_IF(s.mes IS NULL OR g.mes IS NULL
                            OR ABS(COALESCE(s.valor, 0) - COALESCE(g.chuva, 0)) > 0.0005 * g.horas + 0.01) AS falhas,
                   COUNT(*) AS total
            FROM ({silver_por_mes('Chuva')}) s
            FULL OUTER JOIN (SELECT NUM_ANO_MES AS mes, SUM(NUM_CHUVA_MM) AS chuva, COUNT(*) AS horas
                             FROM {FATO_CHUVA} GROUP BY 1) g ON s.mes = g.mes""",
    ),
    condicao("chuva_horaria_nao_negativa", FATO_CHUVA, "Chuva da hora maior ou igual a zero", "NUM_CHUVA_MM < 0"),
    condicao("chuva_horaria_acima_de_120mm", FATO_CHUVA, "Horas com mais de 120 mm (valor fora do comum)",
             "NUM_CHUVA_MM > 120", severidade="AVISO"),

    # Fato de nível
    *regras_da_fato(FATO_NIVEL),
    condicao("nivel_min_menor_ou_igual_max", FATO_NIVEL, "Nível mínimo, médio e máximo da hora coerentes",
             "NOT (NUM_NIVEL_MIN <= NUM_NIVEL_MEDIO + 0.0001 AND NUM_NIVEL_MEDIO <= NUM_NIVEL_MAX + 0.0001)"),
    consulta(
        "sensor_de_nivel", FATO_NIVEL, "Horas na fato de nível de sensores cadastrados como chuva",
        f"""SELECT COUNT_IF(d.DSC_GRANDEZA = 'Chuva') AS falhas, COUNT(*) AS total
            FROM {FATO_NIVEL} f LEFT JOIN {DIM_SENSOR} d ON f.COD_DIM_SENSOR = d.COD_DIM_SENSOR""",
        severidade="AVISO",
    ),

    # Dimensão de sensores
    unico(DIM_SENSOR, ["COD_DIM_SENSOR"]),
    unico(DIM_SENSOR, ["COD_SENSOR"]),
    consulta(
        "membro_desconhecido", DIM_SENSOR, "A dimensão tem exatamente uma linha -1 (não informado)",
        f"SELECT CASE WHEN COUNT_IF(COD_DIM_SENSOR = -1) = 1 THEN 0 ELSE 1 END AS falhas, 1 AS total FROM {DIM_SENSOR}",
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
