# Databricks notebook source
# MAGIC %md
# MAGIC # 05_sibh_silver_quality
# MAGIC
# MAGIC Regras de qualidade das Silvers do SIBH (`fiap_analytics.silver.sibh_stations` e `fiap_analytics.silver.sibh_measurements`). Roda depois das duas Silvers e antes da Gold.
# MAGIC
# MAGIC 1. Regras `ERRO` que falham param a tarefa e o job: a Gold não é carregada com dados ruins.
# MAGIC 2. Regras `AVISO` só registram.
# MAGIC 3. Resultados em `fiap_analytics.bronze.sibh_data_quality_log`, uma linha por regra e execução.
# MAGIC
# MAGIC As regras `data_hora_no_mes_da_janela` e `chuva_acima_de_50mm` também conferem as premissas da Silver (fonte em UTC e chuva em mm por medição).

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

spark.conf.set("spark.sql.session.timeZone", "UTC")

DQ_TABLE = "fiap_analytics.bronze.sibh_data_quality_log"
FONTE = "sibh"
ETAPA = "silver"
BATCH_ID = str(uuid.uuid4())

SENSORES = "fiap_analytics.silver.sibh_stations"
MEDICOES = "fiap_analytics.silver.sibh_measurements"
BRONZE_MEDICOES = "fiap_analytics.bronze.sibh_measurements"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras

# COMMAND ----------

regras = [
    # Sensores
    nao_vazia(SENSORES),
    unico(SENSORES, ["cod_sensor"]),
    nao_nulo(SENSORES, "dc_grandeza"),
    nao_nulo(SENSORES, "nr_latitude"),
    nao_nulo(SENSORES, "nr_longitude"),
    condicao("ponto_na_area_de_sao_paulo", SENSORES, "Sensor dentro da área de São Paulo e entorno",
             "nr_latitude NOT BETWEEN -24.1 AND -23.3 OR nr_longitude NOT BETWEEN -47.0 AND -46.3"),

    # Medições: volume e conciliação com a Bronze
    nao_vazia(MEDICOES),
    consulta(
        "linhas_iguais_chaves_bronze", MEDICOES,
        "A Silver tem uma linha para cada sensor e data e hora distintos da Bronze",
        f"""SELECT ABS((SELECT COUNT(*) FROM {MEDICOES})
                       - (SELECT COUNT(*) FROM (SELECT 1 FROM {BRONZE_MEDICOES} GROUP BY station_prefix_id, date))) AS falhas,
                   (SELECT COUNT(*) FROM {MEDICOES}) AS total""",
    ),

    # Medições: chave e grandeza (obrigatórias nas fatos)
    nao_nulo(MEDICOES, "cod_sensor"),
    nao_nulo(MEDICOES, "dc_grandeza"),

    # Medições: data e hora
    nao_nulo(MEDICOES, "dt_hr_medicao_utc"),
    condicao("data_no_periodo", MEDICOES, "Medição entre 2010-01-01 (início da dim_tempo) e agora",
             "dt_medicao < DATE'2010-01-01' OR dt_hr_medicao_utc > current_timestamp() + INTERVAL 1 HOUR"),
    condicao("data_hora_no_mes_da_janela", MEDICOES, "Data e hora em UTC dentro do mês pedido à API (confere a premissa de UTC)",
             "date_format(dt_hr_medicao_utc, 'yyyy-MM') <> _window", severidade="AVISO", limite=0.01),

    # Medições: valor
    nao_nulo(MEDICOES, "nr_valor", severidade="AVISO", limite=0.01),
    condicao("chuva_nao_negativa", MEDICOES, "Chuva maior ou igual a zero (negativos da fonte ficam nulos na Silver)",
             "dc_grandeza = 'Chuva' AND nr_valor < 0"),
    condicao("chuva_negativa_na_fonte", MEDICOES, "Medições de chuva recebidas com valor negativo (anuladas na Silver; original em nr_valor_origem)",
             "fl_valor_invalido", severidade="AVISO", limite=0.0001),
    condicao("chuva_acima_de_50mm", MEDICOES, "Medições de chuva com mais de 50 mm (valor fora do comum para uma medição)",
             "dc_grandeza = 'Chuva' AND nr_valor > 50", severidade="AVISO", limite=0.0001),

    # Medições: sensor
    existe_em(MEDICOES, "cod_sensor", SENSORES, severidade="AVISO", limite=0.01),
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
