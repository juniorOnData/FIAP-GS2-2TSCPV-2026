# Databricks notebook source
# MAGIC %md
# MAGIC # 03_cemaden_silver_quality
# MAGIC
# MAGIC Regras de qualidade das Silvers do CEMADEN (`fiap_analytics.silver.cemaden_stations` e `fiap_analytics.silver.cemaden_rainfall_readings`). Roda depois das duas Silvers e antes da Gold.
# MAGIC
# MAGIC 1. Regras `ERRO` que falham param a tarefa e o job: a Gold não é carregada com dados ruins.
# MAGIC 2. Regras `AVISO` só registram.
# MAGIC 3. Resultados em `fiap_analytics.bronze.cemaden_data_quality_log`, uma linha por regra e execução.
# MAGIC
# MAGIC As regras `data_hora_no_mes_da_janela` e `leitura_acima_de_50mm` também conferem as premissas da Silver (fonte em UTC e valor em mm por leitura): se elas reprovarem muito, a premissa precisa ser revista.

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

DQ_TABLE = "fiap_analytics.bronze.cemaden_data_quality_log"
FONTE = "cemaden"
ETAPA = "silver"
BATCH_ID = str(uuid.uuid4())

ESTACOES = "fiap_analytics.silver.cemaden_stations"
LEITURAS = "fiap_analytics.silver.cemaden_rainfall_readings"
BRONZE_LEITURAS = "fiap_analytics.bronze.cemaden_rainfall_readings"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras

# COMMAND ----------

regras = [
    # Estações
    nao_vazia(ESTACOES),
    unico(ESTACOES, ["cod_estacao"]),
    nao_nulo(ESTACOES, "nr_latitude"),
    nao_nulo(ESTACOES, "nr_longitude"),
    condicao("ponto_na_area_de_sao_paulo", ESTACOES, "Estação dentro da área de São Paulo e entorno",
             "nr_latitude NOT BETWEEN -24.1 AND -23.3 OR nr_longitude NOT BETWEEN -47.0 AND -46.3"),

    # Leituras: volume e conciliação com a Bronze
    nao_vazia(LEITURAS),
    consulta(
        "linhas_iguais_chaves_bronze", LEITURAS,
        "A Silver tem uma linha para cada estação, sensor e data e hora distintos da Bronze",
        f"""SELECT ABS((SELECT COUNT(*) FROM {LEITURAS})
                       - (SELECT COUNT(*) FROM (SELECT 1 FROM {BRONZE_LEITURAS} GROUP BY codestacao, id_sensor, datahora))) AS falhas,
                   (SELECT COUNT(*) FROM {LEITURAS}) AS total""",
    ),

    # Leituras: chave (estação e sensor são obrigatórios na fato)
    nao_nulo(LEITURAS, "cod_estacao"),
    nao_nulo(LEITURAS, "cod_sensor"),

    # Leituras: data e hora
    nao_nulo(LEITURAS, "dt_hr_leitura_utc"),
    condicao("data_no_periodo", LEITURAS, "Leitura entre 2020-01-01 UTC (início do período do projeto) e agora",
             "dt_hr_leitura_utc < TIMESTAMP'2020-01-01 00:00:00' OR dt_hr_leitura_utc > current_timestamp() + INTERVAL 1 HOUR"),
    condicao("data_hora_no_mes_da_janela", LEITURAS, "Data e hora em UTC dentro do mês pedido à API (confere a premissa de UTC)",
             "date_format(dt_hr_leitura_utc, 'yyyyMM') <> _window", severidade="AVISO", limite=0.01),

    # Leituras: valor
    nao_nulo(LEITURAS, "nr_chuva_mm", severidade="AVISO", limite=0.01),
    condicao("chuva_nao_negativa", LEITURAS, "Chuva maior ou igual a zero", "nr_chuva_mm < 0"),
    condicao("leitura_acima_de_50mm", LEITURAS, "Leituras com mais de 50 mm (valor fora do comum para uma leitura)",
             "nr_chuva_mm > 50", severidade="AVISO", limite=0.0001),

    # Leituras: estação
    existe_em(LEITURAS, "cod_estacao", ESTACOES, severidade="AVISO", limite=0.01),
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
