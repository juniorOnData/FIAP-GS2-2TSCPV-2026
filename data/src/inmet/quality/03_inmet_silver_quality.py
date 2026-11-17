# Databricks notebook source
# MAGIC %md
# MAGIC # 03_inmet_silver_quality
# MAGIC
# MAGIC Regras de qualidade das Silvers do INMET (`fiap_analytics.silver.inmet_stations` e `fiap_analytics.silver.inmet_weather_readings`). Roda depois das duas Silvers e antes da Gold.
# MAGIC
# MAGIC 1. Regras `ERRO` que falham param a tarefa e o job: a Gold não é carregada com dados ruins.
# MAGIC 2. Regras `AVISO` só registram.
# MAGIC 3. Resultados em `fiap_analytics.bronze.inmet_data_quality_log`, uma linha por regra e execução.
# MAGIC
# MAGIC As faixas físicas (temperatura, umidade, pressão) são as plausíveis para São Paulo, a cerca de 800 m de altitude. Fora delas o valor é suspeito, mas o INMET não marca o dado, então essas regras só geram aviso.

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

DQ_TABLE = "fiap_analytics.bronze.inmet_data_quality_log"
FONTE = "inmet"
ETAPA = "silver"
BATCH_ID = str(uuid.uuid4())

ESTACOES = "fiap_analytics.silver.inmet_stations"
MEDICOES = "fiap_analytics.silver.inmet_weather_readings"
BRONZE = "fiap_analytics.bronze.inmet_weather_readings"

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

    # Medições: volume e conciliação com a Bronze
    nao_vazia(MEDICOES),
    consulta(
        "linhas_iguais_chaves_bronze", MEDICOES,
        "A Silver tem uma linha para cada estação, data e hora distintas da Bronze",
        f"""SELECT ABS((SELECT COUNT(*) FROM {MEDICOES})
                       - (SELECT COUNT(*) FROM (SELECT 1 FROM {BRONZE} GROUP BY upper(trim(estacao_codigo)), data, hora_utc))) AS falhas,
                   (SELECT COUNT(*) FROM {MEDICOES}) AS total""",
    ),
    nao_nulo(MEDICOES, "cod_estacao"),
    existe_em(MEDICOES, "cod_estacao", ESTACOES),

    # Medições: data e hora
    nao_nulo(MEDICOES, "dt_hr_medicao_utc"),
    condicao("data_no_periodo", MEDICOES, "Medição entre 2020-01-01 UTC (início do período do projeto) e agora",
             "dt_hr_medicao_utc < TIMESTAMP'2020-01-01 00:00:00' OR dt_hr_medicao_utc > current_timestamp() + INTERVAL 1 HOUR"),
    condicao("data_no_ano_da_janela", MEDICOES, "Medição dentro do ano do arquivo",
             "CAST(year(dt_hr_medicao_utc) AS STRING) <> _window", severidade="AVISO", limite=0.001),
    condicao("hora_cheia", MEDICOES, "Medições em hora cheia (minuto zero)", "minute(dt_hr_medicao_utc) <> 0"),

    # Medições: valores
    nao_nulo(MEDICOES, "nr_chuva_mm", severidade="AVISO", limite=0.10),
    condicao("chuva_nao_negativa", MEDICOES, "Chuva maior ou igual a zero", "nr_chuva_mm < 0"),
    condicao("chuva_acima_de_100mm_na_hora", MEDICOES, "Horas com mais de 100 mm (valor fora do comum)",
             "nr_chuva_mm > 100", severidade="AVISO"),
    condicao("temperatura_plausivel", MEDICOES, "Temperatura entre -5 e 45 °C",
             "nr_temperatura_c NOT BETWEEN -5 AND 45", severidade="AVISO"),
    condicao("umidade_entre_0_e_100", MEDICOES, "Umidade relativa entre 0 e 100%",
             "nr_umidade_pct NOT BETWEEN 0 AND 100", severidade="AVISO"),
    condicao("pressao_plausivel", MEDICOES, "Pressão na estação entre 850 e 1000 mb",
             "nr_pressao_mb NOT BETWEEN 850 AND 1000", severidade="AVISO"),
    condicao("vento_nao_negativo", MEDICOES, "Velocidade e rajada do vento maiores ou iguais a zero",
             "nr_vento_velocidade_ms < 0 OR nr_vento_rajada_ms < 0"),
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
