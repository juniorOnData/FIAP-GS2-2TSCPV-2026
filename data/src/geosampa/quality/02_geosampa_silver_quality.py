# Databricks notebook source
# MAGIC %md
# MAGIC # 02_geosampa_silver_quality
# MAGIC
# MAGIC Regras de qualidade da Silver do GeoSampa (`fiap_analytics.silver.geosampa_flooding_events`). Roda depois de `02_geosampa_flooding_events_silver` e antes da Gold.
# MAGIC
# MAGIC 1. Regras `ERRO` que falham param a tarefa e o job: a Gold não é carregada com dados ruins.
# MAGIC 2. Regras `AVISO` só registram.
# MAGIC 3. Resultados em `fiap_analytics.bronze.geosampa_data_quality_log`, uma linha por regra e execução.

# MAGIC
# MAGIC ## Regras avaliadas
# MAGIC
# MAGIC A lista `regras` verifica volume e correspondência com a Bronze, chave da ocorrência, período da data, SRID e coordenadas, além da identificação de tipo e subprefeitura. As regras classificadas como `ERRO` interrompem a tarefa quando excedem o limite; `AVISO` apenas registra a proporção encontrada.
# MAGIC
# MAGIC `DataQuality.executar(regras)` avalia e grava os resultados no log compartilhado. `dq.validar(resultados)` aplica a política de bloqueio das regras `ERRO`; ele é chamado depois da exibição dos resultados para que uma falha fique registrada antes de interromper o job.

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

DQ_TABLE = "fiap_analytics.bronze.geosampa_data_quality_log"
FONTE = "geosampa"
ETAPA = "silver"
BATCH_ID = str(uuid.uuid4())

SILVER = "fiap_analytics.silver.geosampa_flooding_events"
BRONZE = "fiap_analytics.bronze.geosampa_history_flooding_events"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras

# COMMAND ----------

regras = [
    # Volume e completude
    nao_vazia(SILVER),
    consulta(
        "linhas_iguais_bronze", SILVER, "A Silver tem as mesmas linhas da Bronze (nenhuma perdida ou duplicada nos joins)",
        f"""SELECT ABS((SELECT COUNT(*) FROM {SILVER}) - (SELECT COUNT(*) FROM {BRONZE})) AS falhas,
                   (SELECT COUNT(*) FROM {BRONZE}) AS total""",
    ),

    # Identificação da ocorrência
    nao_nulo(SILVER, "_source_year"),
    nao_nulo(SILVER, "nr_id_ocorrencia"),
    unico(SILVER, ["_source_year", "nr_id_ocorrencia"], nome="ocorrencia_unica_no_ano"),
    unico(BRONZE, ["_source_year", "name"], severidade="AVISO", filtro="name IS NOT NULL",
          nome="name_unico_no_ano"),

    # Data
    nao_nulo(SILVER, "dt_ocorrencia"),
    condicao("data_no_periodo", SILVER, "Data entre 2010-01-01 (início da dim_tempo) e hoje",
             "dt_ocorrencia < DATE'2010-01-01' OR dt_ocorrencia > current_date()"),
    condicao("data_no_ano_do_arquivo", SILVER, "Ano da data igual ao ano do arquivo de origem",
             "year(dt_ocorrencia) <> _source_year", severidade="AVISO"),

    # Localização
    condicao("srid_31983", SILVER, "Geometria em SIRGAS 2000 / UTM 23S (EPSG 31983), o sistema que a Silver converte",
             "_source_srs_id <> 31983"),
    nao_nulo(SILVER, "nr_latitude", severidade="AVISO", limite=0.01),
    condicao("ponto_na_area_de_sao_paulo", SILVER, "Coordenada dentro da área de São Paulo e entorno",
             "nr_latitude NOT BETWEEN -24.1 AND -23.3 OR nr_longitude NOT BETWEEN -47.0 AND -46.3"),

    # Classificação
    nao_nulo(SILVER, "nm_subprefeitura", severidade="AVISO", limite=0.01),
    condicao("tipo_outros", SILVER, "Ocorrências classificadas como 'Outros' (tipo desconhecido ou vazio)",
             "dc_tipo_ocorrencia = 'Outros'", severidade="AVISO", limite=0.05),
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
