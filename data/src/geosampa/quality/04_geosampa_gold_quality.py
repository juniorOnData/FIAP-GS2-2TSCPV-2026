# Databricks notebook source
# MAGIC %md
# MAGIC # 04_geosampa_gold_quality
# MAGIC
# MAGIC Regras de qualidade da Gold do GeoSampa: a fato `fact_geosampa_flooding_events`, as dimensões `dim_geosampa_type_flooding_events` e `dim_geosampa_subprefecture_area` e a `dim_tempo`. Roda depois de `04_fact_geosampa_flooding_events`.
# MAGIC
# MAGIC 1. Regras `ERRO` que falham param a tarefa e o job.
# MAGIC 2. Regras `AVISO` só registram.
# MAGIC 3. Resultados em `fiap_analytics.bronze.geosampa_data_quality_log`, uma linha por regra e execução.

# MAGIC
# MAGIC ## Função e regras avaliadas
# MAGIC
# MAGIC `membro_desconhecido(dim, chave)` cria uma regra que confirma se a dimensão contém exatamente uma linha com chave `-1`. A lista `regras` aplica verificações de volume e unicidade, compara a fato com a Silver por ano, confere as referências dimensionais e a cobertura contínua da `dim_tempo`, e registra ocorrências com localização ou classificação desconhecida.
# MAGIC
# MAGIC `DataQuality.executar(regras)` grava os resultados em `geosampa_data_quality_log`. As regras `ERRO` reprovadas fazem `dq.validar(resultados)` interromper a tarefa; regras `AVISO` permanecem registradas sem bloquear o job.

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

DQ_TABLE = "fiap_analytics.bronze.geosampa_data_quality_log"
FONTE = "geosampa"
ETAPA = "gold"
BATCH_ID = str(uuid.uuid4())

SILVER = "fiap_analytics.silver.geosampa_flooding_events"
FACT = "fiap_analytics.gold.fact_geosampa_flooding_events"
DIM_TEMPO = "fiap_analytics.gold.dim_tempo"
DIM_TIPO = "fiap_analytics.gold.dim_geosampa_type_flooding_events"
DIM_SUBPREFEITURA = "fiap_analytics.gold.dim_geosampa_subprefecture_area"

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
    # Fato: volume e conciliação com a Silver
    nao_vazia(FACT),
    consulta(
        "linhas_iguais_silver_por_ano", FACT, "Por ano de origem, a fato tem as mesmas linhas da Silver",
        f"""SELECT COUNT_IF(NOT (s.n <=> g.n)) AS falhas, COUNT(*) AS total
            FROM (SELECT _source_year AS ano, COUNT(*) AS n FROM {SILVER} GROUP BY 1) s
            FULL OUTER JOIN (SELECT NUM_ANO_ORIGEM AS ano, COUNT(*) AS n FROM {FACT} GROUP BY 1) g
              ON s.ano <=> g.ano""",
    ),
    unico(FACT, ["NUM_ANO_ORIGEM", "NUM_ID_OCORRENCIA"], nome="ocorrencia_unica"),

    # Fato: integridade referencial (chave nula também conta como falha)
    existe_em(FACT, "COD_DIM_TEMPO", DIM_TEMPO),
    existe_em(FACT, "COD_DIM_OCORRENCIA", DIM_TIPO),
    existe_em(FACT, "COD_DIM_SUBPREFEITURA", DIM_SUBPREFEITURA),

    # Fato: ocorrências que caíram no membro desconhecido
    condicao("subprefeitura_nao_informada", FACT, "Ocorrências sem subprefeitura identificada (-1)",
             "COD_DIM_SUBPREFEITURA = -1", severidade="AVISO", limite=0.01),
    condicao("tipo_nao_informado", FACT, "Ocorrências sem tipo identificado (-1)",
             "COD_DIM_OCORRENCIA = -1", severidade="AVISO"),
    condicao("ponto_na_area_de_sao_paulo", FACT, "Coordenada dentro da área de São Paulo e entorno",
             "NUM_LATITUDE NOT BETWEEN -24.1 AND -23.3 OR NUM_LONGITUDE NOT BETWEEN -47.0 AND -46.3"),

    # Dimensões do GeoSampa
    unico(DIM_TIPO, ["COD_DIM_OCORRENCIA"]),
    unico(DIM_TIPO, ["DSC_TIPO_OCORRENCIA"]),
    membro_desconhecido(DIM_TIPO, "COD_DIM_OCORRENCIA"),
    unico(DIM_SUBPREFEITURA, ["COD_DIM_SUBPREFEITURA"]),
    unico(DIM_SUBPREFEITURA, ["SIG_SUBPREFEITURA"]),
    membro_desconhecido(DIM_SUBPREFEITURA, "COD_DIM_SUBPREFEITURA"),

    # dim_tempo
    unico(DIM_TEMPO, ["COD_DIM_TEMPO"]),
    consulta(
        "sem_dias_faltando", DIM_TEMPO, "Nenhum dia faltando entre o primeiro e o último dia da dimensão",
        f"SELECT datediff(MAX(DAT_DIA), MIN(DAT_DIA)) + 1 - COUNT(*) AS falhas, COUNT(*) AS total FROM {DIM_TEMPO}",
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
