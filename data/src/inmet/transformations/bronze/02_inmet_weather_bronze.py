# Databricks notebook source
# MAGIC %md
# MAGIC # 02_inmet_weather_bronze
# MAGIC
# MAGIC Carrega os CSVs horários do INMET do volume `fiap_analytics.bronze.inmet_weather` para a tabela `fiap_analytics.bronze.inmet_weather_readings`.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Anos pendentes: os que tiveram landing (`SUCESSO` ou `PARCIAL`) depois da última carga na Bronze, segundo o `inmet_ingestion_log`.
# MAGIC 2. Formato do INMET: texto em Latin-1, separado por `;`, com 8 linhas de cabeçalho da estação (região, UF, nome, código, latitude, longitude, altitude e data de fundação) e depois uma linha por hora. Os valores ficam como texto (vírgula decimal, `-9999` para falta); a conversão é feita na Silver.
# MAGIC 3. As colunas de medida ganham nomes curtos e estáveis (por exemplo, `PRECIPITAÇÃO TOTAL, HORÁRIO (mm)` vira `precipitacao_total_mm`), reconhecidos pelo começo do nome original. Uma coluna que não for reconhecida para a carga, para não perder dado em silêncio.
# MAGIC 4. Os dados da estação (cabeçalho do arquivo) vão em cada linha, com prefixo `estacao_`.
# MAGIC 5. Idempotência por ano: a fatia `_window = 'AAAA'` é substituída com `replaceWhere`.
# MAGIC 6. Conferência: linhas gravadas precisam bater com as lidas em cada ano.

# COMMAND ----------

import io
import os
import re
import sys
import unicodedata
import uuid
import zipfile
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG, SCHEMA = "fiap_analytics", "bronze"
TABLE = f"{CATALOG}.{SCHEMA}.inmet_weather_readings"
BATCH_ID = str(uuid.uuid4())

# Colunas do CSV do INMET: (começo do nome original sem acento, em maiúsculas) -> nome na Bronze.
# A ordem importa: o primeiro começo que casar vale.
COLUNAS = [
    ("DATA", "data"),
    ("HORA", "hora_utc"),
    ("PRECIPITACAO TOTAL", "precipitacao_total_mm"),
    ("PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO", "pressao_estacao_mb"),
    ("PRESSAO ATMOSFERICA MAX", "pressao_max_mb"),
    ("PRESSAO ATMOSFERICA MIN", "pressao_min_mb"),
    ("RADIACAO GLOBAL", "radiacao_global_kj_m2"),
    ("TEMPERATURA DO AR - BULBO SECO", "temperatura_ar_c"),
    ("TEMPERATURA DO PONTO DE ORVALHO", "temperatura_orvalho_c"),
    ("TEMPERATURA MAXIMA", "temperatura_max_c"),
    ("TEMPERATURA MINIMA", "temperatura_min_c"),
    ("TEMPERATURA ORVALHO MAX", "orvalho_max_c"),
    ("TEMPERATURA ORVALHO MIN", "orvalho_min_c"),
    ("UMIDADE REL. MAX", "umidade_max_pct"),
    ("UMIDADE REL. MIN", "umidade_min_pct"),
    ("UMIDADE RELATIVA DO AR", "umidade_relativa_pct"),
    ("VENTO, DIRECAO", "vento_direcao_graus"),
    ("VENTO, RAJADA", "vento_rajada_ms"),
    ("VENTO, VELOCIDADE", "vento_velocidade_ms"),
]
CABECALHO_ESTACAO = {
    "REGIAO": "estacao_regiao", "UF": "estacao_uf", "ESTACAO": "estacao_nome", "CODIGO (WMO)": "estacao_codigo",
    "LATITUDE": "estacao_latitude", "LONGITUDE": "estacao_longitude", "ALTITUDE": "estacao_altitude",
    "DATA DE FUNDACAO": "estacao_data_fundacao",
}
CAMPOS = list(CABECALHO_ESTACAO.values()) + [c for _, c in COLUNAS]
SCHEMA_BRONZE = T.StructType(
    [T.StructField(c, T.StringType()) for c in CAMPOS]
    + [T.StructField("_source_member", T.StringType()), T.StructField("_source_line", T.IntegerType())]
)

# Módulos de src/common. Sobe pelas pastas até achar src/ para importar.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common.ingestion_log import IngestionLog  # noqa: E402

LOG_TABLE = "fiap_analytics.bronze.inmet_ingestion_log"
FONTE = "inmet_weather"
ETAPA = "bronze"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos CSVs

# COMMAND ----------


def sem_acento(texto):
    return "".join(c for c in unicodedata.normalize("NFKD", texto) if not unicodedata.combining(c)).upper().strip()


def nome_coluna(original):
    chave = sem_acento(original)
    for inicio, nome in COLUNAS:
        if chave.startswith(inicio):
            return nome
    return None


def ler_csv_inmet(conteudo, membro):
    """Linhas (tuplas na ordem de SCHEMA_BRONZE) de um CSV do INMET."""
    linhas = io.TextIOWrapper(io.BytesIO(conteudo), encoding="latin-1", newline="").read().splitlines()
    estacao, i = {}, 0
    while i < len(linhas) and ":" in linhas[i].split(";")[0]:
        rotulo, _, valor = linhas[i].partition(";")
        campo = CABECALHO_ESTACAO.get(sem_acento(rotulo.rstrip(":")))
        if campo:
            estacao[campo] = valor.strip().rstrip(";") or None
        i += 1
    if i >= len(linhas):
        raise RuntimeError(f"{membro}: cabeçalho de colunas não encontrado")
    cabecalho = [c.strip() for c in linhas[i].split(";")]
    indices = {}
    for pos, original in enumerate(cabecalho):
        if not original:
            continue  # o INMET termina cada linha com ';'
        nome = nome_coluna(original)
        if nome is None:
            raise RuntimeError(f"{membro}: coluna não reconhecida '{original}'. Inclua-a em COLUNAS.")
        indices[nome] = pos
    if "data" not in indices or "hora_utc" not in indices:
        raise RuntimeError(f"{membro}: colunas de data e hora não encontradas em {cabecalho}")

    saida = []
    for numero, linha in enumerate(linhas[i + 1:], start=i + 2):
        if not linha.strip():
            continue
        valores = linha.split(";")
        medidas = {n: (valores[p].strip() or None) if p < len(valores) else None for n, p in indices.items()}
        registro = {**estacao, **medidas}
        saida.append(tuple(registro.get(c) for c in CAMPOS) + (membro, numero))
    return saida


# COMMAND ----------

# MAGIC %md
# MAGIC ## Anos pendentes

# COMMAND ----------

pendentes = spark.sql(f"""
WITH landing AS (
  SELECT janela, max_by(arquivo, finalizado_em) AS arquivo, max(finalizado_em) AS landing_em
  FROM {LOG_TABLE}
  WHERE fonte = '{FONTE}' AND etapa = 'landing' AND status IN ('SUCESSO', 'PARCIAL') AND registros > 0
  GROUP BY janela
),
bronze AS (
  SELECT janela, max(finalizado_em) AS bronze_em
  FROM {LOG_TABLE}
  WHERE fonte = '{FONTE}' AND etapa = 'bronze' AND status = 'SUCESSO'
  GROUP BY janela
)
SELECT l.janela, l.arquivo
FROM landing l LEFT JOIN bronze b USING (janela)
WHERE b.bronze_em IS NULL OR b.bronze_em < l.landing_em
ORDER BY l.janela
""").collect()
print(f"{len(pendentes)} anos pendentes:", [p.janela for p in pendentes])

# COMMAND ----------

falhas = []
for p in pendentes:
    iniciado_em = datetime.now(timezone.utc)
    try:
        linhas = []
        with zipfile.ZipFile(p.arquivo) as z:
            membros = sorted(n for n in z.namelist() if n.lower().endswith(".csv"))
            for membro in membros:
                lidas = ler_csv_inmet(z.read(membro), membro)
                linhas.extend(lidas)
                print(f"   {membro}: {len(lidas)} linhas")
        df = (
            spark.createDataFrame(linhas, SCHEMA_BRONZE)
            .withColumn("_source_file", F.lit(p.arquivo))
            .withColumn("_window", F.lit(p.janela))
            .withColumn("_batch_id", F.lit(BATCH_ID))
            .withColumn("_ingested_at", F.current_timestamp())
        )
        if not spark.catalog.tableExists(TABLE):
            df.write.format("delta").saveAsTable(TABLE)
            spark.sql(f"COMMENT ON TABLE {TABLE} IS 'Dados meteorológicos horários brutos do INMET (BDMEP), estações "
                      "automáticas do município de São Paulo, uma fatia por ano (_window). Valores em texto como no CSV; "
                      "hora em UTC. Fonte: Instituto Nacional de Meteorologia.'")
        else:
            df.write.format("delta").mode("overwrite").option("replaceWhere", f"_window = '{p.janela}'").saveAsTable(TABLE)

        gravadas = spark.table(TABLE).filter(F.col("_window") == p.janela).count()
        if gravadas != len(linhas):
            raise RuntimeError(f"lidas {len(linhas)} linhas, gravadas {gravadas}")
        registrar(p.janela, "SUCESSO", iniciado_em, registros=gravadas, arquivo=p.arquivo,
                  mensagem=f"{len(membros)} arquivos: {', '.join(m.split('_')[3] for m in membros)}")
        print(f"{p.janela}: {gravadas} linhas carregadas")
    except Exception as exc:
        registrar(p.janela, "FALHA", iniciado_em, arquivo=p.arquivo, mensagem=str(exc))
        falhas.append(p.janela)
        print(f"{p.janela}: FALHA {exc}")

if falhas:
    raise RuntimeError(f"Anos com falha na Bronze: {falhas}")

display(spark.sql(f"""
    SELECT _window, estacao_codigo, COUNT(*) AS linhas, MIN(data) AS primeiro_dia, MAX(data) AS ultimo_dia
    FROM {TABLE} GROUP BY ALL ORDER BY _window DESC, estacao_codigo
"""))
