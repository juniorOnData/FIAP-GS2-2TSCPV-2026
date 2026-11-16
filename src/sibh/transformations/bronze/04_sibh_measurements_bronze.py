# Databricks notebook source
# MAGIC %md
# MAGIC # SIB_BRZ_002 04_sibh_measurements_bronze
# MAGIC
# MAGIC ## 01. Identificação
# MAGIC
# MAGIC | Campo | Valor |
# MAGIC |---|---|
# MAGIC | Identificador | SIB_BRZ_002 |
# MAGIC | Nome | 04_sibh_measurements_bronze |
# MAGIC | Projeto | ChuvaSegura |
# MAGIC | Domínio de negócio | Hidrologia (SIBH) |
# MAGIC | Camada | Bronze |
# MAGIC | Linguagem | Python |
# MAGIC | Responsável técnico | juniorOnData |
# MAGIC | Responsável funcional | A definir |
# MAGIC | Status | Homologação |
# MAGIC | Versão | 1.3 |
# MAGIC | Criticidade | Média |
# MAGIC | Caminho no repositório | `src/sibh/transformations/bronze/04_sibh_measurements_bronze.py` |
# MAGIC
# MAGIC ## 02. Objetivo
# MAGIC
# MAGIC Carregar na Bronze os meses de medições do SIBH com manifesto, substituindo a fatia de cada mês e conferindo a contagem com o manifesto.
# MAGIC
# MAGIC ## 03. Contexto funcional
# MAGIC
# MAGIC Disponibiliza as medições brutas de chuva e nível em tabela para a futura Silver do SIBH.
# MAGIC
# MAGIC ## 04. Arquitetura e fluxo
# MAGIC
# MAGIC | Posição | Item |
# MAGIC |---|---|
# MAGIC | Etapa anterior | `03_sibh_measurements_landing` |
# MAGIC | Etapa atual | Grava `fiap_analytics.bronze.sibh_measurements` |
# MAGIC | Etapa posterior | Silver do SIBH (a desenvolver) |
# MAGIC
# MAGIC ## 05. Dependências
# MAGIC
# MAGIC | Tipo | Item | Uso |
# MAGIC |---|---|---|
# MAGIC | Volume | `/Volumes/fiap_analytics/bronze/sibh_measurements` | Arquivos e manifestos mensais |
# MAGIC | Módulo | `src/common/ingestion_log.py` | Registro no log |
# MAGIC | Permissão | USE CATALOG em `fiap_analytics`; USE SCHEMA, CREATE TABLE e MODIFY em `fiap_analytics.bronze`; READ VOLUME no volume | Ler arquivos e gravar a tabela |
# MAGIC
# MAGIC ## 06. Parâmetros de entrada
# MAGIC
# MAGIC | Nome | Tipo | Obrigatório | Descrição | Valor padrão | Exemplo |
# MAGIC |---|---|---|---|---|---|
# MAGIC | `window` | text (texto) | Não | `pending` (meses novos ou baixados de novo), `all` ou um mês `AAAA-MM` | pending | 2026-10 |
# MAGIC
# MAGIC ## 07. Fontes de dados
# MAGIC
# MAGIC | Fonte | Formato | Origem | Tipo de leitura | Frequência | Chave | Filtros |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | Volume `sibh_measurements`, meses com manifesto | JSON por linha (gzip) | Landing do SIBH | Por mês | Diária | `station_prefix_id` + `date` | Conforme `window` |
# MAGIC
# MAGIC ## 08. Processamento técnico
# MAGIC
# MAGIC | Etapa | Linguagem | Célula | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | E01 | PY | Parâmetros | Widget `window`. |
# MAGIC | E02 | PY | Configuração | Imports, constantes, campos esperados das medições e schema em texto. |
# MAGIC | E03 | PY | Log de ingestão | Importa o `IngestionLog` e define `registrar` (etapa `bronze`). |
# MAGIC | E04 | PY | Meses a carregar | Lista os meses com manifesto e escolhe conforme o parâmetro. |
# MAGIC | E05 | PY | Carga mês a mês | Leitura, validação (contagem contra o manifesto, sensor e data presentes, campos novos), substituição da fatia com `replaceWhere` (ou `DELETE` se o mês não tem medições), conferência e log. |
# MAGIC | E06 | PY | Resumo por instituição | Sensores, período e linhas por instituição e tipo. |
# MAGIC | E07 | PY | Classificações de qualidade | Distribuição de `measurement_classification_type_id`. |
# MAGIC
# MAGIC ## 09. Regras de negócio
# MAGIC
# MAGIC | ID | Regra | Lógica | Comportamento esperado |
# MAGIC |---|---|---|---|
# MAGIC | RN_SIB_006 | Horário em UTC | `date` fica como a API devolve (UTC, texto `AAAA/MM/DD HH:mm`). | A conversão para America/Sao_Paulo é feita na Silver. |
# MAGIC
# MAGIC ## 10. Saídas de dados
# MAGIC
# MAGIC | Destino | Formato | Granularidade | Chave | Modo de escrita |
# MAGIC |---|---|---|---|---|
# MAGIC | `fiap_analytics.bronze.sibh_measurements` | Delta, particionada por `_window_year` | Uma linha por medição | `station_prefix_id` + `date` | `overwrite` com `replaceWhere` por `_window` (AAAA-MM) |
# MAGIC
# MAGIC As colunas de cada tabela estão no dicionário de dados (`docs/02_catalogos/dicionario_de_dados.csv`).
# MAGIC
# MAGIC ## 11. Qualidade dos dados
# MAGIC
# MAGIC Bloqueia a gravação de um mês se a contagem divergir do manifesto ou se houver linha sem `station_prefix_id` ou `date`. Campos novos vão para `_rescued_data` e geram aviso.
# MAGIC
# MAGIC ## 12. Tratamento de erros
# MAGIC
# MAGIC | Situação | Comportamento | Registro | Recuperação |
# MAGIC |---|---|---|---|
# MAGIC | Mês com falha | Registra `FALHA`, segue para os outros meses e termina com erro | `FALHA` no `fiap_analytics.bronze.sibh_ingestion_log` | Rodar de novo com `window` no mês afetado |
# MAGIC
# MAGIC ## 13. Segurança e governança
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Classificação dos dados | Pública (API pública do SIBH, SP Águas; confirmar os termos de uso) |
# MAGIC | Dados sensíveis | Nenhum |
# MAGIC | Credenciais | Nenhuma (API pública, sem autenticação) |
# MAGIC | Permissões necessárias | USE CATALOG em `fiap_analytics`; USE SCHEMA, CREATE TABLE e MODIFY em `fiap_analytics.bronze`; READ VOLUME no volume |
# MAGIC | Políticas aplicáveis | Termos de uso dos dados do SIBH (confirmar) |
# MAGIC
# MAGIC ## 14. Monitoramento e operação
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Job e tarefa | `ChuvaSegura - SIBH` (`jobs/chuvasegura_sibh.job.yml`), tarefa `sibh_measurements_bronze` |
# MAGIC | Frequência | Diária, 07:30 (America/Sao_Paulo); agenda pausada até o backfill |
# MAGIC | SLA | A definir |
# MAGIC | Métricas | Linhas e sensores por mês |
# MAGIC | Logs | `fiap_analytics.bronze.sibh_ingestion_log` (etapa `bronze`) |
# MAGIC | Alertas | Não configurado (recomendação: notificação de falha no job) |
# MAGIC | Reprocessamento | `window = all` ou o mês desejado |
# MAGIC
# MAGIC ## 15. Histórico de alterações
# MAGIC
# MAGIC | Versão | Data | Responsável | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | 1.0 | 2026-11-16 | juniorOnData | Criação do notebook (commit `2a2af58`, sem PR) |
# MAGIC | 1.1 | 2026-11-16 | juniorOnData | Log de ingestão próprio da origem, no formato padrão (PR #8) |
# MAGIC | 1.2 | 2026-11-16 | juniorOnData | Formato de código fonte, módulo de log compartilhado e novo nome `04_sibh_measurements_bronze` (PR #9) |
# MAGIC | 1.3 | 2026-11-16 | juniorOnData | Documentação no padrão oficial (PR #15) |

# COMMAND ----------

# [PY] E01 Parâmetros
dbutils.widgets.text("window", "pending", "Mês: pending, all ou AAAA-MM")

# COMMAND ----------

# [PY] E02 Configuração
import json
import os
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "fiap_analytics"
SCHEMA = "bronze"
VOLUME = "sibh_measurements"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.sibh_measurements"
DATASET = "sibh_measurements"
ENDPOINT = "https://cth.daee.sp.gov.br/sibh/api/v2/measurements"

WINDOW_ARG = dbutils.widgets.get("window").strip().lower() or "pending"
BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc)

# Campos do serializer default do /measurements (lidos no código da API oficial).
# Os quatro últimos só aparecem quando group_type é diferente de minute.
MEASUREMENT_FIELDS = [
    "measurement_id",
    "station_prefix_id",
    "prefix",
    "station_name",
    "station_type_id",
    "station_owner",
    "city_id",
    "cod_ibge",
    "latitude",
    "longitude",
    "transmission_type_name",
    "measurement_gap",
    "param_type",
    "measurement_classification_type_id",
    "date",
    "value",
    "read_value",
    "qtd",
    "min_value",
    "max_value",
    "min_date",
    "max_date",
]
SOURCE_SCHEMA = T.StructType([T.StructField(c, T.StringType(), True) for c in MEASUREMENT_FIELDS])
print(f"batch_id={BATCH_ID} | window={WINDOW_ARG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log de execução

# COMMAND ----------

# [PY] E03 Log de ingestão
# Log padrão da origem (src/common/ingestion_log.py). Sobe pelas pastas até achar src/ para importar o módulo.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common.ingestion_log import IngestionLog  # noqa: E402

LOG_TABLE = "fiap_analytics.bronze.sibh_ingestion_log"
FONTE = "sibh_measurements"
ETAPA = "bronze"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Meses a carregar

# COMMAND ----------

# [PY] E04 Meses a carregar
def list_landing_months():
    """Lista {AAAA-MM: (caminho_do_arquivo, manifesto)} dos meses completos no volume."""
    found = {}
    if not os.path.isdir(VOLUME_PATH):
        return found
    for year_dir in sorted(d for d in os.listdir(VOLUME_PATH) if d.startswith("ano=")):
        for month_dir in sorted(d for d in os.listdir(f"{VOLUME_PATH}/{year_dir}") if d.startswith("mes=")):
            folder = f"{VOLUME_PATH}/{year_dir}/{month_dir}"
            for name in os.listdir(folder):
                if name.endswith(".manifest.json"):
                    with open(f"{folder}/{name}", encoding="utf-8") as fh:
                        manifest = json.load(fh)
                    data_path = f"{folder}/{manifest['file_name']}"
                    if os.path.exists(data_path):
                        found[manifest["window"]] = (data_path, manifest)
    return found


landing = list_landing_months()
if not landing:
    raise RuntimeError(f"Nenhum mês com manifesto em {VOLUME_PATH}. Rode antes o 03_sibh_measurements_landing.")

if WINDOW_ARG == "pending":
    loaded = set()
    if spark.catalog.tableExists(BRONZE_TABLE):
        loaded = {(r["w"], r["b"]) for r in spark.table(BRONZE_TABLE)
                  .select(F.col("_window").alias("w"), F.col("_landing_batch_id").alias("b")).distinct().collect()}
    windows = sorted(w for w, (_, m) in landing.items() if (w, m["landing_batch_id"]) not in loaded)
    if not windows:
        print("Nada pendente: todos os meses do volume já estão na Bronze.")
        dbutils.notebook.exit("nothing_pending")
elif WINDOW_ARG == "all":
    windows = sorted(landing)
else:
    if WINDOW_ARG not in landing:
        raise RuntimeError(f"Mês {WINDOW_ARG} não está no volume. Disponíveis: {sorted(landing)[:5]} ... {sorted(landing)[-5:]}")
    windows = [WINDOW_ARG]

print(f"Meses a carregar: {len(windows)} ({windows[0]} a {windows[-1]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga mês a mês

# COMMAND ----------

# [PY] E05 Carga mês a mês
def read_month(window: str):
    data_path, manifest = landing[window]
    if int(manifest["record_count"]) == 0:
        return None, manifest
    df = (
        spark.read.schema(SOURCE_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("rescuedDataColumn", "_rescued_data")
        .json(data_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_endpoint", F.lit(ENDPOINT))
        .withColumn("_window", F.lit(window))
        .withColumn("_window_year", F.lit(int(window[:4])))
        .withColumn("_window_month", F.lit(int(window[5:7])))
        .withColumn("_collected_at", F.to_timestamp(F.lit(manifest["collected_at_utc"])))
        .withColumn("_landing_batch_id", F.lit(manifest["landing_batch_id"]))
        .withColumn("_batch_id", F.lit(BATCH_ID))
        .withColumn("_ingested_at", F.current_timestamp())
        .select(*MEASUREMENT_FIELDS, "_source_file", "_source_endpoint", "_window", "_window_year", "_window_month",
                "_collected_at", "_landing_batch_id", "_batch_id", "_ingested_at", "_rescued_data")
    )
    return df, manifest


def validate_month(df, manifest) -> int:
    stats = df.agg(
        F.count("*").alias("n"),
        F.sum(F.col("station_prefix_id").isNull().cast("int")).alias("sem_sensor"),
        F.sum(F.col("date").isNull().cast("int")).alias("sem_data"),
        F.sum(F.col("_rescued_data").isNotNull().cast("int")).alias("rescued"),
    ).first()
    if stats["n"] != int(manifest["record_count"]):
        raise RuntimeError(f"Contagem diferente do manifesto: manifesto={manifest['record_count']} lido={stats['n']}")
    if stats["sem_sensor"] or stats["sem_data"]:
        sample = df.filter(F.col("station_prefix_id").isNull() | F.col("date").isNull()).select("_rescued_data").limit(2).collect()
        raise RuntimeError(f"{stats['sem_sensor']} linhas sem station_prefix_id e {stats['sem_data']} sem date. Exemplo de _rescued_data: {[r[0] for r in sample]}")
    if stats["rescued"]:
        keys = (df.filter(F.col("_rescued_data").isNotNull())
                .select(F.explode(F.expr("json_object_keys(_rescued_data)")).alias("k"))
                .filter(F.col("k") != "_file_path").distinct().collect())
        print(f"  aviso: {stats['rescued']} linhas com campos fora do schema: {sorted(r['k'] for r in keys)}")
    return stats["n"]


failed = []
for window in windows:
    started = datetime.now(timezone.utc)
    data_path, manifest = landing[window]
    try:
        df, manifest = read_month(window)
        condition = f"_window = '{window}'"
        if df is None:
            # Mês sem medições: remove uma carga anterior do mês, se houver, e registra.
            if spark.catalog.tableExists(BRONZE_TABLE):
                spark.sql(f"DELETE FROM {BRONZE_TABLE} WHERE {condition}")
            n = 0
        else:
            n = validate_month(df, manifest)
            writer = df.write.format("delta").mode("overwrite")
            if not spark.catalog.tableExists(BRONZE_TABLE):
                writer.partitionBy("_window_year").saveAsTable(BRONZE_TABLE)
                spark.sql(f"""
                    COMMENT ON TABLE {BRONZE_TABLE} IS
                    'Bronze das medições do SIBH/SP Águas ({ENDPOINT}): chuva e nível dos sensores da capital, sem CEMADEN. Colunas originais em string, date em UTC. Uma fatia por mês (_window). Fonte: SIBH, Agência de Águas do Estado de São Paulo (SP Águas).'
                """)
            else:
                writer.option("replaceWhere", condition).saveAsTable(BRONZE_TABLE)

            in_table = spark.table(BRONZE_TABLE).filter(F.col("_window") == window).count()
            if in_table != n:
                raise RuntimeError(f"Conferência falhou: lidas={n} na tabela={in_table}")

        registrar(window, "SUCESSO" if n else "SEM_DADOS", started, registros=n, arquivo=data_path,
                  sha256=manifest.get("sha256"))
        print(f"{window}: {n} linhas")
    except Exception as exc:
        failed.append(window)
        registrar(window, "FALHA", started, arquivo=data_path, mensagem=str(exc))
        print(f"{window}: FALHOU ({exc})")

if failed:
    raise RuntimeError(f"{len(failed)} meses falharam: {failed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Primeira olhada (insumo para a Silver)

# COMMAND ----------

# [PY] E06 Resumo por instituição
display(spark.sql(f"""
    SELECT
        m.station_owner,
        CASE m.station_type_id WHEN '2' THEN 'chuva' WHEN '1' THEN 'nível' ELSE m.station_type_id END AS tipo,
        COUNT(DISTINCT m.station_prefix_id) AS sensores,
        MIN(m.date) AS primeira_leitura,
        MAX(m.date) AS ultima_leitura,
        COUNT(*) AS linhas,
        COUNT(DISTINCT m.measurement_classification_type_id) AS classificacoes
    FROM {BRONZE_TABLE} m
    GROUP BY ALL
    ORDER BY m.station_owner, tipo
"""))

# COMMAND ----------

# [PY] E07 Classificações de qualidade
# Distribuição das classificações de qualidade (a consulta pública devolve 1, 2 e 3; o significado ainda não foi documentado).
display(spark.sql(f"""
    SELECT measurement_classification_type_id, COUNT(*) AS linhas
    FROM {BRONZE_TABLE}
    GROUP BY ALL
    ORDER BY linhas DESC
"""))
