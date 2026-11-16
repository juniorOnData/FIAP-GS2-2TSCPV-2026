# Databricks notebook source
# MAGIC %md
# MAGIC # GEO_BRZ_002 01_geosampa_history_flooding_events_bronze
# MAGIC
# MAGIC ## 01. Identificação
# MAGIC
# MAGIC | Campo | Valor |
# MAGIC |---|---|
# MAGIC | Identificador | GEO_BRZ_002 |
# MAGIC | Nome | 01_geosampa_history_flooding_events_bronze |
# MAGIC | Projeto | ChuvaSegura |
# MAGIC | Domínio de negócio | Ocorrências de alagamento (GeoSampa) |
# MAGIC | Camada | Bronze |
# MAGIC | Linguagem | Python e SQL |
# MAGIC | Responsável técnico | juniorOnData |
# MAGIC | Responsável funcional | A definir |
# MAGIC | Status | Produção |
# MAGIC | Versão | 1.5 |
# MAGIC | Criticidade | Média |
# MAGIC | Caminho no repositório | `src/geosampa/ingestion/01_geosampa_history_flooding_events_bronze.py` |
# MAGIC
# MAGIC ## 02. Objetivo
# MAGIC
# MAGIC Carregar na Bronze os arquivos GeoPackage de ocorrências do GeoSampa (alagamento, deslizamento, inundação, queda de árvore) enviados para o volume, guardando os atributos como texto, a geometria em WKB e as colunas de linhagem. Garante uma versão por ano e substitui a versão anterior quando um arquivo é corrigido.
# MAGIC
# MAGIC ## 03. Contexto funcional
# MAGIC
# MAGIC As ocorrências são o fato central do projeto: é com elas que a chuva e o nível dos rios serão cruzados. Os arquivos são publicados por ano pela Prefeitura e enviados manualmente ao volume.
# MAGIC
# MAGIC ## 04. Arquitetura e fluxo
# MAGIC
# MAGIC | Posição | Item |
# MAGIC |---|---|
# MAGIC | Etapa anterior | Upload dos arquivos `.gpkg` ou `.zip` no volume `geosampa_history_flooding_events` |
# MAGIC | Etapa atual | Grava `fiap_analytics.bronze.geosampa_history_flooding_events` |
# MAGIC | Etapa posterior | `02_geosampa_flooding_events_silver` |
# MAGIC
# MAGIC ## 05. Dependências
# MAGIC
# MAGIC | Tipo | Item | Uso |
# MAGIC |---|---|---|
# MAGIC | Volume | `/Volumes/fiap_analytics/bronze/geosampa_history_flooding_events` | Arquivos de origem |
# MAGIC | Módulo | `src/common/ingestion_log.py` | Registro no `geosampa_ingestion_log` |
# MAGIC | Biblioteca | `sqlite3`, `zipfile`, `hashlib` (biblioteca padrão do Python) | Leitura do GeoPackage, extração do zip e SHA256 |
# MAGIC | Permissão | USE CATALOG em `fiap_analytics`; USE SCHEMA, CREATE TABLE e MODIFY em `fiap_analytics.bronze`; READ VOLUME e WRITE VOLUME no volume | Ler arquivos, criar o volume e gravar a tabela |
# MAGIC
# MAGIC ## 06. Parâmetros de entrada
# MAGIC
# MAGIC Não aplicável. O notebook processa todos os arquivos do volume; não há widgets.
# MAGIC
# MAGIC ## 07. Fontes de dados
# MAGIC
# MAGIC | Fonte | Formato | Origem | Tipo de leitura | Frequência | Chave | Filtros |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | `/Volumes/fiap_analytics/bronze/geosampa_history_flooding_events/**` | GeoPackage (`.gpkg`) ou `.zip` com `.gpkg` | GeoSampa, Prefeitura de São Paulo | Incremental por arquivo (SHA256) | Quando chega um arquivo novo | Arquivo (SHA256) e ano do nome | Somente camadas do tipo `features` |
# MAGIC
# MAGIC ## 08. Processamento técnico
# MAGIC
# MAGIC | Etapa | Linguagem | Célula | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | E01 | PY | Imports | Bibliotecas padrão do Python e PySpark. |
# MAGIC | E02 | PY | Configuração e log | Caminho do volume, tabela de destino, tamanhos de envelope do GeoPackage, `BATCH_ID` e log de ingestão (`geosampa_ingestion_log`, etapa `bronze`). |
# MAGIC | E03 | SQL | Volume | `CREATE VOLUME IF NOT EXISTS` do volume de origem. |
# MAGIC | E04 | PY | Funções | SHA256 do arquivo, remoção do cabeçalho GeoPackage para obter o WKB, verificação de arquivo já carregado, ano do arquivo, chave da versão, remoção de versões anteriores e leitura das camadas. |
# MAGIC | E05 | PY | Leitura dos arquivos | Valida um arquivo por ano, pula arquivos já carregados, extrai o zip com proteção contra caminhos maliciosos e lê as camadas. Registra `FALHA` se a leitura falhar. |
# MAGIC | E06 | PY | Gravação | União das camadas, conferência da contagem, `append` com `mergeSchema`, conferência por camada, remoção da versão anterior do ano, registro `SUCESSO` e resumo por ano. |
# MAGIC
# MAGIC ## 09. Regras de negócio
# MAGIC
# MAGIC | ID | Regra | Lógica | Comportamento esperado |
# MAGIC |---|---|---|---|
# MAGIC | RN_GEO_001 | Ano de origem do arquivo | O ano é o primeiro trecho `20AA` do nome do arquivo e vai para `_source_year`. | Arquivo sem ano no nome usa o caminho como chave da versão e fica com `_source_year` nulo (barrado na Silver pela regra DQ_GEO_SLV_003). |
# MAGIC | RN_GEO_002 | Uma versão por ano | O volume deve ter um arquivo por ano. Um arquivo com SHA256 novo para um ano já carregado é gravado e, depois da conferência, as linhas das versões anteriores daquele ano são apagadas. | Cada ano fica com um único SHA256 na tabela. Dois arquivos do mesmo ano no volume param a carga com `FALHA`. |
# MAGIC
# MAGIC ## 10. Saídas de dados
# MAGIC
# MAGIC | Destino | Formato | Granularidade | Chave | Modo de escrita |
# MAGIC |---|---|---|---|---|
# MAGIC | `fiap_analytics.bronze.geosampa_history_flooding_events` | Delta | Uma linha por feição de cada camada do arquivo | `_source_year` + `id` (ou `primaryindex`) | `append` com `mergeSchema`; versão anterior removida por `DELETE` |
# MAGIC
# MAGIC As colunas de cada tabela estão no dicionário de dados (`docs/02_catalogos/dicionario_de_dados.csv`).
# MAGIC
# MAGIC ## 11. Qualidade dos dados
# MAGIC
# MAGIC Validações no próprio notebook: contagem lida igual à gravada, no total e por camada (`assert`); uma versão por ano ao final da limpeza (`assert`); bloqueio de dois arquivos do mesmo ano. Os atributos variam por ano (por exemplo, `subprefeit` ou `subpreit`, `id` ou `primaryindex`); o `mergeSchema` acrescenta as colunas novas e a Silver trata as variações. A duplicidade de `name` no ano é monitorada pela regra DQ_GEO_SLV_006.
# MAGIC
# MAGIC ## 12. Tratamento de erros
# MAGIC
# MAGIC | Situação | Comportamento | Registro | Recuperação |
# MAGIC |---|---|---|---|
# MAGIC | Dois arquivos do mesmo ano no volume | Para antes de ler qualquer arquivo | `FALHA` no `geosampa_ingestion_log` | Deixar só a versão mais recente no volume e rodar de novo |
# MAGIC | Erro ao ler um arquivo (zip inválido, GeoPackage corrompido) | Para a execução | `FALHA` com a mensagem do erro | Substituir o arquivo e rodar de novo |
# MAGIC | Contagem divergente na gravação | Para a execução antes de remover a versão anterior | `FALHA` para cada arquivo do lote | Rodar de novo: arquivos já gravados são pulados e a limpeza termina o serviço |
# MAGIC
# MAGIC ## 13. Segurança e governança
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Classificação dos dados | Pública (dados abertos da fonte; confirmar os termos de uso) |
# MAGIC | Dados sensíveis | Nenhum. As ocorrências têm coordenadas de logradouro, sem identificação de pessoas |
# MAGIC | Credenciais | Nenhuma |
# MAGIC | Permissões necessárias | USE CATALOG em `fiap_analytics`; USE SCHEMA, CREATE TABLE e MODIFY em `fiap_analytics.bronze`; READ VOLUME e WRITE VOLUME no volume |
# MAGIC | Políticas aplicáveis | Termos de uso dos dados abertos da Prefeitura de São Paulo (confirmar) |
# MAGIC
# MAGIC ## 14. Monitoramento e operação
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Job e tarefa | `ChuvaSegura` (`jobs/chuvasegura_geosampa.job.yml`), tarefa `ingestion_history_flooding_events` |
# MAGIC | Frequência | Diária, 09:00:30 (America/Sao_Paulo) |
# MAGIC | SLA | A definir |
# MAGIC | Métricas | Linhas por ano e arquivos carregados ou pulados |
# MAGIC | Logs | `fiap_analytics.bronze.geosampa_ingestion_log` (etapa `bronze`, janela = ano) |
# MAGIC | Alertas | Não configurado (recomendação: notificação de falha no job) |
# MAGIC | Reprocessamento | Trocar o arquivo do ano no volume e rodar o job. Ver reprocessamento (`docs/04_operacao/reprocessamento.md`) |
# MAGIC
# MAGIC ## 15. Histórico de alterações
# MAGIC
# MAGIC | Versão | Data | Responsável | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | 1.0 | 2026-11-06 | juniorOnData | Criação da ingestão dos arquivos GeoPackage (PR #1) |
# MAGIC | 1.1 | 2026-11-06 | juniorOnData | Ajuste de caminho (PR #4) |
# MAGIC | 1.2 | 2026-11-16 | juniorOnData | Registro no `geosampa_ingestion_log` (PR #8) |
# MAGIC | 1.3 | 2026-11-16 | juniorOnData | Formato de código fonte e módulo de log compartilhado (PR #9) |
# MAGIC | 1.4 | 2026-11-16 | juniorOnData | Arquivo corrigido substitui a versão anterior do ano (PR #13) |
# MAGIC | 1.5 | 2026-11-16 | juniorOnData | Documentação no padrão oficial (PR #15) |

# COMMAND ----------

# [PY] E01 Imports
import glob
import hashlib
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import uuid
import zipfile
from collections import Counter
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, StringType, StructField, StructType

# COMMAND ----------

# [PY] E02 Configuração e log
VOLUME_PATH = "/Volumes/fiap_analytics/bronze/geosampa_history_flooding_events"
TABLE = "fiap_analytics.bronze.geosampa_history_flooding_events"

ENVELOPE_SIZE = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}

BATCH_ID = str(uuid.uuid4())

# Log padrão da origem (src/common/ingestion_log.py). Sobe pelas pastas até achar src/ para importar o módulo.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common.ingestion_log import IngestionLog  # noqa: E402

LOG_TABLE = "fiap_analytics.bronze.geosampa_ingestion_log"
FONTE = "geosampa_history_flooding_events"
ETAPA = "bronze"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

# MAGIC %sql
# MAGIC -- [SQL] E03 Volume
# MAGIC CREATE VOLUME IF NOT EXISTS fiap_analytics.bronze.geosampa_history_flooding_events;

# COMMAND ----------

# [PY] E04 Funções
def sha256_of(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def gpkg_to_wkb(blob):
    """Remove o cabecalho do GeoPackage e devolve o WKB padrao."""
    if blob is None:
        return None
    data = bytes(blob)
    if data[:2] != b"GP":
        return None
    flags = data[3]
    if (flags >> 4) & 1:  # geometria vazia
        return None
    return data[8 + ENVELOPE_SIZE[(flags >> 1) & 7]:]


def to_text(value):
    return None if value is None else str(value)


def is_loaded(sha):
    if not spark.catalog.tableExists(TABLE):
        return False
    return not spark.table(TABLE).filter(F.col("_source_sha256") == sha).isEmpty()


def source_year(src):
    match = re.search(r"(20\d{2})", os.path.basename(src))
    return int(match.group(1)) if match else None


def version_key(src):
    """Chave da versão: o ano do arquivo, ou o caminho quando o nome não tem ano."""
    year = source_year(src)
    return str(year) if year else src


# Mesma chave, calculada sobre a tabela.
VERSION_KEY = F.coalesce(F.col("_source_year").cast("string"), F.col("_source_file"))


def remove_superseded():
    """Mantém, para cada ano, só a versão carregada por último. Devolve {chave: linhas removidas}."""
    if not spark.catalog.tableExists(TABLE):
        return Counter()
    versions = (
        spark.table(TABLE)
        .groupBy(VERSION_KEY.alias("key"), "_source_sha256")
        .agg(F.max("_ingested_at").alias("loaded_at"), F.count("*").alias("rows"))
        .collect()
    )
    latest = {}
    for v in versions:
        if v.key not in latest or v.loaded_at > latest[v.key].loaded_at:
            latest[v.key] = v
    old = [v for v in versions if v._source_sha256 != latest[v.key]._source_sha256]
    if old:
        shas = ", ".join(f"'{v._source_sha256}'" for v in old)
        spark.sql(f"DELETE FROM {TABLE} WHERE _source_sha256 IN ({shas})")

    left = (spark.table(TABLE).groupBy(VERSION_KEY.alias("key"))
            .agg(F.countDistinct("_source_sha256").alias("n")).filter("n > 1").collect())
    assert not left, f"Ainda há mais de uma versão para: {[r.key for r in left]}"

    removed = Counter()
    for v in old:
        removed[v.key] += v.rows
        print(f"SUBSTITUIDO: versão anterior de {v.key} ({v._source_sha256[:8]}, {v.rows} linhas) removida")
    return removed


def read_gpkg(local_gpkg, source_file, sha, year):
    """Le cada camada do GPKG e devolve [(camada, n_linhas, DataFrame)]."""
    conn = sqlite3.connect(local_gpkg)
    result = []
    try:
        layers = [r[0] for r in conn.execute(
            "SELECT table_name FROM gpkg_contents WHERE data_type = 'features'")]
        for layer in layers:
            geom = conn.execute(
                "SELECT column_name, srs_id FROM gpkg_geometry_columns WHERE table_name = ?",
                (layer,)).fetchone()
            geom_col, srs_id = geom if geom else (None, -1)

            attrs = [r[1] for r in conn.execute(f'PRAGMA table_info("{layer}")')
                     if r[1] != geom_col]
            cols = [f'"{c}"' for c in attrs] + [f'"{geom_col}"' if geom_col else "NULL"]
            raw = conn.execute(f'SELECT {", ".join(cols)} FROM "{layer}"').fetchall()

            schema = StructType(
                [StructField(c, StringType(), True) for c in attrs]
                + [StructField("geom_wkb", BinaryType(), True)]
            )
            rows = [tuple(to_text(v) for v in r[:-1]) + (gpkg_to_wkb(r[-1]),) for r in raw]

            df = (
                spark.createDataFrame(rows, schema)
                .withColumn("_source_file", F.lit(source_file))
                .withColumn("_source_layer", F.lit(layer))
                .withColumn("_source_year", F.lit(year).cast("int"))
                .withColumn("_source_sha256", F.lit(sha))
                .withColumn("_source_srs_id", F.lit(int(srs_id)).cast("int"))
                .withColumn("_ingested_at", F.current_timestamp())
            )
            result.append((layer, len(rows), df))
    finally:
        conn.close()
    return result

# COMMAND ----------

# [PY] E05 Leitura dos arquivos
sources = sorted(
    p for p in glob.glob(f"{VOLUME_PATH}/**/*", recursive=True)
    if os.path.isfile(p) and p.lower().endswith((".gpkg", ".zip"))
)

# Um arquivo por ano no volume: com dois, a versão antiga e a nova se substituiriam a cada execução.
by_key = {}
for src in sources:
    by_key.setdefault(version_key(src), []).append(os.path.basename(src))
conflicts = {k: v for k, v in by_key.items() if len(v) > 1}
if conflicts:
    for key, names in conflicts.items():
        registrar(key, "FALHA", datetime.now(timezone.utc),
                  mensagem=f"mais de um arquivo para o mesmo ano no volume: {names}")
    raise RuntimeError(f"Mais de um arquivo para o mesmo ano no volume: {conflicts}. "
                       "Deixe só a versão mais recente de cada ano e rode de novo.")

frames = []
expected = Counter()
pending_log = []  # (arquivo, sha256, janela, iniciado_em) dos arquivos lidos, registrados depois da gravação

for src in sources:
    sha = sha256_of(src)
    if is_loaded(sha):
        print(f"PULADO (ja carregado): {os.path.basename(src)}")
        continue

    year = source_year(src)
    janela = str(year) if year else os.path.basename(src)
    started = datetime.now(timezone.utc)

    try:
        with tempfile.TemporaryDirectory() as tmp:
            if src.lower().endswith(".zip"):
                with zipfile.ZipFile(src) as z:
                    for name in z.namelist():
                        target = os.path.realpath(os.path.join(tmp, name))
                        if not target.startswith(os.path.realpath(tmp) + os.sep):
                            raise ValueError(f"Caminho suspeito no zip: {name}")
                    z.extractall(tmp)
                gpkgs = [p for p in glob.glob(f"{tmp}/**/*", recursive=True)
                         if p.lower().endswith(".gpkg")]
            else:
                local = os.path.join(tmp, "source.gpkg")
                shutil.copy(src, local)
                gpkgs = [local]

            for g in gpkgs:
                for layer, n, df in read_gpkg(g, src, sha, year):
                    frames.append(df)
                    expected[(sha, layer)] += n
                    print(f"LIDO: {os.path.basename(src)} / {layer}: {n} linhas (ano {year})")
    except Exception as exc:
        registrar(janela, "FALHA", started, arquivo=src, sha256=sha, mensagem=str(exc))
        raise

    pending_log.append((src, sha, janela, started))

# COMMAND ----------

# [PY] E06 Gravação
if not frames:
    print("Nada novo para carregar.")
    # Termina a limpeza de uma execução interrompida entre a gravação e a remoção da versão anterior.
    remove_superseded()
else:
    try:
        bronze = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)

        total_expected = sum(expected.values())
        total_df = bronze.count()
        assert total_df == total_expected, f"Lido {total_expected}, no DataFrame {total_df}"

        (bronze.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(TABLE))

        target = spark.table(TABLE)
        for (sha, layer), n in expected.items():
            got = target.filter(
                (F.col("_source_sha256") == sha) & (F.col("_source_layer") == layer)
            ).count()
            assert got == n, f"Camada {layer} ({sha[:8]}): lido {n}, gravado {got}"

        # Arquivo corrigido de um ano já carregado: a versão nova fica e a anterior sai.
        removed = remove_superseded()
    except Exception as exc:
        for src, sha, janela, started in pending_log:
            registrar(janela, "FALHA", started, arquivo=src, sha256=sha, mensagem=str(exc))
        raise

    for src, sha, janela, started in pending_log:
        rows = sum(n for (s, _), n in expected.items() if s == sha)
        replaced = removed.get(version_key(src), 0)
        registrar(janela, "SUCESSO" if rows else "SEM_DADOS", started, registros=rows, arquivo=src, sha256=sha,
                  mensagem=f"substituiu a versão anterior do arquivo ({replaced} linhas removidas)" if replaced else None)
    print(f"OK: {total_expected} linhas gravadas e conferidas em {TABLE}")

display(
    spark.table(TABLE).groupBy("_source_year").agg(F.count("*").alias("rows")).orderBy("_source_year")
)
