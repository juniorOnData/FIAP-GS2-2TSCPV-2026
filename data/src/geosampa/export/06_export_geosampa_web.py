# Databricks notebook source
# MAGIC %md
# MAGIC # Exportar GeoSampa Gold para o site ChuvaSegura-SP
# MAGIC
# MAGIC Publica ocorrências e subprefeituras no contrato estático da aplicação em
# MAGIC `/Volumes/fiap_analytics/gold/geosampa_chuvasegura_web/`. Depois, baixe os três
# MAGIC arquivos gerados (`manifest.json`, `ocorrencias.json` e `subprefeituras.json`)
# MAGIC para `app/data/gold/` antes de trocar `DATA_ROOT` para `data/gold`.
# MAGIC
# MAGIC Execute depois do job `chuvasegura_geosampa` e da etapa `quality_gold_geosampa`.
# MAGIC As demais fontes continuam ausentes nesse primeiro pacote real.

# COMMAND ----------

from datetime import datetime
from zoneinfo import ZoneInfo
import json

from pyspark.sql import functions as F

CATALOG = "fiap_analytics"
VOLUME = f"{CATALOG}.gold.geosampa_chuvasegura_web"
VOLUME_PATH = f"/Volumes/{CATALOG}/gold/geosampa_chuvasegura_web"
DQ_TABLE = f"{CATALOG}.bronze.geosampa_data_quality_log"

FACT = f"{CATALOG}.gold.fact_geosampa_flooding_events"
DIM_TEMPO = f"{CATALOG}.gold.dim_tempo"
DIM_TIPO = f"{CATALOG}.gold.dim_geosampa_type_flooding_events"
DIM_SUBPREFEITURA = f"{CATALOG}.gold.dim_geosampa_subprefecture_area"

FONTE = {
    "id": "geosampa",
    "nome": "GeoSampa — Prefeitura de São Paulo",
    "url": "https://geosampa.prefeitura.sp.gov.br/",
}
LICENCA = "Dados abertos da Prefeitura de São Paulo; consultar os termos do GeoSampa."
AGORA = datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")
VERSAO = "1.0.0"

# COMMAND ----------

# Use o resultado da etapa de qualidade Gold mais recente. A própria tarefa de
# qualidade já interrompe o job se uma regra ERRO falhar; aqui bloqueamos também
# uma exportação sem execução registrada ou com falha de severidade ERRO.
if not spark.catalog.tableExists(DQ_TABLE):
    raise RuntimeError(f"Tabela de qualidade não encontrada: {DQ_TABLE}")

dq = spark.table(DQ_TABLE).where((F.col("etapa") == "gold") & (F.col("fonte") == "geosampa"))
ultima = dq.orderBy(F.col("executado_em").desc()).select("batch_id").first()
if not ultima or not ultima["batch_id"]:
    raise RuntimeError("Não há execução de qualidade Gold do GeoSampa; rode quality_gold_geosampa primeiro.")

dq_run = dq.where(F.col("batch_id") == ultima["batch_id"])
erros = dq_run.where((F.col("severidade") == "ERRO") & (F.col("status") == "FALHA")).count()
avisos = dq_run.where((F.col("severidade") == "AVISO") & (F.col("status") == "FALHA")).count()
if erros:
    raise RuntimeError(f"A qualidade Gold do GeoSampa tem {erros} regra(s) ERRO em falha; exportação cancelada.")

quality_state = "aprovado_com_avisos" if avisos else "aprovado"
quality_description = (
    f"Regras Gold executadas em {AGORA}: sem falhas ERRO; {avisos} aviso(s)."
    if avisos else f"Regras Gold executadas em {AGORA}: aprovadas, sem falhas ERRO ou AVISO."
)
quality = {"estado": quality_state, "descricao": quality_description}

# COMMAND ----------

# Só exporta pontos com coordenadas dentro do envelope de qualidade do projeto.
# A quantidade removida fica no manifesto para que a filtragem seja transparente.
f = spark.table(FACT).alias("f")
t = spark.table(DIM_TEMPO).alias("t")
tipo = spark.table(DIM_TIPO).alias("tipo")
sp = spark.table(DIM_SUBPREFEITURA).alias("sp")

joined = (
    f.join(t, F.col("f.COD_DIM_TEMPO") == F.col("t.COD_DIM_TEMPO"), "left")
     .join(tipo, F.col("f.COD_DIM_OCORRENCIA") == F.col("tipo.COD_DIM_OCORRENCIA"), "left")
     .join(sp, F.col("f.COD_DIM_SUBPREFEITURA") == F.col("sp.COD_DIM_SUBPREFEITURA"), "left")
)

total_fato = joined.count()
localizado = joined.where(
    F.col("f.NUM_LATITUDE").between(-24.1, -23.3)
    & F.col("f.NUM_LONGITUDE").between(-47.0, -46.3)
)
sem_coordenada = total_fato - localizado.count()

eventos = (
    localizado.select(
        F.col("f.NUM_ANO_ORIGEM").cast("int").alias("ano_origem"),
        F.col("f.NUM_ID_OCORRENCIA").cast("long").alias("id"),
        F.date_format(F.col("t.DAT_DIA"), "yyyy-MM-dd").alias("data"),
        F.coalesce(F.col("tipo.DSC_TIPO_OCORRENCIA"), F.lit("Não informado")).alias("tipo"),
        F.when(F.col("sp.COD_DIM_SUBPREFEITURA") == -1, F.lit(None).cast("string"))
         .otherwise(F.col("sp.SIG_SUBPREFEITURA")).alias("subprefeitura"),
        F.col("f.NUM_LATITUDE").cast("double").alias("lat"),
        F.col("f.NUM_LONGITUDE").cast("double").alias("lon"),
    )
    .orderBy("data", "ano_origem", "id")
)

colunas = {
    "ano_origem": [], "id": [], "data": [], "tipo": [],
    "subprefeitura": [], "lat": [], "lon": [], "grau_susc_inundacao": [],
}
for row in eventos.toLocalIterator():
    for key in ("ano_origem", "id", "data", "tipo", "subprefeitura", "lat", "lon"):
        value = row[key]
        colunas[key].append(value.isoformat() if hasattr(value, "isoformat") else value)
    # O primeiro pacote publica GeoSampa sem o cruzamento espacial com a carta SGB.
    # null significa “não cruzado nesta publicação”, não “fora de área suscetível”.
    colunas["grau_susc_inundacao"].append(None)

if not colunas["id"]:
    raise RuntimeError("A Gold do GeoSampa não gerou ocorrências com coordenadas válidas; publicação cancelada.")

periodo = {"inicio": min(colunas["data"]), "fim": max(colunas["data"])}
referencia = f"{periodo['fim']}T23:59:00-03:00"

subpref_dim = (
    spark.table(DIM_SUBPREFEITURA)
    .where((F.col("COD_DIM_SUBPREFEITURA") != -1) & (F.col("TIP_UNIDADE") == "SUBPREFEITURA"))
    .where(F.col("SIG_SUBPREFEITURA").isNotNull() & F.col("NOM_SUBPREFEITURA").isNotNull())
    .select(F.col("SIG_SUBPREFEITURA").alias("sigla"), F.col("NOM_SUBPREFEITURA").alias("nome"))
    .dropDuplicates(["sigla"])
    .orderBy("nome")
)
subprefeituras = [{"sigla": r["sigla"], "nome": r["nome"]} for r in subpref_dim.collect()]

# COMMAND ----------

def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def dataset_meta(dataset_id, titulo, natureza, tabelas, registros, periodo=None, data_referencia=None, granularidade=None, extra=None):
    result = {
        "id": dataset_id,
        "titulo": titulo,
        "versao": VERSAO,
        "natureza": natureza,
        "fonte": FONTE,
        "tabelas_origem": tabelas,
        "camada_origem": "Gold",
        "processado_em": AGORA,
        "publicado_em": AGORA,
        "periodo": periodo,
        "data_referencia": data_referencia,
        "granularidade": granularidade,
        "licenca": LICENCA,
        "qualidade": quality,
        "registros": registros,
    }
    if extra:
        result.update(extra)
    return result


datasets = [
    {
        "id": "subprefeituras", "arquivo": "subprefeituras.json", "titulo": "Subprefeituras",
        "fonte": "geosampa", "natureza": "oficial",
        "tabelas_origem": [DIM_SUBPREFEITURA], "processado_em": AGORA, "publicado_em": AGORA,
        "data_referencia": referencia[:10], "registros": len(subprefeituras), "qualidade": quality_state,
    },
    {
        "id": "ocorrencias", "arquivo": "ocorrencias.json", "titulo": "Ocorrências históricas",
        "fonte": "geosampa", "natureza": "historico",
        "tabelas_origem": [FACT, DIM_TEMPO, DIM_TIPO, DIM_SUBPREFEITURA],
        "processado_em": AGORA, "publicado_em": AGORA, "periodo": periodo,
        "registros": len(colunas["id"]), "registros_origem": total_fato,
        "registros_sem_coordenada_valida": sem_coordenada, "qualidade": quality_state,
    },
]

files = {
    "subprefeituras.json": {
        "meta": dataset_meta(
            "subprefeituras", "Subprefeituras", "oficial", [DIM_SUBPREFEITURA], len(subprefeituras),
            data_referencia=referencia[:10], granularidade={"espacial": "subprefeitura", "temporal": "cadastro"},
        ),
        "data": subprefeituras,
    },
    "ocorrencias.json": {
        "meta": dataset_meta(
            "ocorrencias", "Ocorrências históricas", "historico",
            [FACT, DIM_TEMPO, DIM_TIPO, DIM_SUBPREFEITURA], len(colunas["id"]),
            periodo=periodo, granularidade={"espacial": "ponto", "temporal": "dia"},
            extra={
                "unidade": "ocorrências",
                "registros_origem": total_fato,
                "registros_sem_coordenada_valida": sem_coordenada,
                "observacao": "A Gold do GeoSampa não informa hora do evento. O campo de suscetibilidade permanece nulo até a publicação do cruzamento com a carta SGB.",
            },
        ),
        "data": colunas,
    },
}

for filename, payload in files.items():
    raw = json_bytes(payload)
    files[filename] = raw
    next(d for d in datasets if d["arquivo"] == filename)["bytes"] = len(raw)

manifest = {
    "produto": "ChuvaSegura-SP",
    "modo": "gold",
    "aviso": "Dados reais publicados a partir das tabelas Gold do Databricks. Somente os conjuntos listados estão disponíveis nesta publicação.",
    "versao": VERSAO,
    "referencia": referencia,
    "publicado_em": AGORA,
    "catalogo": CATALOG,
    "datasets": datasets,
    "linhagem": [{
        "fonte": "GeoSampa",
        "tabelas": [
            "bronze.geosampa_history_flooding_events",
            "silver.geosampa_flooding_events",
            "gold.fact_geosampa_flooding_events",
        ],
        "publicado_na_web": True,
        "processado_em": AGORA,
        "qualidade": quality_state,
        "avisos": avisos,
    }],
    "nao_publicados": [
        {"id": "chuva", "titulo": "Chuva observada", "motivo": "A publicação atual contém somente dados GeoSampa."},
        {"id": "nivel", "titulo": "Nível de rios e córregos", "motivo": "A publicação atual contém somente dados GeoSampa."},
        {"id": "suscetibilidade", "titulo": "Suscetibilidade a inundações", "motivo": "O cruzamento com as áreas SGB será publicado em etapa posterior."},
        {"id": "avisos_oficiais", "titulo": "Avisos oficiais", "motivo": "Nenhuma fonte de avisos está integrada ao pipeline."},
        {"id": "limites", "titulo": "Limites administrativos", "motivo": "A Gold contém o cadastro, mas não a geometria dos limites."},
        {"id": "setores", "titulo": "Setores censitários (IBGE)", "motivo": "Camada ainda não exportada para a web."},
        {"id": "vias", "titulo": "Rede viária (OpenStreetMap)", "motivo": "Camada ainda não exportada para a web."},
    ],
}
files["manifest.json"] = json_bytes(manifest)

# COMMAND ----------

# Volume gerenciado no Unity Catalog. O manifesto é gravado por último para ser
# o marcador da versão completa do pacote.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME}")
dbutils.fs.mkdirs(VOLUME_PATH)
for filename in ("subprefeituras.json", "ocorrencias.json"):
    dbutils.fs.put(f"{VOLUME_PATH}/{filename}", files[filename].decode("utf-8"), True)
dbutils.fs.put(f"{VOLUME_PATH}/manifest.json", files["manifest.json"].decode("utf-8"), True)

print(f"Export publicado em {VOLUME_PATH}")
print(f"Ocorrências no export: {len(colunas['id'])} de {total_fato}; sem coordenada válida: {sem_coordenada}")
print(f"Subprefeituras no export: {len(subprefeituras)}")
print(f"Qualidade: {quality_state} ({avisos} avisos)")
display(spark.createDataFrame([(name, len(raw)) for name, raw in files.items()], ["arquivo", "bytes"]))
