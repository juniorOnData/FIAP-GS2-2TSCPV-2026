# Databricks notebook source
# MAGIC %md
# MAGIC # 01_export_web
# MAGIC
# MAGIC Última etapa do job `ChuvaSegura`: publica as tabelas Gold no contrato estático do site (`app/DATA_CONTRACT.md`), no Volume `fiap_analytics.gold.chuvasegura_web`.
# MAGIC
# MAGIC 1. Roda depois de todas as fontes (`run_if: ALL_DONE`): uma API fora do ar não impede a publicação das outras fontes.
# MAGIC 2. Cada fonte só é exportada se a qualidade Gold dela rodou **nesta execução** do job e não teve falha `ERRO`. Fora disso, a publicação é cancelada, para não republicar dados de uma carga que falhou no meio.
# MAGIC 3. Nesta versão, só o GeoSampa é exportado (ocorrências e subprefeituras). As outras fontes entram como novas funções `exportar_<fonte>()`.
# MAGIC 4. O `manifest.json` é gravado por último: ele marca a versão completa do pacote.
# MAGIC
# MAGIC Depois, copie os arquivos do Volume para `app/data/gold/` e troque `DATA_ROOT` em `app/assets/js/config.js` para `data/gold`.
# MAGIC
# MAGIC Widget `inicio_execucao`: hora de início do job (`{{job.start_time.iso_datetime}}`, preenchida pelo job). Vazio numa execução manual: vale a execução de qualidade mais recente.

# COMMAND ----------

dbutils.widgets.text("inicio_execucao", "", "Início do job (ISO 8601); vazio = última qualidade")

# COMMAND ----------

import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")  # executado_em e inicio_execucao comparados em UTC

CATALOG = "fiap_analytics"
VOLUME = f"{CATALOG}.gold.chuvasegura_web"
VOLUME_PATH = f"/Volumes/{CATALOG}/gold/chuvasegura_web"

DIM_TEMPO = f"{CATALOG}.gold.dim_tempo"
AGORA = datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")
VERSAO = "1.1.0"
INICIO_EXECUCAO = dbutils.widgets.get("inicio_execucao").strip()

print(f"Publicação {VERSAO} em {AGORA} | início do job: {INICIO_EXECUCAO or '(execução manual)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trava de qualidade por fonte

# COMMAND ----------


def qualidade_gold(fonte):
    """Resultado da execução de qualidade Gold mais recente da fonte. Cancela a publicação se ela teve
    falha ERRO ou, dentro do job, se não rodou nesta execução (a carga da fonte falhou antes)."""
    tabela = f"{CATALOG}.bronze.{fonte}_data_quality_log"
    if not spark.catalog.tableExists(tabela):
        raise RuntimeError(f"Tabela de qualidade não encontrada: {tabela}")
    dq = spark.table(tabela).where((F.col("etapa") == "gold") & (F.col("fonte") == fonte))
    ultima = dq.orderBy(F.col("executado_em").desc()).select("batch_id", "executado_em").first()
    if not ultima:
        raise RuntimeError(f"Não há execução de qualidade Gold de {fonte}.")
    if INICIO_EXECUCAO:
        inicio = spark.range(1).select(F.lit(INICIO_EXECUCAO).cast("timestamp").alias("t")).first()["t"]
        if inicio is None:
            raise ValueError(f"inicio_execucao inválido: {INICIO_EXECUCAO!r}")
        if ultima["executado_em"] < inicio:
            raise RuntimeError(
                f"A qualidade Gold de {fonte} não rodou nesta execução (última em {ultima['executado_em']}): "
                "a carga da fonte falhou antes. Publicação cancelada.")
    run = dq.where(F.col("batch_id") == ultima["batch_id"])
    erros = run.where((F.col("severidade") == "ERRO") & (F.col("status") == "FALHA")).count()
    avisos = run.where((F.col("severidade") == "AVISO") & (F.col("status") == "FALHA")).count()
    if erros:
        raise RuntimeError(f"A qualidade Gold de {fonte} tem {erros} regra(s) ERRO em falha. Publicação cancelada.")
    quando = ultima["executado_em"].isoformat(timespec="seconds")
    estado = "aprovado_com_avisos" if avisos else "aprovado"
    descricao = (f"Regras Gold executadas em {quando} (UTC): sem falhas ERRO; {avisos} aviso(s)." if avisos
                 else f"Regras Gold executadas em {quando} (UTC): aprovadas, sem falhas ERRO ou AVISO.")
    return {"estado": estado, "descricao": descricao, "avisos": avisos}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conteúdo do pacote

# COMMAND ----------


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def dataset_meta(dataset_id, titulo, natureza, fonte, licenca, tabelas, registros, qualidade,
                 periodo=None, data_referencia=None, granularidade=None, extra=None):
    meta = {
        "id": dataset_id, "titulo": titulo, "versao": VERSAO, "natureza": natureza, "fonte": fonte,
        "tabelas_origem": tabelas, "camada_origem": "Gold", "processado_em": AGORA, "publicado_em": AGORA,
        "periodo": periodo, "data_referencia": data_referencia, "granularidade": granularidade,
        "licenca": licenca, "qualidade": {"estado": qualidade["estado"], "descricao": qualidade["descricao"]},
        "registros": registros,
    }
    if extra:
        meta.update(extra)
    return meta


def entrada_manifesto(arquivo, meta, extra=None):
    entrada = {
        "id": meta["id"], "arquivo": arquivo, "titulo": meta["titulo"], "fonte": meta["fonte"]["id"],
        "natureza": meta["natureza"], "tabelas_origem": meta["tabelas_origem"],
        "processado_em": AGORA, "publicado_em": AGORA, "periodo": meta.get("periodo"),
        "data_referencia": meta.get("data_referencia"), "registros": meta["registros"],
        "qualidade": meta["qualidade"]["estado"],
    }
    if extra:
        entrada.update(extra)
    return entrada

# COMMAND ----------


def exportar_geosampa():
    """Ocorrências históricas e subprefeituras. Retorna (arquivos, entradas do manifesto, linhagem)."""
    fato = f"{CATALOG}.gold.fact_geosampa_flooding_events"
    dim_tipo = f"{CATALOG}.gold.dim_geosampa_type_flooding_events"
    dim_sp = f"{CATALOG}.gold.dim_geosampa_subprefecture_area"
    fonte = {"id": "geosampa", "nome": "GeoSampa — Prefeitura de São Paulo", "url": "https://geosampa.prefeitura.sp.gov.br/"}
    licenca = "Dados abertos da Prefeitura de São Paulo; consultar os termos do GeoSampa."
    qualidade = qualidade_gold("geosampa")

    joined = (
        spark.table(fato).alias("f")
        .join(spark.table(DIM_TEMPO).alias("t"), F.col("f.COD_DIM_TEMPO") == F.col("t.COD_DIM_TEMPO"), "left")
        .join(spark.table(dim_tipo).alias("tipo"), F.col("f.COD_DIM_OCORRENCIA") == F.col("tipo.COD_DIM_OCORRENCIA"), "left")
        .join(spark.table(dim_sp).alias("sp"), F.col("f.COD_DIM_SUBPREFEITURA") == F.col("sp.COD_DIM_SUBPREFEITURA"), "left")
    )
    total_fato = joined.count()
    # Só pontos com data e com coordenadas no envelope de qualidade do projeto; o descarte vai para o manifesto.
    validos = joined.where(
        F.col("t.DAT_DIA").isNotNull()
        & F.col("f.NUM_LATITUDE").between(-24.1, -23.3)
        & F.col("f.NUM_LONGITUDE").between(-47.0, -46.3)
    )
    eventos = validos.select(
        F.col("f.NUM_ANO_ORIGEM").cast("int").alias("ano_origem"),
        F.col("f.NUM_ID_OCORRENCIA").cast("long").alias("id"),
        F.date_format(F.col("t.DAT_DIA"), "yyyy-MM-dd").alias("data"),
        F.coalesce(F.col("tipo.DSC_TIPO_OCORRENCIA"), F.lit("Não informado")).alias("tipo"),
        F.when(F.col("sp.COD_DIM_SUBPREFEITURA") == -1, F.lit(None).cast("string")).otherwise(F.col("sp.SIG_SUBPREFEITURA")).alias("subprefeitura"),
        F.col("f.NUM_LATITUDE").cast("double").alias("lat"),
        F.col("f.NUM_LONGITUDE").cast("double").alias("lon"),
    ).orderBy("data", "ano_origem", "id")

    nomes = ("ano_origem", "id", "data", "tipo", "subprefeitura", "lat", "lon")
    colunas = {k: [] for k in nomes}
    for row in eventos.toLocalIterator():
        for k in nomes:
            colunas[k].append(row[k])
    # Cruzamento com a carta SGB ainda não publicado: nulo = "não cruzado", não "fora de área suscetível".
    colunas["grau_susc_inundacao"] = [None] * len(colunas["id"])
    if not colunas["id"]:
        raise RuntimeError("A Gold do GeoSampa não gerou ocorrências com data e coordenadas válidas. Publicação cancelada.")
    descartadas = total_fato - len(colunas["id"])
    periodo = {"inicio": colunas["data"][0], "fim": colunas["data"][-1]}

    subprefeituras = [
        {"sigla": r["sigla"], "nome": r["nome"]} for r in spark.table(dim_sp)
        .where((F.col("COD_DIM_SUBPREFEITURA") != -1) & (F.col("TIP_UNIDADE") == "SUBPREFEITURA"))
        .where(F.col("SIG_SUBPREFEITURA").isNotNull() & F.col("NOM_SUBPREFEITURA").isNotNull())
        .select(F.col("SIG_SUBPREFEITURA").alias("sigla"), F.col("NOM_SUBPREFEITURA").alias("nome"))
        .dropDuplicates(["sigla"]).orderBy("nome").collect()
    ]

    meta_sp = dataset_meta("subprefeituras", "Subprefeituras", "oficial", fonte, licenca, [dim_sp], len(subprefeituras), qualidade,
                           data_referencia=periodo["fim"], granularidade={"espacial": "subprefeitura", "temporal": "cadastro"})
    meta_oc = dataset_meta(
        "ocorrencias", "Ocorrências históricas", "historico", fonte, licenca, [fato, DIM_TEMPO, dim_tipo, dim_sp],
        len(colunas["id"]), qualidade, periodo=periodo, granularidade={"espacial": "ponto", "temporal": "dia"},
        extra={"unidade": "ocorrências", "registros_origem": total_fato, "registros_descartados": descartadas,
               "observacao": "A Gold do GeoSampa não informa hora do evento. O campo de suscetibilidade fica nulo até a publicação do cruzamento com a carta SGB."},
    )
    arquivos = {
        "subprefeituras.json": json_bytes({"meta": meta_sp, "data": subprefeituras}),
        "ocorrencias.json": json_bytes({"meta": meta_oc, "data": colunas}),
    }
    entradas = [
        entrada_manifesto("subprefeituras.json", meta_sp, {"bytes": len(arquivos["subprefeituras.json"])}),
        entrada_manifesto("ocorrencias.json", meta_oc, {"bytes": len(arquivos["ocorrencias.json"]), "registros_origem": total_fato,
                                                       "registros_descartados": descartadas}),
    ]
    linhagem = {
        "fonte": "GeoSampa",
        "tabelas": ["bronze.geosampa_history_flooding_events", "silver.geosampa_flooding_events", "gold.fact_geosampa_flooding_events"],
        "publicado_na_web": True, "processado_em": AGORA, "qualidade": qualidade["estado"], "avisos": qualidade["avisos"],
    }
    print(f"GeoSampa: {len(colunas['id'])} ocorrências de {total_fato} ({descartadas} sem data ou coordenada válida); "
          f"{len(subprefeituras)} subprefeituras; qualidade {qualidade['estado']}")
    return arquivos, entradas, linhagem, periodo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Montagem e gravação

# COMMAND ----------

arquivos, datasets, linhagem, periodo_oc = exportar_geosampa()

manifesto = {
    "produto": "ChuvaSegura-SP",
    "modo": "gold",
    "aviso": "Dados reais publicados a partir das tabelas Gold do Databricks. Somente os conjuntos listados estão disponíveis nesta publicação.",
    "versao": VERSAO,
    "referencia": f"{periodo_oc['fim']}T23:59:00-03:00",
    "publicado_em": AGORA,
    "catalogo": CATALOG,
    "datasets": datasets,
    "linhagem": [linhagem],
    "nao_publicados": [
        {"id": "chuva", "titulo": "Chuva observada", "motivo": "A publicação atual contém somente dados do GeoSampa."},
        {"id": "nivel", "titulo": "Nível de rios e córregos", "motivo": "A publicação atual contém somente dados do GeoSampa."},
        {"id": "suscetibilidade", "titulo": "Suscetibilidade a inundações", "motivo": "O cruzamento com as áreas do SGB será publicado em etapa posterior."},
        {"id": "avisos_oficiais", "titulo": "Avisos oficiais", "motivo": "Nenhuma fonte de avisos está integrada ao pipeline."},
        {"id": "limites", "titulo": "Limites administrativos", "motivo": "A Gold contém o cadastro, mas não a geometria dos limites."},
        {"id": "setores", "titulo": "Setores censitários (IBGE)", "motivo": "Camada ainda não exportada para a web."},
        {"id": "vias", "titulo": "Rede viária (OpenStreetMap)", "motivo": "Camada ainda não exportada para a web."},
    ],
}

spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME}")
os.makedirs(VOLUME_PATH, exist_ok=True)
for nome, conteudo in {**arquivos, "manifest.json": json_bytes(manifesto)}.items():  # manifesto por último
    with open(f"{VOLUME_PATH}/{nome}", "wb") as fh:
        fh.write(conteudo)
    print(f"{nome}: {len(conteudo) / 1024:.1f} KB")

print(f"Pacote {VERSAO} publicado em {VOLUME_PATH}")
