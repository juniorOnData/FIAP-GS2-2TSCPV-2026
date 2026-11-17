# Databricks notebook source
# MAGIC %md
# MAGIC # 01_export_web
# MAGIC
# MAGIC Última etapa do job `ChuvaSegura`: publica as tabelas Gold no contrato estático do site (`app/DATA_CONTRACT.md`) e envia o pacote ao GitHub, de onde a Vercel republica o site.
# MAGIC
# MAGIC 1. Roda depois de todas as fontes (`run_if: ALL_DONE`).
# MAGIC 2. Cada fonte só é exportada se a qualidade Gold dela rodou **nesta execução** do job e não teve falha `ERRO`.
# MAGIC 3. Fontes exportadas hoje: GeoSampa (ocorrências e subprefeituras), CEMADEN, SIBH e INMET (estações, chuva diária e horária) e SIBH (nível). IBGE, OSM e a suscetibilidade do SGB entram em etapas posteriores.
# MAGIC 4. O pacote é gravado no Volume `fiap_analytics.gold.chuvasegura_web` (o `manifest.json` por último). **Tudo ou nada no site:** só vai ao GitHub se todas as fontes pedidas passaram. Se alguma falhar, o site continua com a versão anterior, o que passou fica no Volume para conferência e a tarefa termina com erro.
# MAGIC 5. O commit só acontece se os dados mudaram (compara o SHA-256 de cada conjunto com o manifesto publicado).
# MAGIC
# MAGIC Widgets: `inicio_execucao` (preenchido pelo job; vazio numa execução manual = vale a qualidade mais recente), `fontes` (lista; tire uma fonte que ainda não rodou, como o INMET antes do backfill), `github_publicar` (`sim`/`nao`), `github_repo`, `github_branch` e `unidade_nivel_origem` (unidade da cota do SIBH, `cm` por padrão: o export converte para metros).
# MAGIC
# MAGIC Segredo: scope `github`, chave `token` (token do GitHub com *Contents: read and write* só neste repositório).

# COMMAND ----------

dbutils.widgets.text("inicio_execucao", "", "Início do job (ISO 8601); vazio = última qualidade")
dbutils.widgets.text("fontes", "geosampa,cemaden,sibh,inmet", "Fontes a exportar")
dbutils.widgets.dropdown("github_publicar", "sim", ["sim", "nao"], "Enviar ao GitHub")
dbutils.widgets.text("github_repo", "juniorOnData/FIAP-GS2-2TSCPV-2026", "Repositório (dono/nome)")
dbutils.widgets.text("github_branch", "main", "Branch de produção da Vercel")
dbutils.widgets.dropdown("unidade_nivel_origem", "cm", ["cm", "m"], "Unidade do nível na fonte (SIBH)")

# COMMAND ----------

import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F

# Módulos de src/common. Sobe pelas pastas até achar src/ para importar.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common import github_publish  # noqa: E402
from common.publicacao_web import (  # noqa: E402
    DIA_INICIAL, DIAS_NIVEL, FATOR_PARA_METRO, data_para_cod, hora_sp, iso_sp, json_bytes, pior_qualidade, serie_diaria, serie_horaria,
    sha256_dados,
)

spark.conf.set("spark.sql.session.timeZone", "UTC")  # executado_em e inicio_execucao comparados em UTC

CATALOG = "fiap_analytics"
VOLUME = f"{CATALOG}.gold.chuvasegura_web"
VOLUME_PATH = f"/Volumes/{CATALOG}/gold/chuvasegura_web"
DIM_TEMPO = f"{CATALOG}.gold.dim_tempo"
PASTA_SITE = "app/data/gold"  # destino no repositório
VERSAO = "1.3.0"

AGORA_DT = datetime.now(ZoneInfo("America/Sao_Paulo"))
AGORA = AGORA_DT.isoformat(timespec="seconds")
INICIO_EXECUCAO = dbutils.widgets.get("inicio_execucao").strip()
FONTES = {f.strip().lower() for f in dbutils.widgets.get("fontes").split(",") if f.strip()}
DESCONHECIDAS = FONTES - {"geosampa", "cemaden", "sibh", "inmet"}
if DESCONHECIDAS:
    raise ValueError(f"Fontes desconhecidas em 'fontes': {sorted(DESCONHECIDAS)}")
GITHUB_PUBLICAR = dbutils.widgets.get("github_publicar") == "sim"
GITHUB_REPO = dbutils.widgets.get("github_repo").strip()
GITHUB_BRANCH = dbutils.widgets.get("github_branch").strip()
UNIDADE_NIVEL_ORIGEM = dbutils.widgets.get("unidade_nivel_origem")
FATOR_NIVEL = FATOR_PARA_METRO[UNIDADE_NIVEL_ORIGEM]
ENVELOPE = (-24.1, -23.3, -47.0, -46.3)  # lat mín, lat máx, lon mín, lon máx (município e entorno)

print(f"Publicação {VERSAO} em {AGORA} | fontes: {sorted(FONTES)} | início do job: {INICIO_EXECUCAO or '(execução manual)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trava de qualidade por fonte

# COMMAND ----------

_qualidade_cache = {}


def qualidade_gold(fonte):
    """Resultado da execução de qualidade Gold mais recente da fonte. Falha se ela teve ERRO ou, dentro do job,
    se não rodou nesta execução (a carga da fonte falhou antes)."""
    if fonte in _qualidade_cache:
        return _qualidade_cache[fonte]
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
                f"A qualidade Gold de {fonte} não rodou nesta execução (última em {ultima['executado_em']}): a carga falhou antes.")
    run = dq.where(F.col("batch_id") == ultima["batch_id"])
    erros = run.where((F.col("severidade") == "ERRO") & (F.col("status") == "FALHA")).count()
    avisos = run.where((F.col("severidade") == "AVISO") & (F.col("status") == "FALHA")).count()
    if erros:
        raise RuntimeError(f"A qualidade Gold de {fonte} tem {erros} regra(s) ERRO em falha.")
    quando = ultima["executado_em"].isoformat(timespec="seconds")
    estado = "aprovado_com_avisos" if avisos else "aprovado"
    descricao = (f"Regras Gold de {fonte} executadas em {quando} (UTC): sem falhas ERRO; {avisos} aviso(s)." if avisos
                 else f"Regras Gold de {fonte} executadas em {quando} (UTC): aprovadas, sem falhas ERRO ou AVISO.")
    _qualidade_cache[fonte] = {"estado": estado, "descricao": descricao, "avisos": avisos}
    return _qualidade_cache[fonte]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadados

# COMMAND ----------


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


def pacote(arquivo, meta, data):
    """Arquivo do site + entrada do manifesto (com o SHA-256 dos dados, usado para evitar commits sem mudança)."""
    corpo = json_bytes({"meta": meta, "data": data})
    entrada = {
        "id": meta["id"], "arquivo": arquivo, "titulo": meta["titulo"], "fonte": meta["fonte"]["id"],
        "natureza": meta["natureza"], "tabelas_origem": meta["tabelas_origem"], "processado_em": AGORA,
        "publicado_em": AGORA, "periodo": meta.get("periodo"), "data_referencia": meta.get("data_referencia"),
        "registros": meta["registros"], "qualidade": meta["qualidade"]["estado"], "bytes": len(corpo),
        "sha256_dados": sha256_dados(data),
    }
    for k in ("referencia", "registros_origem", "registros_descartados"):
        if k in meta:
            entrada[k] = meta[k]
    return corpo, entrada

# COMMAND ----------

# MAGIC %md
# MAGIC ## GeoSampa: ocorrências e subprefeituras

# COMMAND ----------


def exportar_geosampa():
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
    validos = joined.where(
        F.col("t.DAT_DIA").isNotNull()
        & F.col("f.NUM_LATITUDE").between(ENVELOPE[0], ENVELOPE[1])
        & F.col("f.NUM_LONGITUDE").between(ENVELOPE[2], ENVELOPE[3])
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
        raise RuntimeError("A Gold do GeoSampa não gerou ocorrências com data e coordenadas válidas.")
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
    arq_sp, ent_sp = pacote("subprefeituras.json", meta_sp, subprefeituras)
    arq_oc, ent_oc = pacote("ocorrencias.json", meta_oc, colunas)
    print(f"GeoSampa: {len(colunas['id'])} ocorrências de {total_fato} ({descartadas} sem data ou coordenada válida); "
          f"{len(subprefeituras)} subprefeituras; qualidade {qualidade['estado']}")
    return {
        "arquivos": {"subprefeituras.json": arq_sp, "ocorrencias.json": arq_oc},
        "datasets": [ent_sp, ent_oc],
        "linhagem": [{"fonte": "GeoSampa", "tabelas": ["bronze.geosampa_history_flooding_events", "silver.geosampa_flooding_events", "gold.fact_geosampa_flooding_events"],
                      "publicado_na_web": True, "processado_em": AGORA, "qualidade": qualidade["estado"], "avisos": qualidade["avisos"]}],
        "referencia": f"{periodo['fim']}T23:59:00-03:00",
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estações, chuva e nível (CEMADEN, SIBH, INMET)

# COMMAND ----------

REDES = {
    "cemaden": {
        "nome": "CEMADEN", "dim": f"{CATALOG}.gold.dim_cemaden_stations", "fato": f"{CATALOG}.gold.fact_cemaden_rainfall_hourly",
        "tabelas": ["dim_cemaden_stations", "fact_cemaden_rainfall_hourly"],
        "linhagem": ["bronze.cemaden_rainfall_readings", "silver.cemaden_rainfall_readings", "gold.fact_cemaden_rainfall_hourly"],
        "url": "https://www.gov.br/cemaden/",
    },
    "sibh": {
        "nome": "SIBH / SP Águas", "dim": f"{CATALOG}.gold.dim_sibh_sensors", "fato": f"{CATALOG}.gold.fact_sibh_rainfall_hourly",
        "fato_nivel": f"{CATALOG}.gold.fact_sibh_level_hourly",
        "tabelas": ["dim_sibh_sensors", "fact_sibh_rainfall_hourly", "fact_sibh_level_hourly"],
        "linhagem": ["bronze.sibh_measurements", "silver.sibh_measurements", "gold.fact_sibh_rainfall_hourly"],
        "url": "https://sibh.daee.sp.gov.br/",
    },
    "inmet": {
        "nome": "INMET", "dim": f"{CATALOG}.gold.dim_inmet_stations", "fato": f"{CATALOG}.gold.fact_inmet_weather_hourly",
        "tabelas": ["dim_inmet_stations", "fact_inmet_weather_hourly"],
        "linhagem": ["bronze.inmet_weather", "silver.inmet_weather_readings", "gold.fact_inmet_weather_hourly"],
        "url": "https://portal.inmet.gov.br/",
    },
}


def dentro(lat, lon):
    return (lat is not None and lon is not None
            and ENVELOPE[0] <= lat <= ENVELOPE[1] and ENVELOPE[2] <= lon <= ENVELOPE[3])


def cadastro_redes(redes):
    """Estações de chuva e sensores de nível, já no formato do contrato."""
    chuva, nivel = [], []
    if "cemaden" in redes:
        for r in spark.table(REDES["cemaden"]["dim"]).where(F.col("COD_DIM_ESTACAO") != -1).collect():
            if dentro(r["NUM_LATITUDE"], r["NUM_LONGITUDE"]):
                chuva.append({"id": f"cemaden:{r['COD_ESTACAO']}", "codigo": r["COD_ESTACAO"], "rede": "CEMADEN",
                              "nome": r["NOM_ESTACAO"] or f"Estação {r['COD_ESTACAO']}", "grandeza": "Chuva", "instituicao": "CEMADEN",
                              "lat": round(r["NUM_LATITUDE"], 5), "lon": round(r["NUM_LONGITUDE"], 5), "subprefeitura": None,
                              "ativa": bool(r["FLG_ATIVA"]), "tabela": "dim_cemaden_stations"})
    if "inmet" in redes:
        for r in spark.table(REDES["inmet"]["dim"]).where(F.col("COD_DIM_ESTACAO") != -1).collect():
            if dentro(r["NUM_LATITUDE"], r["NUM_LONGITUDE"]):
                chuva.append({"id": f"inmet:{r['COD_ESTACAO']}", "codigo": r["COD_ESTACAO"], "rede": "INMET",
                              "nome": r["NOM_ESTACAO"] or f"Estação {r['COD_ESTACAO']}", "grandeza": "Chuva", "instituicao": "INMET",
                              "lat": round(r["NUM_LATITUDE"], 5), "lon": round(r["NUM_LONGITUDE"], 5), "subprefeitura": None,
                              "ativa": True, "tabela": "dim_inmet_stations"})
    if "sibh" in redes:
        for r in spark.table(REDES["sibh"]["dim"]).where(F.col("COD_DIM_SENSOR") != -1).collect():
            if not dentro(r["NUM_LATITUDE"], r["NUM_LONGITUDE"]) or r["DSC_GRANDEZA"] not in ("Chuva", "Nível"):
                continue
            base = {"id": f"sibh:{r['COD_SENSOR']}", "codigo": r["COD_SENSOR"], "rede": "SIBH",
                    "nome": r["NOM_ESTACAO"] or f"Sensor {r['COD_SENSOR']}", "grandeza": r["DSC_GRANDEZA"],
                    "instituicao": r["NOM_INSTITUICAO"] or "SIBH", "lat": round(r["NUM_LATITUDE"], 5), "lon": round(r["NUM_LONGITUDE"], 5),
                    "subprefeitura": None, "ativa": bool(r["FLG_EM_OPERACAO"]), "tabela": "dim_sibh_sensors"}
            if r["DSC_GRANDEZA"] == "Chuva":
                chuva.append(base)
            else:
                nivel.append({**base, "curso_dagua": r["NOM_ESTACAO"] or f"Sensor {r['COD_SENSOR']}", "unidade": "m"})
    return chuva, nivel


def linhas_chuva_horaria(redes):
    """(id, cod_dim_tempo, hora, mm) das redes, limitado às horas já ocorridas. CEMADEN pode ter mais de um sensor por
    estação: vale o maior valor da hora (os sensores medem a mesma chuva), para não somar em dobro."""
    partes = []
    if "cemaden" in redes:
        partes.append(spark.table(REDES["cemaden"]["fato"]).groupBy("COD_ESTACAO", "COD_DIM_TEMPO", "NUM_HORA")
                      .agg(F.max("NUM_CHUVA_MM").alias("mm"))
                      .select(F.concat(F.lit("cemaden:"), "COD_ESTACAO").alias("id"), "COD_DIM_TEMPO", "NUM_HORA", "mm"))
    if "sibh" in redes:
        partes.append(spark.table(REDES["sibh"]["fato"])
                      .select(F.concat(F.lit("sibh:"), "COD_SENSOR").alias("id"), "COD_DIM_TEMPO", "NUM_HORA", F.col("NUM_CHUVA_MM").alias("mm")))
    if "inmet" in redes:
        partes.append(spark.table(REDES["inmet"]["fato"])
                      .select(F.concat(F.lit("inmet:"), "COD_ESTACAO").alias("id"), "COD_DIM_TEMPO", "NUM_HORA", F.col("NUM_CHUVA_MM").alias("mm")))
    df = partes[0]
    for p in partes[1:]:
        df = df.unionByName(p)
    agora_chave = data_para_cod(AGORA_DT.date()) * 100 + AGORA_DT.hour
    return df.where(F.col("mm").isNotNull() & ((F.col("COD_DIM_TEMPO") * 100 + F.col("NUM_HORA")) <= agora_chave))


def exportar_estacoes_e_chuva(redes):
    """Gera estacoes, chuva_diaria e chuva_horaria das redes que passaram na qualidade."""
    qual = {r: qualidade_gold(r) for r in redes}  # levanta se alguma falhar: quem chama trata por rede
    estado = pior_qualidade([q["estado"] for q in qual.values()])
    qualidade = {"estado": estado, "descricao": " ".join(q["descricao"] for q in qual.values())}
    nomes = ", ".join(REDES[r]["nome"] for r in redes)
    fonte = {"id": "multiplas", "nome": nomes}
    licenca = "Dados públicos das redes de origem; consultar os termos de cada fonte."
    tabelas = [f"gold.{t}" for r in redes for t in REDES[r]["tabelas"] if not t.startswith("fact_sibh_level")]

    cad_chuva, cad_nivel = cadastro_redes(redes)
    horaria = linhas_chuva_horaria(redes).cache()
    ref_chave = horaria.agg(F.max(F.col("COD_DIM_TEMPO") * 100 + F.col("NUM_HORA"))).first()[0]
    if ref_chave is None:
        raise RuntimeError("As fatos de chuva não têm nenhuma hora com leitura.")
    hora_final = hora_sp(ref_chave // 100, ref_chave % 100)
    dia_final = hora_final.date()

    diaria_df = horaria.groupBy("id", "COD_DIM_TEMPO").agg(F.sum("mm").alias("mm"))
    diaria = serie_diaria(((r["id"], r["COD_DIM_TEMPO"], r["mm"]) for r in diaria_df.toLocalIterator()), dia_final)
    corte = data_para_cod(dia_final - timedelta(days=8))
    recentes = horaria.where(F.col("COD_DIM_TEMPO") >= corte)
    inicio_h, hora = serie_horaria(((r["id"], r["COD_DIM_TEMPO"], r["NUM_HORA"], r["mm"]) for r in recentes.toLocalIterator()), hora_final)
    horaria.unpersist()

    # Só entram estações com leitura no período ou ativas no cadastro (inativas e sem histórico só poluem o mapa).
    com_dado = set(diaria)
    chuva = [e for e in cad_chuva if e["id"] in com_dado or e["ativa"]]
    ids = {e["id"] for e in chuva}
    diaria = {k: v for k, v in diaria.items() if k in ids}
    hora = {k: v for k, v in hora.items() if k in ids}
    if not chuva:
        raise RuntimeError("Nenhuma estação de chuva com coordenadas válidas.")

    referencia = iso_sp(hora_final)
    base_meta = dict(unidade="mm", referencia=referencia)
    meta_di = dataset_meta("chuva_diaria", "Chuva diária por estação", "observado", fonte, licenca, tabelas, len(diaria), qualidade,
                           periodo={"inicio": DIA_INICIAL.isoformat(), "fim": dia_final.isoformat()},
                           granularidade={"espacial": "estação", "temporal": "dia (soma das horas no fuso de São Paulo)"},
                           extra={**base_meta, "estacoes_publicadas": len(ids),
                                  "observacao": "Dia sem leitura é nulo, não zero. Estação com mais de um sensor: vale o maior valor de cada hora."})
    meta_ho = dataset_meta("chuva_horaria", "Chuva horária recente", "observado", fonte, licenca, tabelas, len(hora), qualidade,
                           periodo={"inicio": iso_sp(inicio_h), "fim": referencia},
                           granularidade={"espacial": "estação", "temporal": "hora (início do intervalo, fuso de São Paulo)"}, extra=base_meta)
    out = {}
    out["chuva_diaria.json"] = pacote("chuva_diaria.json", meta_di, {"inicio": DIA_INICIAL.isoformat(), "series": diaria})
    out["chuva_horaria.json"] = pacote("chuva_horaria.json", meta_ho, {"inicio": iso_sp(inicio_h), "series": hora})
    print(f"Chuva: {len(ids)} estações ({', '.join(redes)}); referência {referencia}; diária até {dia_final}; qualidade {estado}")
    linhagem = [{"fonte": REDES[r]["nome"], "tabelas": REDES[r]["linhagem"], "publicado_na_web": True, "processado_em": AGORA,
                 "qualidade": qual[r]["estado"], "avisos": qual[r]["avisos"]} for r in redes]
    return {"chuva": chuva, "nivel_cadastro": cad_nivel, "arquivos": out, "linhagem": linhagem, "referencia": referencia,
            "dia_final": dia_final, "fonte": fonte, "licenca": licenca, "qualidade": qualidade, "tabelas": tabelas}


def exportar_nivel(cad_nivel):
    """nivel.json a partir do SIBH (qualidade já conferida em exportar_estacoes_e_chuva)."""
    qualidade = qualidade_gold("sibh")
    fato = REDES["sibh"]["fato_nivel"]
    agora_chave = data_para_cod(AGORA_DT.date()) * 100 + AGORA_DT.hour
    base = (spark.table(fato).where(F.col("NUM_NIVEL_MEDIO").isNotNull() & ((F.col("COD_DIM_TEMPO") * 100 + F.col("NUM_HORA")) <= agora_chave))
            .select(F.concat(F.lit("sibh:"), "COD_SENSOR").alias("id"), "COD_DIM_TEMPO", "NUM_HORA", (F.col("NUM_NIVEL_MEDIO") * F.lit(FATOR_NIVEL)).alias("v")))
    ids = {s["id"] for s in cad_nivel}
    base = base.where(F.col("id").isin(*ids)) if ids else base
    base = base.cache()
    ref_chave = base.agg(F.max(F.col("COD_DIM_TEMPO") * 100 + F.col("NUM_HORA"))).first()[0]
    if ref_chave is None:
        raise RuntimeError("A fato de nível do SIBH não tem leituras dos sensores cadastrados.")
    hora_final = hora_sp(ref_chave // 100, ref_chave % 100)
    dia_final = hora_final.date()
    dia_ini = dia_final - timedelta(days=DIAS_NIVEL - 1)
    diario_df = (base.where(F.col("COD_DIM_TEMPO") >= data_para_cod(dia_ini)).groupBy("id", "COD_DIM_TEMPO").agg(F.avg("v").alias("v")))
    diario = serie_diaria(((r["id"], r["COD_DIM_TEMPO"], r["v"]) for r in diario_df.toLocalIterator()), dia_final, casas=3, dia_inicial=dia_ini)
    corte = data_para_cod(dia_final - timedelta(days=8))
    inicio_h, horario = serie_horaria(((r["id"], r["COD_DIM_TEMPO"], r["NUM_HORA"], r["v"]) for r in base.where(F.col("COD_DIM_TEMPO") >= corte).toLocalIterator()),
                                      hora_final, casas=3)
    base.unpersist()
    sensores = [s for s in cad_nivel if s["id"] in diario or s["ativa"]]
    ids = {s["id"] for s in sensores}
    diario = {k: v for k, v in diario.items() if k in ids}
    horario = {k: v for k, v in horario.items() if k in ids}
    fonte = {"id": "sibh", "nome": REDES["sibh"]["nome"], "url": REDES["sibh"]["url"]}
    meta = dataset_meta("nivel", "Nível de rios e córregos", "observado", fonte, "Dados públicos do SIBH.",
                        [f"gold.{t}" for t in ("fact_sibh_level_hourly", "dim_sibh_sensors")], len(ids), qualidade,
                        periodo={"inicio": dia_ini.isoformat(), "fim": dia_final.isoformat()},
                        granularidade={"espacial": "sensor", "temporal": "hora (7 dias) e média diária (365 dias)"},
                        extra={"unidade": "m", "unidade_origem": UNIDADE_NIVEL_ORIGEM,
                               "descricao_valor": "Cota informada pela fonte, convertida para metros: altitude da superfície da água na referência do sensor, não a profundidade do rio.",
                               "referencia": iso_sp(hora_final),
                               "limiares": "Não há limiares oficiais de transbordamento integrados nesta versão."})
    data = {"horario_inicio": iso_sp(inicio_h), "horario": horario, "diario_inicio": dia_ini.isoformat(), "diario": diario}
    print(f"Nível: {len(ids)} sensores; referência {iso_sp(hora_final)}; cota convertida de {UNIDADE_NIVEL_ORIGEM} para m")
    return {"nivel.json": pacote("nivel.json", meta, data)}, sensores

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução por fonte

# COMMAND ----------

arquivos, datasets, linhagem, referencias, falhas = {}, [], [], [], []
publicados = set()


def tentar(nome, fn):
    """Executa um exportador; falha de uma fonte não impede as outras de gerarem o pacote."""
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 - registrado e relançado no fim
        falhas.append((nome, f"{type(exc).__name__}: {exc}"))
        print(f"FALHA em {nome}: {exc}")
        return None


if "geosampa" in FONTES:
    r = tentar("geosampa", exportar_geosampa)
    if r:
        arquivos.update(r["arquivos"])
        datasets += r["datasets"]
        linhagem += r["linhagem"]
        referencias.append(r["referencia"])
        publicados |= {"subprefeituras", "ocorrencias"}

redes_ok = []
for rede in ("cemaden", "sibh", "inmet"):
    if rede in FONTES:
        if tentar(rede, lambda rede=rede: qualidade_gold(rede)):
            redes_ok.append(rede)

chuva_res = tentar("chuva", lambda: exportar_estacoes_e_chuva(redes_ok)) if redes_ok else None
if chuva_res:
    for nome, (corpo, entrada) in chuva_res["arquivos"].items():
        arquivos[nome] = corpo
        datasets.append(entrada)
    linhagem += chuva_res["linhagem"]
    referencias.append(chuva_res["referencia"])
    publicados |= {"chuva_diaria", "chuva_horaria"}

    cad_nivel = chuva_res["nivel_cadastro"]
    nivel_res = tentar("nivel", lambda: exportar_nivel(cad_nivel)) if "sibh" in redes_ok and cad_nivel else None
    if nivel_res:
        arq_nivel, sensores = nivel_res
        for nome, (corpo, entrada) in arq_nivel.items():
            arquivos[nome] = corpo
            datasets.append(entrada)
        publicados.add("nivel")
    else:
        sensores = []

    meta_est = dataset_meta(
        "estacoes", "Estações e sensores", "oficial", chuva_res["fonte"], chuva_res["licenca"],
        [t for t in chuva_res["tabelas"] if "fact_" not in t], len(chuva_res["chuva"]) + len(sensores), chuva_res["qualidade"],
        granularidade={"espacial": "ponto", "temporal": "cadastro"},
        extra={"observacao": "A Gold não tem a subprefeitura das estações: o filtro por subprefeitura não as inclui."})
    corpo, entrada = pacote("estacoes.json", meta_est, {"chuva": chuva_res["chuva"], "nivel": sensores})
    arquivos["estacoes.json"] = corpo
    datasets.append(entrada)
    publicados.add("estacoes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manifesto

# COMMAND ----------

if not datasets:
    raise RuntimeError("Nada foi exportado: " + "; ".join(f"{n}: {m}" for n, m in falhas))

MOTIVO_FALHA = {n: m for n, m in falhas}
CATALOGO_NAO_PUBLICADO = [
    ("chuva", "Chuva observada", "chuva_diaria"), ("nivel", "Nível de rios e córregos", "nivel"),
    ("suscetibilidade", "Suscetibilidade a inundações", None), ("avisos_oficiais", "Avisos oficiais", None),
    ("limites", "Limites administrativos", None), ("setores", "Setores censitários (IBGE)", None), ("vias", "Rede viária (OpenStreetMap)", None),
]
MOTIVOS_FIXOS = {
    "suscetibilidade": "O cruzamento e os polígonos do SGB serão publicados em etapa posterior.",
    "avisos_oficiais": "Nenhuma fonte de avisos está integrada ao pipeline.",
    "limites": "A Gold contém o cadastro das subprefeituras, mas não a geometria dos limites.",
    "setores": "Camada ainda não exportada para a web.", "vias": "Camada ainda não exportada para a web.",
}
nao_publicados = []
for id_, titulo, ds in CATALOGO_NAO_PUBLICADO:
    if ds and ds in publicados:
        continue
    if id_ in MOTIVOS_FIXOS:
        motivo = MOTIVOS_FIXOS[id_]
    else:
        fontes_rel = {"chuva": ("chuva", "cemaden", "sibh", "inmet"), "nivel": ("nivel", "sibh")}[id_]
        erros = [f"{n}: {MOTIVO_FALHA[n]}" for n in fontes_rel if n in MOTIVO_FALHA]
        motivo = ("Falhou nesta execução (" + "; ".join(erros) + ").") if erros else "Fonte não incluída nesta publicação."
    nao_publicados.append({"id": id_, "titulo": titulo, "motivo": motivo})

manifesto = {
    "produto": "ChuvaSegura-SP", "modo": "gold",
    "aviso": "Dados reais publicados a partir das tabelas Gold do Databricks. Somente os conjuntos listados estão disponíveis nesta publicação.",
    "versao": VERSAO, "referencia": max(referencias), "publicado_em": AGORA, "catalogo": CATALOG,
    "datasets": datasets, "linhagem": linhagem, "nao_publicados": nao_publicados,
}
arquivos["manifest.json"] = json_bytes(manifesto)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravação no Volume e envio ao GitHub

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME}")
os.makedirs(VOLUME_PATH, exist_ok=True)
for nome in [n for n in arquivos if n != "manifest.json"] + ["manifest.json"]:  # manifesto por último
    with open(f"{VOLUME_PATH}/{nome}", "wb") as fh:
        fh.write(arquivos[nome])
    print(f"Volume: {nome} ({len(arquivos[nome]) / 1024:.1f} KB)")

commit = None
if falhas:
    print("Há fontes com falha: o site NÃO foi atualizado (tudo ou nada). O pacote parcial está no Volume.")
elif not GITHUB_PUBLICAR:
    print("github_publicar = nao: pacote só no Volume.")
else:
    token = dbutils.secrets.get(scope="github", key="token")
    atual = github_publish.ler_json(token, GITHUB_REPO, GITHUB_BRANCH, f"{PASTA_SITE}/manifest.json")
    hashes_atuais = {d["id"]: d.get("sha256_dados") for d in (atual or {}).get("datasets", [])}
    hashes_novos = {d["id"]: d["sha256_dados"] for d in datasets}
    if atual and atual.get("modo") == "gold" and hashes_atuais == hashes_novos:
        print("Dados idênticos aos já publicados no site: sem commit.")
    else:
        resumo = ", ".join(sorted(publicados))
        commit = github_publish.publicar(
            token, GITHUB_REPO, GITHUB_BRANCH, {f"{PASTA_SITE}/{n}": c for n, c in arquivos.items()},
            f"Dados: publicação automática {AGORA}\n\nConjuntos: {resumo}. Gerado por data/src/shared/export/01_export_web.py.")
        print(f"Commit {commit} em {GITHUB_REPO}@{GITHUB_BRANCH}: a Vercel publica o site em seguida." if commit else "GitHub: sem mudanças.")

if falhas:
    raise RuntimeError("Exportação incompleta: " + " | ".join(f"{n}: {m}" for n, m in falhas))
