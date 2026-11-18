# Databricks notebook source
# MAGIC %md
# MAGIC # 01_export_web
# MAGIC
# MAGIC ## Funções do notebook
# MAGIC
# MAGIC `qualidade_gold` impede a exportação de uma fonte sem validação aprovada nesta execução. `dataset_meta` monta os metadados do conjunto publicado e `pacote` serializa os dados e calcula seu hash. `dentro` confere se uma coordenada está no recorte do município; `cadastro_redes` prepara as estações e `linhas_chuva_horaria` monta as séries por hora. `exportar_geosampa`, `exportar_estacoes_e_chuva` e `exportar_nivel` produzem os arquivos das respectivas fontes. `tentar` registra falhas por fonte para que as demais ainda possam ser avaliadas antes da publicação.
# MAGIC
# MAGIC Última etapa do job `ChuvaSegura`: publica as tabelas Gold no contrato estático do site (`app/DATA_CONTRACT.md`) e envia o pacote ao GitHub, de onde a Vercel republica o site.
# MAGIC
# MAGIC 1. Roda depois de todas as fontes (`run_if: ALL_DONE`).
# MAGIC 2. Cada fonte só é exportada se a qualidade Gold dela rodou **nesta execução** do job e não teve falha `ERRO`.
# MAGIC 3. Fontes exportadas: GeoSampa (ocorrências e subprefeituras), CEMADEN, SIBH e INMET (estações, chuva diária e horária), SIBH (nível), SGB (áreas de suscetibilidade a inundação, cruzadas com as ocorrências), IBGE (setores censitários com população) e OSM (vias principais). As geometrias são simplificadas para a escala do mapa; os limites administrativos não têm geometria na Gold.
# MAGIC 4. O pacote é gravado no Volume `fiap_analytics.gold.chuvasegura_web` (o `manifest.json` por último) e vai ao GitHub com as fontes que passaram. **Fonte com falha não derruba as outras:** os conjuntos dela continuam no site com a versão já publicada (o arquivo não é sobrescrito e o manifesto marca `mantido`). Falha que vem de antes do export (qualidade Gold reprovada ou que não rodou nesta execução) já aparece na tarefa da própria fonte, então o export termina com sucesso; erro no código do export termina a tarefa com erro.
# MAGIC 5. O commit só acontece se os dados mudaram (compara o SHA-256 de cada conjunto com o manifesto publicado).
# MAGIC
# MAGIC Widgets: `inicio_execucao` (preenchido pelo job; vazio numa execução manual = vale a qualidade mais recente), `fontes` (lista; tire uma fonte que ainda não rodou, como o INMET antes do backfill), `github_publicar` (`sim`/`nao`), `github_repo`, `github_branch` e `unidade_nivel_origem` (unidade da cota do SIBH, `cm` por padrão: o export converte para metros).
# MAGIC
# MAGIC Segredo: scope `github`, chave `token` (token do GitHub com *Contents: read and write* só neste repositório).

# COMMAND ----------

dbutils.widgets.text("inicio_execucao", "", "Início do job (ISO 8601); vazio = última qualidade")
dbutils.widgets.text("fontes", "geosampa,cemaden,sibh,inmet,sgb,ibge,osm,hidrografia", "Fontes a exportar")
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
from common import geo, github_publish  # noqa: E402
from common.publicacao_web import (  # noqa: E402
    DIA_INICIAL, DIAS_NIVEL, FATOR_PARA_METRO, IndiceLinhas, IndicePoligonos, area_uniao_por_grau, cod_para_data, data_para_cod, hora_sp, iso_sp, json_bytes,
    linhas_web, pior_qualidade, poligonos_web, serie_diaria, serie_horaria, sha256_dados,
)

spark.conf.set("spark.sql.session.timeZone", "UTC")  # executado_em e inicio_execucao comparados em UTC

CATALOG = "fiap_analytics"
VOLUME = f"{CATALOG}.gold.chuvasegura_web"
VOLUME_PATH = f"/Volumes/{CATALOG}/gold/chuvasegura_web"
DIM_TEMPO = f"{CATALOG}.gold.dim_tempo"
PASTA_SITE = "app/data/gold"  # destino no repositório
VERSAO = "1.4.0"

AGORA_DT = datetime.now(ZoneInfo("America/Sao_Paulo"))
AGORA = AGORA_DT.isoformat(timespec="seconds")
INICIO_EXECUCAO = dbutils.widgets.get("inicio_execucao").strip()
FONTES = {f.strip().lower() for f in dbutils.widgets.get("fontes").split(",") if f.strip()}
DESCONHECIDAS = FONTES - {"geosampa", "cemaden", "sibh", "inmet", "sgb", "ibge", "osm", "hidrografia"}
if DESCONHECIDAS:
    raise ValueError(f"Fontes desconhecidas em 'fontes': {sorted(DESCONHECIDAS)}")
GITHUB_PUBLICAR = dbutils.widgets.get("github_publicar") == "sim"
GITHUB_REPO = dbutils.widgets.get("github_repo").strip()
GITHUB_BRANCH = dbutils.widgets.get("github_branch").strip()
UNIDADE_NIVEL_ORIGEM = dbutils.widgets.get("unidade_nivel_origem")
FATOR_NIVEL = FATOR_PARA_METRO[UNIDADE_NIVEL_ORIGEM]
ENVELOPE = (-24.1, -23.3, -47.0, -46.3)  # lat mín, lat máx, lon mín, lon máx (município e entorno)
# Simplificação das geometrias (Douglas-Peucker, em metros): abaixo de ~1 pixel no zoom em que cada camada é lida.
TOLERANCIA_SUSC_M = 5
TOLERANCIA_LIMITE_M = 15
TOLERANCIA_AGUA_M = 10
DIST_AGUA_MAX_M = 500  # além disso, a distância da ocorrência ao curso d'água fica nula
TOLERANCIA_SETOR_M = 8
TOLERANCIA_VIA_M = 8
HIERARQUIA_VIAS_MAX = 5  # vias expressas a coletoras e seus acessos; ruas locais, serviço e trilhas ficam de fora

print(f"Publicação {VERSAO} em {AGORA} | fontes: {sorted(FONTES)} | início do job: {INICIO_EXECUCAO or '(execução manual)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trava de qualidade por fonte

# COMMAND ----------

_qualidade_cache = {}


class FonteIndisponivel(RuntimeError):
    """A fonte não ficou pronta nesta execução (carga ou qualidade Gold falhou antes do export)."""


def qualidade_gold(fonte):
    """Resultado da execução de qualidade Gold mais recente da fonte. Falha se ela teve ERRO ou, dentro do job,
    se não rodou nesta execução (a carga da fonte falhou antes)."""
    if fonte in _qualidade_cache:
        return _qualidade_cache[fonte]
    tabela = f"{CATALOG}.bronze.{fonte}_data_quality_log"
    if not spark.catalog.tableExists(tabela):
        raise FonteIndisponivel(f"Tabela de qualidade não encontrada: {tabela}")
    dq = spark.table(tabela).where((F.col("etapa") == "gold") & (F.col("fonte") == fonte))
    ultima = dq.orderBy(F.col("executado_em").desc()).select("batch_id", "executado_em").first()
    if not ultima:
        raise FonteIndisponivel(f"Não há execução de qualidade Gold de {fonte}.")
    if INICIO_EXECUCAO:
        inicio = spark.range(1).select(F.lit(INICIO_EXECUCAO).cast("timestamp").alias("t")).first()["t"]
        if inicio is None:
            raise ValueError(f"inicio_execucao inválido: {INICIO_EXECUCAO!r}")
        if ultima["executado_em"] < inicio:
            raise FonteIndisponivel(
                f"A qualidade Gold de {fonte} não rodou nesta execução (última em {ultima['executado_em']}): a carga falhou antes.")
    run = dq.where(F.col("batch_id") == ultima["batch_id"])
    erros = run.where((F.col("severidade") == "ERRO") & (F.col("status") == "FALHA")).count()
    avisos = run.where((F.col("severidade") == "AVISO") & (F.col("status") == "FALHA")).count()
    if erros:
        raise FonteIndisponivel(f"A qualidade Gold de {fonte} tem {erros} regra(s) ERRO em falha.")
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
        "meta": meta,  # cópia no manifesto: as telas de metadados não precisam baixar os arquivos grandes
    }
    for k in ("referencia", "registros_origem", "registros_descartados"):
        if k in meta:
            entrada[k] = meta[k]
    return corpo, entrada

# COMMAND ----------

# MAGIC %md
# MAGIC ## SGB: suscetibilidade a inundação
# MAGIC
# MAGIC Roda antes do GeoSampa, porque as ocorrências recebem o grau da área de inundação que as contém (o maior, se houver mais de uma). As áreas por grau são a união dos polígonos (a camada de inundação tem sobreposições), e a área do município é a união da camada de movimentos de massa, que cobre todo o território da carta.

# COMMAND ----------

SGB = {"areas": f"{CATALOG}.gold.dim_sgb_susceptibility_areas", "classes": f"{CATALOG}.gold.dim_sgb_susceptibility_class"}


def poligonos_da_camada(camada):
    """[(linha, poligonos)] das áreas graduadas de uma camada, com a geometria completa (para cruzar e medir)."""
    linhas = (
        spark.table(SGB["areas"]).alias("a")
        .join(spark.table(SGB["classes"]).alias("c"), F.col("a.COD_DIM_CLASSE") == F.col("c.COD_DIM_CLASSE"), "left")
        .where((F.col("a.COD_CAMADA") == camada) & (F.col("a.COD_DIM_AREA_SUSCETIBILIDADE") != -1) & F.col("a.GEO_WKB").isNotNull())
        .select(F.col("a.COD_DIM_AREA_SUSCETIBILIDADE").alias("id"), F.col("a.NUM_GRAU_SUSCETIBILIDADE").alias("grau"),
                F.col("c.DSC_CLASSE").alias("classe"), F.col("a.NUM_AREA_KM2").alias("area_km2"), F.col("a.GEO_WKB").alias("wkb"),
                F.col("a.DSC_VERSAO_FONTE").alias("versao"))
        .orderBy("id")
    )
    out = []
    for r in linhas.toLocalIterator():
        g = geo.de_wkb(r["wkb"])
        pols = [p for p in geo.poligonos(g) if p] if g else []
        if pols:
            out.append((r, g, pols))
    return out


def exportar_sgb():
    qualidade = qualidade_gold("sgb")
    fonte = {"id": "sgb", "nome": "SGB/CPRM e IPT — Carta de suscetibilidade (2015)", "url": "https://rigeo.sgb.gov.br/handle/doc/15216"}
    classes = [
        {"grau": r["grau"], "classe": r["classe"], "camada": "inundacao", "caracteristicas": r["car"], "declividade": r["decl"],
         "area_quadro_sintese_km2": r["quadro"]}
        for r in spark.table(SGB["classes"])
        .where((F.col("COD_CAMADA") == "inundacao") & F.col("NUM_GRAU_SUSCETIBILIDADE").isNotNull())
        .select(F.col("NUM_GRAU_SUSCETIBILIDADE").alias("grau"), F.col("DSC_CLASSE").alias("classe"),
                F.col("DSC_CARACTERISTICAS").alias("car"), F.col("DSC_DECLIVIDADE").alias("decl"),
                F.col("NUM_AREA_QUADRO_SINTESE_KM2").alias("quadro"))
        .orderBy(F.col("grau").desc()).collect()
    ]
    if {c["grau"] for c in classes} != {1, 2, 3}:
        raise RuntimeError(f"Classes de inundação incompletas na Gold do SGB: {[c['grau'] for c in classes]}")

    areas, itens, versoes = [], [], set()
    for r, g, pols in poligonos_da_camada("inundacao"):
        if r["grau"] not in (1, 2, 3):
            continue
        web = poligonos_web(g, TOLERANCIA_SUSC_M)
        if not web:
            continue
        areas.append({"id": int(r["id"]), "camada": "inundacao", "grau": int(r["grau"]), "classe": r["classe"],
                      "area_km2": round(r["area_km2"], 4), "geometria": web})
        itens.append((int(r["grau"]), pols))
        versoes.add(r["versao"])
    if not areas:
        raise RuntimeError("A Gold do SGB não tem áreas de inundação com geometria.")

    uniao = area_uniao_por_grau(itens)
    municipio = sum(area_uniao_por_grau([(1, pols) for _, _, pols in poligonos_da_camada("movimento_massa")]).values())
    if not 1400 <= municipio <= 1650:
        raise RuntimeError(f"Área do município pela carta fora do esperado: {municipio:.1f} km² (cerca de 1.521 km²).")

    meta = dataset_meta(
        "suscetibilidade", "Suscetibilidade a inundações", "derivado", fonte, "Dados públicos do Serviço Geológico do Brasil.",
        [SGB["areas"], SGB["classes"]], len(areas), qualidade, data_referencia="2015",
        granularidade={"espacial": "polígono, escala 1:75.000", "temporal": "estático (carta de 2015)"},
        extra={"unidade": "classe (Alta, Média, Baixa)", "area_municipio_km2": round(municipio, 2),
               "area_uniao_km2": {str(k): round(uniao.get(k, 0.0), 2) for k in (3, 2, 1)},
               "simplificacao_m": TOLERANCIA_SUSC_M, "versao_fonte": ", ".join(sorted(v for v in versoes if v)),
               "observacao": "Suscetibilidade não é previsão: indica a predisposição do terreno, sem considerar a chuva. "
                             "Áreas por classe sem contar sobreposições (cada ponto no maior grau); polígonos simplificados para o mapa."},
    )
    corpo, entrada = pacote("suscetibilidade.json", meta, {"classes": classes, "areas": areas})
    print(f"SGB: {len(areas)} áreas de inundação ({len(corpo) / 1024:.0f} KB); união por grau {meta['area_uniao_km2']} km²; "
          f"município {municipio:.1f} km²; qualidade {qualidade['estado']}")
    return {
        "arquivos": {"suscetibilidade.json": corpo}, "datasets": [entrada],
        "linhagem": [{"fonte": "SGB", "tabelas": ["bronze.sgb_susceptibility_areas", "silver.sgb_susceptibility_areas", "gold.dim_sgb_susceptibility_areas"],
                      "publicado_na_web": True, "processado_em": AGORA, "qualidade": qualidade["estado"], "avisos": qualidade["avisos"]}],
        "indice": IndicePoligonos(itens),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## GeoSampa: ocorrências e subprefeituras

# COMMAND ----------


def exportar_hidrografia():
    """hidrografia.json (cursos e corpos d'água do OSM) e a função de distância usada nas ocorrências."""
    qualidade = qualidade_gold("osm_agua")
    fonte = {"id": "osm", "nome": "OpenStreetMap (Geofabrik)", "url": "https://www.openstreetmap.org/copyright"}
    dims = {"cursos": f"{CATALOG}.gold.dim_osm_waterways", "corpos": f"{CATALOG}.gold.dim_osm_water_areas"}
    dados, linhas_dist, poligonos_agua = {}, [], []
    for chave, tabela in dims.items():
        medida = "NUM_COMPRIMENTO_KM" if chave == "cursos" else "NUM_AREA_KM2"
        col = {k: [] for k in ("id", "nome", "classe", "km" if chave == "cursos" else "area_km2", "geometria")}
        for r in spark.table(tabela).orderBy("NUM_OSM_ID").toLocalIterator():
            g = geo.de_wkb(r["GEO_WKB"])
            if not g:
                continue
            web = linhas_web(g, TOLERANCIA_AGUA_M) if chave == "cursos" else poligonos_web(g, TOLERANCIA_AGUA_M)
            if not web:
                continue
            col["id"].append(r["NUM_OSM_ID"])
            col["nome"].append(r["NOM_HIDRO"])
            col["classe"].append(r["DSC_CLASSE"])
            col["km" if chave == "cursos" else "area_km2"].append(None if r[medida] is None else round(r[medida], 3))
            col["geometria"].append(web)
            if chave == "cursos":
                linhas_dist += geo.linhas(g)
            else:
                pols = [pol for pol in geo.poligonos(g) if pol]
                poligonos_agua.append((1, pols))
                linhas_dist += [anel for pol in pols for anel in pol]
        if not col["id"]:
            raise RuntimeError(f"{tabela} não tem feições com geometria.")
        dados[chave] = col
    indice_linhas = IndiceLinhas(linhas_dist)
    indice_corpos = IndicePoligonos(poligonos_agua)

    def distancia(lon, lat):
        if indice_corpos.maior_valor(lon, lat):
            return 0
        d = indice_linhas.distancia(lon, lat, DIST_AGUA_MAX_M)
        return None if d is None else round(d)

    km = sum(v or 0 for v in dados["cursos"]["km"])
    meta = dataset_meta(
        "hidrografia", "Hidrografia (OpenStreetMap)", "colaborativo", fonte,
        "© colaboradores do OpenStreetMap, ODbL 1.0: exige atribuição; base derivada distribuída fica sob a ODbL.",
        list(dims.values()), len(dados["cursos"]["id"]) + len(dados["corpos"]["id"]), qualidade,
        granularidade={"espacial": "curso e corpo d'água (way do OSM)", "temporal": "extrato da Geofabrik"},
        extra={"km_cursos": round(km, 1), "simplificacao_m": TOLERANCIA_AGUA_M,
               "observacao": "Em São Paulo muitos córregos correm canalizados ou sob avenidas, e o OpenStreetMap os mapeia de forma "
                             "desigual: a ausência de um curso d'água no mapa não quer dizer que ele não exista."},
    )
    corpo, entrada = pacote("hidrografia.json", meta, dados)
    print(f"Hidrografia: {len(dados['cursos']['id'])} cursos ({km:.0f} km) e {len(dados['corpos']['id'])} corpos d'água "
          f"({len(corpo) / 1024 / 1024:.1f} MB); qualidade {qualidade['estado']}")
    return {
        "arquivos": {"hidrografia.json": corpo}, "datasets": [entrada],
        "linhagem": [{"fonte": "OpenStreetMap (hidrografia)", "tabelas": ["bronze.osm_waterways", "bronze.osm_water_areas", "gold.dim_osm_waterways", "gold.dim_osm_water_areas"],
                      "publicado_na_web": True, "processado_em": AGORA, "qualidade": qualidade["estado"], "avisos": qualidade["avisos"]}],
        "distancia": distancia,
    }


LIMITES = f"{CATALOG}.gold.dim_geosampa_subprefecture_limits"
INDICE_SUBPREF = None  # preenchido pelo export do GeoSampa; usado para dar subprefeitura às estações
MOTIVO_SEM_LIMITES = None


def exportar_limites(fonte, licenca, qualidade):
    """limites.json (polígonos das subprefeituras) e o índice espacial para as estações, se a Gold tiver os limites."""
    global INDICE_SUBPREF, MOTIVO_SEM_LIMITES
    if not spark.catalog.tableExists(LIMITES):
        MOTIVO_SEM_LIMITES = ("Os limites ainda não foram carregados: envie o shapefile das subprefeituras do GeoSampa ao Volume "
                              "fiap_analytics.bronze.geosampa_subprefecture_limits (o download do GeoSampa exige CAPTCHA).")
        return None
    partes = {}
    for r in spark.table(LIMITES).orderBy("SIG_SUBPREFEITURA", "NUM_ID_FEICAO").toLocalIterator():
        g = geo.de_wkb(r["GEO_WKB"])
        if not g:
            continue
        p = partes.setdefault(r["SIG_SUBPREFEITURA"], {"nome": r["NOM_SUBPREFEITURA"], "pols": [], "area": 0.0, "versao": r["DSC_VERSAO_FONTE"]})
        p["pols"] += [pol for pol in geo.poligonos(g) if pol]
        p["area"] += r["NUM_AREA_KM2"] or 0
    if not partes:
        raise RuntimeError(f"{LIMITES} existe, mas está vazia.")
    INDICE_SUBPREF = IndicePoligonos([(sigla, p["pols"]) for sigla, p in partes.items()])
    itens = []
    for sigla, p in sorted(partes.items(), key=lambda kv: kv[1]["nome"]):
        pts = [pt for pol in p["pols"] for pt in pol[0]]
        itens.append({"sigla": sigla, "nome": p["nome"], "area_km2": round(p["area"], 2),
                      "envelope": [round(min(x for x, _ in pts), 5), round(min(y for _, y in pts), 5), round(max(x for x, _ in pts), 5), round(max(y for _, y in pts), 5)],
                      "geometria": poligonos_web(("MultiPolygon", p["pols"]), TOLERANCIA_LIMITE_M)})
    meta = dataset_meta("limites", "Limites das subprefeituras", "oficial", fonte, licenca, [LIMITES], len(itens), qualidade,
                        data_referencia=next(iter(partes.values()))["versao"], granularidade={"espacial": "subprefeitura", "temporal": "cadastro"},
                        extra={"simplificacao_m": TOLERANCIA_LIMITE_M, "area_total_km2": round(sum(i["area_km2"] for i in itens), 1)})
    corpo, entrada = pacote("limites.json", meta, {"subprefeituras": itens})
    print(f"Limites: {len(itens)} subprefeituras ({len(corpo) / 1024:.0f} KB)")
    return corpo, entrada


def exportar_geosampa(indice_susc=None, dist_agua=None):
    """indice_susc: áreas de inundação do SGB (IndicePoligonos), ou None se o SGB não foi exportado.
    dist_agua: função (lon, lat) -> metros até o curso ou corpo d'água mais próximo, ou None se a hidrografia não foi exportada."""
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
    # Grau da área de inundação que contém o ponto (0 = fora das áreas mapeadas); nulo = cruzamento não feito.
    colunas["grau_susc_inundacao"] = (
        [indice_susc.maior_valor(lon, lat) for lon, lat in zip(colunas["lon"], colunas["lat"])] if indice_susc
        else [None] * len(colunas["id"]))
    colunas["dist_agua_m"] = ([dist_agua(lon, lat) for lon, lat in zip(colunas["lon"], colunas["lat"])] if dist_agua
                              else [None] * len(colunas["id"]))
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
               "observacao": "A Gold do GeoSampa não informa hora do evento. " + (
                   "grau_susc_inundacao: maior grau das áreas de inundação do SGB que contêm o ponto (0 = fora delas)." if indice_susc
                   else "grau_susc_inundacao fica nulo: o cruzamento com a carta do SGB não foi feito nesta publicação.") + (
                   f" dist_agua_m: metros até o curso ou corpo d'água do OpenStreetMap mais próximo (0 = dentro de um corpo d'água; nula acima de {DIST_AGUA_MAX_M} m)."
                   if dist_agua else " dist_agua_m fica nula: a hidrografia não foi exportada nesta publicação."),
               "dist_agua_max_m": DIST_AGUA_MAX_M if dist_agua else None},
    )
    arq_sp, ent_sp = pacote("subprefeituras.json", meta_sp, subprefeituras)
    arq_oc, ent_oc = pacote("ocorrencias.json", meta_oc, colunas)
    arquivos_gs = {"subprefeituras.json": arq_sp, "ocorrencias.json": arq_oc}
    datasets_gs = [ent_sp, ent_oc]
    limites = tentar("limites", lambda: exportar_limites(fonte, licenca, qualidade))  # falha nos limites não derruba as ocorrências
    if limites:
        arquivos_gs["limites.json"] = limites[0]
        datasets_gs.append(limites[1])
    print(f"GeoSampa: {len(colunas['id'])} ocorrências de {total_fato} ({descartadas} sem data ou coordenada válida); "
          f"{len(subprefeituras)} subprefeituras; qualidade {qualidade['estado']}")
    return {
        "arquivos": arquivos_gs,
        "datasets": datasets_gs,
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
    if INDICE_SUBPREF:
        for s in chuva + nivel:
            s["subprefeitura"] = INDICE_SUBPREF.maior_valor(s["lon"], s["lat"], padrao=None)
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
# MAGIC ## IBGE: setores censitários com população
# MAGIC
# MAGIC Uma feição por setor do Censo 2022, com pessoas, domicílios e densidade. Dado agregado por setor: não identifica pessoas.

# COMMAND ----------


def exportar_ibge():
    qualidade = qualidade_gold("ibge")
    dim = f"{CATALOG}.gold.dim_ibge_census_tracts"
    fato = f"{CATALOG}.gold.fact_ibge_census_tract_population"
    fonte = {"id": "ibge", "nome": "IBGE — Censo 2022", "url": "https://www.ibge.gov.br/"}
    ref = spark.table(fato).where(F.col("COD_DIM_SETOR") != -1).agg(F.max("COD_DIM_TEMPO")).first()[0]
    if ref is None:
        raise RuntimeError("A fato de população do IBGE está vazia.")
    linhas = (
        spark.table(dim).alias("d")
        .join(spark.table(fato).where(F.col("COD_DIM_TEMPO") == ref).alias("f"), F.col("d.COD_DIM_SETOR") == F.col("f.COD_DIM_SETOR"), "left")
        .where((F.col("d.COD_DIM_SETOR") != -1) & F.col("d.GEO_WKB").isNotNull())
        .select(F.col("d.COD_SETOR").alias("id"), F.col("d.NOM_DISTRITO").alias("distrito"), F.col("d.DSC_SITUACAO").alias("situacao"),
                F.col("d.FLG_FAVELA").alias("favela"), F.col("f.NUM_PESSOAS").alias("pessoas"), F.col("f.NUM_DOMICILIOS").alias("domicilios"),
                F.col("d.NUM_AREA_KM2").alias("area_km2"), F.col("f.NUM_DENSIDADE_HAB_KM2").alias("densidade"), F.col("d.GEO_WKB").alias("wkb"))
        .orderBy("id")
    )
    nomes = ("id", "distrito", "situacao", "favela", "pessoas", "domicilios", "area_km2", "densidade", "geometria")
    col = {k: [] for k in nomes}
    sem_geometria = 0
    for r in linhas.toLocalIterator():
        g = geo.de_wkb(r["wkb"])
        web = poligonos_web(g, TOLERANCIA_SETOR_M) if g else []
        if not web:
            sem_geometria += 1
            continue
        col["id"].append(r["id"])
        col["distrito"].append(r["distrito"])
        col["situacao"].append(r["situacao"])
        col["favela"].append(bool(r["favela"]))
        col["pessoas"].append(r["pessoas"])
        col["domicilios"].append(r["domicilios"])
        col["area_km2"].append(None if r["area_km2"] is None else round(r["area_km2"], 4))
        col["densidade"].append(None if r["densidade"] is None else round(r["densidade"]))
        col["geometria"].append(web)
    if not col["id"]:
        raise RuntimeError("A Gold do IBGE não gerou setores com geometria.")
    pessoas = sum(v or 0 for v in col["pessoas"])
    data_ref = cod_para_data(ref).isoformat()
    meta = dataset_meta(
        "setores", "Setores censitários (IBGE)", "oficial", fonte, "Dados públicos do IBGE.", [dim, fato], len(col["id"]), qualidade,
        data_referencia=data_ref, granularidade={"espacial": "setor censitário", "temporal": f"Censo 2022 (referência {data_ref})"},
        extra={"unidade": "pessoas e domicílios; densidade em hab/km²", "pessoas_total": pessoas,
               "domicilios_total": sum(v or 0 for v in col["domicilios"]), "registros_descartados": sem_geometria,
               "simplificacao_m": TOLERANCIA_SETOR_M,
               "observacao": "Dados agregados por setor censitário: não identificam pessoas nem permitem inferir exposição individual."},
    )
    corpo, entrada = pacote("setores.json", meta, col)
    print(f"IBGE: {len(col['id'])} setores ({sem_geometria} sem geometria), {pessoas} pessoas; {len(corpo) / 1024 / 1024:.1f} MB; "
          f"qualidade {qualidade['estado']}")
    return {
        "arquivos": {"setores.json": corpo}, "datasets": [entrada],
        "linhagem": [{"fonte": "IBGE", "tabelas": ["bronze.ibge_census_tracts", "silver.ibge_census_tracts", "gold.fact_ibge_census_tract_population"],
                      "publicado_na_web": True, "processado_em": AGORA, "qualidade": qualidade["estado"], "avisos": qualidade["avisos"]}],
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## OpenStreetMap: vias principais
# MAGIC
# MAGIC Só as vias de hierarquia 1 a 5 (expressas, arteriais, coletoras e seus acessos). Ruas locais, vias de serviço, caminhos de pedestres e trilhas ficam de fora: são a maior parte das centenas de milhares de vias e não cabem num arquivo estático leve. Dados © colaboradores do OpenStreetMap, ODbL 1.0.

# COMMAND ----------


def exportar_osm():
    qualidade = qualidade_gold("osm")
    dim = f"{CATALOG}.gold.dim_osm_roads"
    dim_classe = f"{CATALOG}.gold.dim_osm_road_class"
    fonte = {"id": "osm", "nome": "OpenStreetMap (Geofabrik)", "url": "https://www.openstreetmap.org/copyright"}
    base = (spark.table(dim).alias("v")
            .join(spark.table(dim_classe).alias("c"), F.col("v.COD_DIM_CLASSE_VIA") == F.col("c.COD_DIM_CLASSE_VIA"), "left")
            .where(F.col("v.COD_DIM_VIA") != -1))
    total = base.agg(F.count("*").alias("n"), F.sum("v.NUM_COMPRIMENTO_KM").alias("km")).first()
    linhas = (
        base.where(F.col("c.NUM_HIERARQUIA").between(1, HIERARQUIA_VIAS_MAX) & F.col("v.GEO_WKB").isNotNull())
        .select(F.col("v.NUM_OSM_ID").alias("id"), F.col("v.NOM_VIA").alias("nome"), F.col("v.COD_REFERENCIA").alias("ref"),
                F.col("c.DSC_CLASSE").alias("classe"), F.col("c.NUM_HIERARQUIA").alias("hierarquia"), F.col("v.FLG_PONTE").alias("ponte"),
                F.col("v.FLG_TUNEL").alias("tunel"), F.col("v.NUM_COMPRIMENTO_KM").alias("km"), F.col("v.GEO_WKB").alias("wkb"))
        .orderBy("id")
    )
    nomes = ("id", "nome", "ref", "classe", "hierarquia", "ponte", "tunel", "km", "geometria")
    col = {k: [] for k in nomes}
    for r in linhas.toLocalIterator():
        g = geo.de_wkb(r["wkb"])
        web = linhas_web(g, TOLERANCIA_VIA_M) if g else []
        if not web:
            continue
        col["id"].append(r["id"])
        col["nome"].append(r["nome"])
        col["ref"].append(r["ref"])
        col["classe"].append(r["classe"])
        col["hierarquia"].append(r["hierarquia"])
        col["ponte"].append(bool(r["ponte"]))
        col["tunel"].append(bool(r["tunel"]))
        col["km"].append(None if r["km"] is None else round(r["km"], 3))
        col["geometria"].append(web)
    if not col["id"]:
        raise RuntimeError("A Gold do OSM não gerou vias principais com geometria.")
    km = sum(v or 0 for v in col["km"])
    meta = dataset_meta(
        "vias", "Rede viária principal (OpenStreetMap)", "colaborativo", fonte,
        "© colaboradores do OpenStreetMap, ODbL 1.0: exige atribuição; base derivada distribuída fica sob a ODbL.",
        [dim, dim_classe], len(col["id"]), qualidade,
        granularidade={"espacial": "trecho de via (way do OSM)", "temporal": "extrato da Geofabrik"},
        extra={"unidade": "km", "km_publicados": round(km, 1), "vias_total": total["n"], "km_total": round(total["km"] or 0, 1),
               "hierarquia_max": HIERARQUIA_VIAS_MAX, "simplificacao_m": TOLERANCIA_VIA_M,
               "observacao": f"Só vias de hierarquia 1 a {HIERARQUIA_VIAS_MAX} (expressas, arteriais, coletoras e acessos): "
                             f"{len(col['id'])} de {total['n']} vias, {km:.0f} de {total['km'] or 0:.0f} km. Sem roteamento nem trânsito."},
    )
    corpo, entrada = pacote("vias.json", meta, col)
    print(f"OSM: {len(col['id'])} vias principais de {total['n']} ({len(corpo) / 1024 / 1024:.1f} MB); qualidade {qualidade['estado']}")
    return {
        "arquivos": {"vias.json": corpo}, "datasets": [entrada],
        "linhagem": [{"fonte": "OpenStreetMap", "tabelas": ["bronze.osm_roads", "silver.osm_roads", "gold.dim_osm_roads"],
                      "publicado_na_web": True, "processado_em": AGORA, "qualidade": qualidade["estado"], "avisos": qualidade["avisos"]}],
    }

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
        falhas.append((nome, f"{type(exc).__name__}: {exc}", isinstance(exc, FonteIndisponivel)))
        print(f"FALHA em {nome}: {exc}")
        return None


def incluir(r, ids):
    global datasets, linhagem
    arquivos.update(r["arquivos"])
    datasets += r["datasets"]
    linhagem += r["linhagem"]
    publicados.update(ids)


indice_susc = None
if "sgb" in FONTES:
    r = tentar("sgb", exportar_sgb)
    if r:
        incluir(r, {"suscetibilidade"})
        indice_susc = r["indice"]

dist_agua = None
if "hidrografia" in FONTES:
    r = tentar("hidrografia", exportar_hidrografia)
    if r:
        incluir(r, {"hidrografia"})
        dist_agua = r["distancia"]

if "geosampa" in FONTES:
    r = tentar("geosampa", lambda: exportar_geosampa(indice_susc, dist_agua))
    if r:
        arquivos.update(r["arquivos"])
        datasets += r["datasets"]
        linhagem += r["linhagem"]
        referencias.append(r["referencia"])
        publicados |= {d["id"] for d in r["datasets"]}

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
        extra={"observacao": "Subprefeitura de cada estação pela localização dentro dos limites do GeoSampa (nula fora do município)." if INDICE_SUBPREF
               else "Sem os limites das subprefeituras, as estações ficam sem subprefeitura e o filtro por subprefeitura não as inclui."})
    corpo, entrada = pacote("estacoes.json", meta_est, {"chuva": chuva_res["chuva"], "nivel": sensores})
    arquivos["estacoes.json"] = corpo
    datasets.append(entrada)
    publicados.add("estacoes")

for fonte_geo, exportador, conjunto in (("ibge", exportar_ibge, "setores"), ("osm", exportar_osm, "vias")):
    if fonte_geo in FONTES:
        r = tentar(fonte_geo, exportador)
        if r:
            incluir(r, {conjunto})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manifesto

# COMMAND ----------

MOTIVO_FALHA = {n: m for n, m, _ in falhas}
FALHAS_NO_EXPORT = [(n, m) for n, m, a_montante in falhas if not a_montante]

# Conjunto de uma fonte que falhou fica com a versão já publicada: entra no manifesto novo e o arquivo não é reenviado.
FONTES_DO_CONJUNTO = {
    "subprefeituras": ("geosampa",), "ocorrencias": ("geosampa",), "limites": ("geosampa", "limites"),
    "chuva_diaria": ("chuva", "cemaden", "sibh", "inmet"), "chuva_horaria": ("chuva", "cemaden", "sibh", "inmet"),
    "estacoes": ("chuva", "cemaden", "sibh", "inmet"), "nivel": ("nivel", "sibh", "chuva"),
    "suscetibilidade": ("sgb",), "setores": ("ibge",), "vias": ("osm",), "hidrografia": ("hidrografia",),
}
token = dbutils.secrets.get(scope="github", key="token") if GITHUB_PUBLICAR else None
atual = github_publish.ler_json(token, GITHUB_REPO, GITHUB_BRANCH, f"{PASTA_SITE}/manifest.json") if token else None
mantidos = []
if atual and atual.get("modo") == "gold":
    for d in atual.get("datasets", []):
        if d["id"] not in publicados and any(n in MOTIVO_FALHA for n in FONTES_DO_CONJUNTO.get(d["id"], ())):
            datasets.append({**d, "mantido": True})
            publicados.add(d["id"])
            mantidos.append(d["id"])

if not datasets:
    raise RuntimeError("Nada foi exportado: " + "; ".join(f"{n}: {m}" for n, m, _ in falhas))
CATALOGO_NAO_PUBLICADO = [
    ("chuva", "Chuva observada", "chuva_diaria"), ("nivel", "Nível de rios e córregos", "nivel"),
    ("suscetibilidade", "Suscetibilidade a inundações", "suscetibilidade"), ("avisos_oficiais", "Avisos oficiais", None),
    ("limites", "Limites administrativos", "limites"), ("setores", "Setores censitários (IBGE)", "setores"),
    ("vias", "Rede viária (OpenStreetMap)", "vias"), ("hidrografia", "Hidrografia (OpenStreetMap)", "hidrografia"),
]
MOTIVOS_FIXOS = {
    "avisos_oficiais": "Nenhuma fonte de avisos está integrada ao pipeline.",
}
nao_publicados = []
for id_, titulo, ds in CATALOGO_NAO_PUBLICADO:
    if ds and ds in publicados:
        continue
    if id_ in MOTIVOS_FIXOS:
        motivo = MOTIVOS_FIXOS[id_]
    else:
        fontes_rel = {"chuva": ("chuva", "cemaden", "sibh", "inmet"), "nivel": ("nivel", "sibh"), "suscetibilidade": ("sgb",), "limites": ("geosampa", "limites"),
                      "setores": ("ibge",), "vias": ("osm",), "hidrografia": ("hidrografia",)}[id_]
        erros = [f"{n}: {MOTIVO_FALHA[n]}" for n in fontes_rel if n in MOTIVO_FALHA]
        motivo = (("Falhou nesta execução (" + "; ".join(erros) + ").") if erros
                  else MOTIVO_SEM_LIMITES if id_ == "limites" and MOTIVO_SEM_LIMITES else "Fonte não incluída nesta publicação.")
    nao_publicados.append({"id": id_, "titulo": titulo, "motivo": motivo})

manifesto = {
    "produto": "ChuvaSegura-SP", "modo": "gold",
    "aviso": "Dados reais publicados a partir das tabelas Gold do Databricks. Somente os conjuntos listados estão disponíveis nesta publicação.",
    "versao": VERSAO, "referencia": max(referencias) if referencias else atual["referencia"], "publicado_em": AGORA,
    "catalogo": CATALOG, "datasets": datasets, "linhagem": linhagem, "nao_publicados": nao_publicados,
    "falhas": [{"fonte": n, "motivo": m} for n, m, _ in falhas],
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
if not GITHUB_PUBLICAR:
    print("github_publicar = nao: pacote só no Volume.")
else:
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

for n, m, a_montante in falhas:
    print(f"{'Fonte indisponível' if a_montante else 'ERRO NO EXPORT'} — {n}: {m}")
if mantidos:
    print(f"Mantidos no site com a versão anterior: {', '.join(mantidos)}.")
if FALHAS_NO_EXPORT:
    raise RuntimeError("Erro no export: " + " | ".join(f"{n}: {m}" for n, m in FALHAS_NO_EXPORT))
