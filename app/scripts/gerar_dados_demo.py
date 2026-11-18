"""Gera o pacote de dados SINTÉTICOS do modo demonstração do ChuvaSegura-SP.

Os arquivos seguem o mesmo contrato dos exports da Gold (app/DATA_CONTRACT.md),
mas todos os valores são inventados. Cada arquivo traz `natureza = "sintetico"`
e a interface exibe o rótulo de demonstração de forma persistente (F17 do PDR).

Uso:
    python app/scripts/gerar_dados_demo.py                      # referência = hora cheia anterior
    python app/scripts/gerar_dados_demo.py --referencia 2026-11-16T23:00

Saída: app/data/demo/*.json (nunca em app/data/gold/).
"""

from __future__ import annotations

import argparse
import json
import math
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

TZ_SP = timezone(timedelta(hours=-3))  # São Paulo sem horário de verão desde 2019
APP = Path(__file__).resolve().parents[1]  # pasta app/
SAIDA = APP / "data" / "demo"
INICIO = date(2020, 1, 1)
AREA_MUNICIPIO_KM2 = 1521.11  # área oficial do município (IBGE)
VERSAO = "demo-1.0.0"

# Contorno aproximado do município, usado só para sortear pontos (não é publicado).
CONTORNO = [
    (-46.75, -23.357), (-46.68, -23.38), (-46.62, -23.40), (-46.55, -23.46),
    (-46.47, -23.48), (-46.40, -23.47), (-46.365, -23.52), (-46.37, -23.58),
    (-46.43, -23.63), (-46.50, -23.62), (-46.57, -23.63), (-46.63, -23.72),
    (-46.66, -23.80), (-46.67, -23.95), (-46.72, -24.00), (-46.78, -23.98),
    (-46.78, -23.85), (-46.79, -23.75), (-46.81, -23.66), (-46.82, -23.57),
    (-46.80, -23.50), (-46.80, -23.45), (-46.79, -23.39),
]

# Subprefeituras: sigla, nome e um ponto interno aproximado (só para atribuir
# os pontos sintéticos; a Gold real não tem geometria de subprefeitura).
SUBPREFEITURAS = [
    ("PR", "Perus", -23.405, -46.753), ("PJ", "Pirituba/Jaraguá", -23.465, -46.735),
    ("FO", "Freguesia/Brasilândia", -23.465, -46.695), ("CV", "Casa Verde/Cachoeirinha", -23.478, -46.660),
    ("ST", "Santana/Tucuruvi", -23.480, -46.615), ("JT", "Jaçanã/Tremembé", -23.440, -46.590),
    ("MG", "Vila Maria/Vila Guilherme", -23.510, -46.585), ("LA", "Lapa", -23.520, -46.710),
    ("SE", "Sé", -23.550, -46.635), ("BT", "Butantã", -23.585, -46.745),
    ("PI", "Pinheiros", -23.575, -46.690), ("VM", "Vila Mariana", -23.595, -46.635),
    ("IP", "Ipiranga", -23.605, -46.605), ("SA", "Santo Amaro", -23.650, -46.700),
    ("JA", "Jabaquara", -23.645, -46.645), ("AD", "Cidade Ademar", -23.675, -46.655),
    ("CL", "Campo Limpo", -23.640, -46.765), ("MB", "M'Boi Mirim", -23.700, -46.770),
    ("CS", "Capela do Socorro", -23.740, -46.700), ("PA", "Parelheiros", -23.880, -46.720),
    ("PE", "Penha", -23.525, -46.535), ("EM", "Ermelino Matarazzo", -23.495, -46.480),
    ("MP", "São Miguel Paulista", -23.500, -46.445), ("IT", "Itaim Paulista", -23.505, -46.400),
    ("MO", "Mooca", -23.555, -46.585), ("AF", "Aricanduva/Formosa/Carrão", -23.560, -46.525),
    ("IQ", "Itaquera", -23.545, -46.455), ("G", "Guaianases", -23.545, -46.410),
    ("VP", "Vila Prudente", -23.595, -46.570), ("SM", "São Mateus", -23.605, -46.475),
    ("CT", "Cidade Tiradentes", -23.590, -46.405), ("SB", "Sapopemba", -23.605, -46.515),
]

# Traçados aproximados de cursos d'água (lon, lat), usados para desenhar a
# suscetibilidade sintética e posicionar os sensores de nível.
RIOS = {
    "Rio Tietê": [(-46.80, -23.515), (-46.75, -23.52), (-46.71, -23.515), (-46.67, -23.515),
                  (-46.64, -23.515), (-46.61, -23.52), (-46.57, -23.505), (-46.53, -23.50),
                  (-46.49, -23.49), (-46.45, -23.48), (-46.40, -23.475)],
    "Rio Pinheiros": [(-46.745, -23.535), (-46.725, -23.565), (-46.705, -23.59), (-46.695, -23.62),
                      (-46.70, -23.65), (-46.705, -23.68), (-46.69, -23.71)],
    "Rio Tamanduateí": [(-46.625, -23.525), (-46.615, -23.55), (-46.605, -23.575),
                        (-46.59, -23.60), (-46.575, -23.62)],
    "Rio Aricanduva": [(-46.555, -23.515), (-46.53, -23.545), (-46.51, -23.565), (-46.49, -23.585)],
    "Córrego Pirajuçara": [(-46.72, -23.57), (-46.745, -23.595), (-46.77, -23.62)],
    "Córrego Cabuçu de Baixo": [(-46.66, -23.515), (-46.67, -23.48), (-46.68, -23.455)],
    "Córrego M'Boi Mirim": [(-46.735, -23.66), (-46.76, -23.69)],
    "Rio Itaquera": [(-46.47, -23.49), (-46.46, -23.53), (-46.45, -23.56)],
}

CLASSES_SUSC = {
    3: ("Alta", "Planícies aluviais e terraços baixos junto aos canais, declividade abaixo de 2°."),
    2: ("Média", "Terraços e baixas encostas próximas às planícies, declividade baixa."),
    1: ("Baixa", "Áreas mais elevadas em relação às planícies, com menor recorrência de cheias."),
}

TIPOS = [("Alagamento", 0.70), ("Queda de Árvore", 0.15), ("Inundação", 0.10), ("Deslizamento", 0.05)]

KM_LAT = 110.57
KM_LON = 111.32 * math.cos(math.radians(23.6))


def ponto_no_poligono(x: float, y: float, pol: list[tuple[float, float]]) -> bool:
    dentro = False
    j = len(pol) - 1
    for i in range(len(pol)):
        xi, yi = pol[i]
        xj, yj = pol[j]
        if (yi > y) != (yj > y) and x < (xj - xi) * (y - yi) / (yj - yi + 1e-15) + xi:
            dentro = not dentro
        j = i
    return dentro


def dist_km(lat1, lon1, lat2, lon2) -> float:
    return math.hypot((lat1 - lat2) * KM_LAT, (lon1 - lon2) * KM_LON)


def area_km2(pol) -> float:
    s = 0.0
    for i in range(len(pol)):
        x1, y1 = pol[i]
        x2, y2 = pol[(i + 1) % len(pol)]
        s += (x1 * KM_LON) * (y2 * KM_LAT) - (x2 * KM_LON) * (y1 * KM_LAT)
    return abs(s) / 2


def sortear_no_municipio(rng: random.Random) -> tuple[float, float]:
    while True:
        lon = rng.uniform(-46.83, -46.36)
        lat = rng.uniform(-24.01, -23.35)
        if ponto_no_poligono(lon, lat, CONTORNO):
            return lat, lon


def subprefeitura_mais_proxima(lat, lon) -> str:
    return min(SUBPREFEITURAS, key=lambda s: dist_km(lat, lon, s[2], s[3]))[0]


def amostrar_rio(pontos, passo_km=1.2):
    saida = []
    for (x1, y1), (x2, y2) in zip(pontos, pontos[1:]):
        d = dist_km(y1, x1, y2, x2)
        n = max(1, int(d / passo_km))
        for k in range(n):
            t = k / n
            saida.append((x1 + (x2 - x1) * t, y1 + (y2 - y1) * t))
    saida.append(pontos[-1])
    return saida


def bolha(rng, lon, lat, raio_km, vertices=14):
    pol = []
    for k in range(vertices):
        ang = 2 * math.pi * k / vertices
        r = raio_km * rng.uniform(0.75, 1.25)
        pol.append((round(lon + r * math.cos(ang) / KM_LON, 5), round(lat + r * math.sin(ang) / KM_LAT, 5)))
    return pol


def poisson(rng: random.Random, lam: float) -> int:
    if lam > 30:
        return max(0, round(rng.gauss(lam, math.sqrt(lam))))
    limite, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limite:
            return k
        k += 1


def ts(dt: datetime) -> str:
    return dt.astimezone(TZ_SP).isoformat(timespec="seconds")


def meta(id_, titulo, fonte, tabelas, natureza_simulada, ref: datetime, **extra):
    publicado = ref + timedelta(minutes=40)
    m = {
        "id": id_,
        "titulo": titulo,
        "versao": VERSAO,
        "natureza": "sintetico",
        "natureza_representada": natureza_simulada,
        "aviso": "Valor demonstrativo, sem conexão com dados reais.",
        "fonte": fonte,
        "tabelas_origem": tabelas,
        "camada_origem": "gold (simulada)",
        "processado_em": ts(ref + timedelta(minutes=20)),
        "publicado_em": ts(publicado),
        "fuso": "America/Sao_Paulo (UTC−03:00)",
        "qualidade": {"estado": "simulado", "descricao": "Dados sintéticos: as regras de qualidade da Gold não se aplicam."},
    }
    m.update(extra)
    return m


FONTES = {
    "geosampa": {"id": "geosampa", "nome": "GeoSampa — Prefeitura de São Paulo", "url": "https://geosampa.prefeitura.sp.gov.br/"},
    "cemaden": {"id": "cemaden", "nome": "CEMADEN — PED", "url": "https://www.gov.br/cemaden/"},
    "sibh": {"id": "sibh", "nome": "SIBH / SP Águas", "url": "https://sibh.daee.sp.gov.br/"},
    "inmet": {"id": "inmet", "nome": "INMET — BDMEP", "url": "https://portal.inmet.gov.br/"},
    "sgb": {"id": "sgb", "nome": "SGB/CPRM — Carta de suscetibilidade (IPT/CPRM, 2015)", "url": "https://rigeo.sgb.gov.br/handle/doc/15216"},
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--referencia", help="data e hora de referência local (AAAA-MM-DDTHH:MM)")
    ap.add_argument("--semente", type=int, default=20261117)
    args = ap.parse_args()

    if args.referencia:
        ref = datetime.fromisoformat(args.referencia).replace(tzinfo=TZ_SP)
    else:
        ref = datetime.now(TZ_SP).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    rng = random.Random(args.semente)
    SAIDA.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ estações
    estacoes = []
    for n in range(1, 41):
        lat, lon = sortear_no_municipio(rng)
        sp = subprefeitura_mais_proxima(lat, lon)
        nome_sp = next(s[1] for s in SUBPREFEITURAS if s[0] == sp)
        estacoes.append({
            "id": f"cemaden:3550308{n:02d}A", "codigo": f"3550308{n:02d}A", "rede": "CEMADEN",
            "nome": f"{nome_sp} — pluviômetro {n:02d}", "grandeza": "Chuva", "instituicao": "CEMADEN",
            "lat": round(lat, 5), "lon": round(lon, 5), "subprefeitura": sp,
            "ativa": rng.random() > 0.07, "tabela": "dim_cemaden_stations",
        })
    for n in range(1, 21):
        lat, lon = sortear_no_municipio(rng)
        sp = subprefeitura_mais_proxima(lat, lon)
        nome_sp = next(s[1] for s in SUBPREFEITURAS if s[0] == sp)
        inst = rng.choice(["SAISP", "SP Águas", "SABESP"])
        estacoes.append({
            "id": f"sibh:{1200 + n}", "codigo": f"{1200 + n}", "rede": "SIBH",
            "nome": f"{nome_sp} — posto pluviométrico {n:02d}", "grandeza": "Chuva", "instituicao": inst,
            "lat": round(lat, 5), "lon": round(lon, 5), "subprefeitura": sp,
            "ativa": rng.random() > 0.1, "tabela": "dim_sibh_sensors",
        })
    for cod, nome, lat, lon in [("A701", "São Paulo — Mirante", -23.4963, -46.6200),
                                ("A771", "São Paulo — Interlagos", -23.7244, -46.6775)]:
        estacoes.append({
            "id": f"inmet:{cod}", "codigo": cod, "rede": "INMET", "nome": nome, "grandeza": "Chuva",
            "instituicao": "INMET", "lat": lat, "lon": lon,
            "subprefeitura": subprefeitura_mais_proxima(lat, lon), "ativa": True, "tabela": "dim_inmet_stations",
        })

    sensores_nivel = []
    n = 0
    for rio, pts in RIOS.items():
        amostra = amostrar_rio(pts, passo_km=6.0)
        for lon, lat in amostra[:: max(1, len(amostra) // 3)][:3]:
            n += 1
            sp = subprefeitura_mais_proxima(lat, lon)
            sensores_nivel.append({
                "id": f"sibh:{2300 + n}", "codigo": f"{2300 + n}", "rede": "SIBH", "grandeza": "Nível",
                "nome": f"{rio} — posto fluviométrico {n:02d}", "curso_dagua": rio,
                "instituicao": rng.choice(["SAISP", "SP Águas"]), "lat": round(lat, 5), "lon": round(lon, 5),
                "subprefeitura": sp, "ativa": rng.random() > 0.08, "unidade": "m",
                "nivel_base": round(rng.uniform(0.8, 2.8), 2), "tabela": "dim_sibh_sensors",
            })

    # ------------------------------------------------------- chuva diária e horária
    dias = (ref.date() - INICIO).days + 1
    datas = [INICIO + timedelta(days=i) for i in range(dias)]
    chuvoso = lambda d: d.month in (10, 11, 12, 1, 2, 3)
    eventos = []  # (dia, base_mm, centro_lat, centro_lon, intensidade_mm, raio_km)
    for d in datas:
        p = 0.52 if chuvoso(d) else 0.2
        if rng.random() < p:
            base = rng.gammavariate(0.9, 7.0 if chuvoso(d) else 4.0)
            clat, clon = sortear_no_municipio(rng)
            intens = rng.gammavariate(1.2, 14.0) if chuvoso(d) and rng.random() < 0.55 else 0.0
            eventos.append((base, clat, clon, intens, rng.uniform(4, 12)))
        else:
            eventos.append(None)

    def chuva_no_ponto(i, lat, lon, ruido=True):
        ev = eventos[i]
        if ev is None:
            return 0.0
        base, clat, clon, intens, raio = ev
        d = dist_km(lat, lon, clat, clon)
        v = base * (rng.uniform(0.55, 1.45) if ruido else 1.0) + intens * math.exp(-(d * d) / (2 * raio * raio))
        return v if v >= 0.2 else 0.0

    serie_diaria = {}
    for e in estacoes:
        valores = []
        # algumas estações começam depois de 2020 e algumas têm falhas
        inicio_op = rng.choice([0] * 8 + [rng.randint(100, 900)])
        for i in range(dias):
            if i < inicio_op or (rng.random() < 0.012):
                valores.append(None)
            else:
                valores.append(round(chuva_no_ponto(i, e["lat"], e["lon"]), 1))
        if not e["ativa"]:
            corte = dias - rng.randint(20, 200)
            valores[corte:] = [None] * (dias - corte)
        serie_diaria[e["id"]] = valores

    # horária: últimas 168 horas até a referência, coerente com a diária
    perfil = [0.2, 0.15, 0.1, 0.1, 0.1, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.6,
              0.9, 1.4, 2.2, 3.0, 3.3, 2.8, 2.0, 1.3, 0.9, 0.6, 0.4, 0.3]
    horas = [ref - timedelta(hours=167 - k) for k in range(168)]
    serie_horaria = {}
    for e in estacoes:
        valores = []
        for h in horas:
            i = (h.date() - INICIO).days
            total = serie_diaria[e["id"]][i]
            if total is None:
                valores.append(None)
                continue
            pesos = [w * rng.uniform(0.5, 1.5) for w in perfil]
            valores.append(round(total * pesos[h.hour] / sum(pesos), 1))
        serie_horaria[e["id"]] = valores
    # a diária do dia corrente só conta até a hora de referência
    for e in estacoes:
        hoje = [v for h, v in zip(horas, serie_horaria[e["id"]]) if h.date() == ref.date()]
        if serie_diaria[e["id"]][-1] is not None:
            serie_diaria[e["id"]][-1] = round(sum(v for v in hoje if v is not None), 1)

    # ---------------------------------------------------------- nível hidrológico
    chuva_cidade = []
    for i in range(dias):
        vals = [serie_diaria[e["id"]][i] for e in estacoes if serie_diaria[e["id"]][i] is not None]
        chuva_cidade.append(sum(vals) / len(vals) if vals else 0.0)
    nivel_diario, nivel_horario = {}, {}
    for s in sensores_nivel:
        lag = 0.0
        diaria = []
        for i in range(dias):
            local = chuva_no_ponto(i, s["lat"], s["lon"], ruido=False)
            lag = lag * 0.55 + local * 0.035
            diaria.append(round(s["nivel_base"] + lag + rng.uniform(-0.05, 0.05), 2))
        if not s["ativa"]:
            corte = dias - rng.randint(15, 120)
            diaria[corte:] = [None] * (dias - corte)
        nivel_diario[s["id"]] = diaria[-365:]
        hv = []
        nivel = s["nivel_base"]
        for h in horas:
            i = (h.date() - INICIO).days
            if diaria[i] is None:
                hv.append(None)
                continue
            chuva_h = serie_horaria[min(estacoes, key=lambda e: dist_km(e["lat"], e["lon"], s["lat"], s["lon"]))["id"]][horas.index(h)] or 0
            nivel = nivel + (s["nivel_base"] - nivel) * 0.08 + chuva_h * 0.06
            hv.append(round(nivel + rng.uniform(-0.02, 0.02), 2))
        nivel_horario[s["id"]] = hv

    # ------------------------------------------------------------ suscetibilidade
    areas = []
    fid = 0
    for rio, pts in RIOS.items():
        for lon, lat in amostrar_rio(pts):
            for grau, raio in ((1, 0.95), (2, 0.6), (3, 0.3)):
                fid += 1
                pol = bolha(rng, lon, lat, raio)
                areas.append({"id": fid, "camada": "inundacao", "grau": grau, "classe": CLASSES_SUSC[grau][0],
                              "curso_dagua": rio, "area_km2": round(area_km2(pol), 3), "poligono": pol})
    # união por grau (grade de ~100 m), porque os polígonos se sobrepõem
    passo = 0.001
    celulas = {}
    for a in areas:
        xs = [p[0] for p in a["poligono"]]
        ys = [p[1] for p in a["poligono"]]
        for ix in range(int(min(xs) / passo) - 1, int(max(xs) / passo) + 2):
            for iy in range(int(min(ys) / passo) - 1, int(max(ys) / passo) + 2):
                x, y = (ix + 0.5) * passo, (iy + 0.5) * passo
                if ponto_no_poligono(x, y, a["poligono"]):
                    celulas[(ix, iy)] = max(celulas.get((ix, iy), 0), a["grau"])
    area_celula = (passo * KM_LON) * (passo * KM_LAT)
    uniao = {g: round(sum(1 for v in celulas.values() if v == g) * area_celula, 2) for g in (1, 2, 3)}

    def grau_no_ponto(lat, lon):
        g = 0
        for a in areas:
            if a["grau"] <= g:
                continue
            xs = [p[0] for p in a["poligono"]]
            ys = [p[1] for p in a["poligono"]]
            if min(xs) <= lon <= max(xs) and min(ys) <= lat <= max(ys) and ponto_no_poligono(lon, lat, a["poligono"]):
                g = a["grau"]
        return g  # 0 = fora das áreas (cruzado); nulo ficaria para "não cruzado"

    # --------------------------------------------------------------- ocorrências
    focos = [(sum(p[1] for p in a["poligono"]) / len(a["poligono"]), sum(p[0] for p in a["poligono"]) / len(a["poligono"]), a["grau"]) for a in areas]
    pesos_focos = [{3: 6.0, 2: 2.5, 1: 1.0}[g] for _, _, g in focos]
    # pontos recorrentes: alguns endereços acumulam ocorrências ano após ano
    recorrentes = [sortear_no_municipio(rng) for _ in range(25)] + [(f[0], f[1]) for f in rng.sample(focos, 35)]
    fim_ocorrencias = ref.date() - timedelta(days=9)  # o arquivo do GeoSampa chega com atraso
    oc = {k: [] for k in ("ano_origem", "id", "data", "tipo", "subprefeitura", "lat", "lon", "grau_susc_inundacao")}
    contador_ano = {}
    for i, d in enumerate(datas):
        if d > fim_ocorrencias:
            break
        ev = eventos[i]
        if ev is None:
            lam = 0.25
        else:
            base, clat, clon, intens, raio = ev
            pico = base + intens
            lam = 0.3 + max(0.0, pico - 10) ** 1.2 / 2
        k = min(120, poisson(rng, lam))
        for _ in range(k):
            tipo = rng.choices([t for t, _ in TIPOS], [w for _, w in TIPOS])[0]
            r = rng.random()
            if tipo == "Deslizamento":
                lat, lon = sortear_no_municipio(rng)
            elif r < 0.25:
                lat, lon = rng.choice(recorrentes)
                lat += rng.gauss(0, 0.0004); lon += rng.gauss(0, 0.0004)
            elif r < 0.75 and ev is not None:
                cands = rng.choices(focos, pesos_focos, k=6)
                lat, lon, _ = min(cands, key=lambda f: dist_km(f[0], f[1], ev[1], ev[2]) * rng.uniform(0.6, 1.4))
                lat += rng.gauss(0, 0.0025); lon += rng.gauss(0, 0.0025)
            else:
                lat, lon = sortear_no_municipio(rng)
            if not ponto_no_poligono(lon, lat, CONTORNO):
                continue
            contador_ano[d.year] = contador_ano.get(d.year, 0) + 1
            oc["ano_origem"].append(d.year)
            oc["id"].append(contador_ano[d.year])
            oc["data"].append(d.isoformat())
            oc["tipo"].append(tipo)
            oc["subprefeitura"].append(subprefeitura_mais_proxima(lat, lon))
            oc["lat"].append(round(lat, 5))
            oc["lon"].append(round(lon, 5))
            oc["grau_susc_inundacao"].append(grau_no_ponto(lat, lon))

    # -------------------------------------------------------------------- gravação
    def gravar(nome, obj):
        caminho = SAIDA / nome
        caminho.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        return caminho.stat().st_size

    datasets = []

    def publicar(nome, obj):
        tamanho = gravar(nome, obj)
        m = obj["meta"]
        datasets.append({"arquivo": nome, "id": m["id"], "titulo": m["titulo"], "fonte": m["fonte"]["id"] if isinstance(m["fonte"], dict) else m["fonte"],
                         "natureza": m["natureza"], "natureza_representada": m["natureza_representada"],
                         "tabelas_origem": m["tabelas_origem"], "periodo": m.get("periodo"), "processado_em": m["processado_em"],
                         "publicado_em": m["publicado_em"], "registros": m.get("registros"), "bytes": tamanho,
                         "qualidade": m["qualidade"]["estado"]})

    publicar("subprefeituras.json", {
        "meta": meta("subprefeituras", "Subprefeituras", FONTES["geosampa"], ["gold.dim_geosampa_subprefecture_area"], "oficial", ref,
                     registros=len(SUBPREFEITURAS), granularidade={"espacial": "subprefeitura", "temporal": None},
                     licenca="Dados abertos da Prefeitura de São Paulo",
                     observacao="A dimensão da Gold não tem geometria: o recorte territorial é feito pelos pontos da subprefeitura."),
        "data": [{"sigla": s, "nome": nome} for s, nome, _, _ in SUBPREFEITURAS],
    })

    publicar("ocorrencias.json", {
        "meta": meta("ocorrencias", "Ocorrências históricas", FONTES["geosampa"],
                     ["gold.fact_geosampa_flooding_events", "gold.dim_geosampa_type_flooding_events", "gold.dim_geosampa_subprefecture_area", "gold.dim_sgb_susceptibility_areas"],
                     "historico", ref, registros=len(oc["id"]),
                     periodo={"inicio": INICIO.isoformat(), "fim": fim_ocorrencias.isoformat()},
                     granularidade={"espacial": "ponto (SIRGAS 2000)", "temporal": "dia (sem hora)"},
                     licenca="Dados abertos da Prefeitura de São Paulo", unidade="ocorrência",
                     formato="colunar: cada chave de `data` é uma coluna"),
        "data": oc,
    })

    publicar("estacoes.json", {
        "meta": meta("estacoes", "Estações e sensores", {"id": "multiplas", "nome": "CEMADEN, SIBH e INMET"},
                     ["gold.dim_cemaden_stations", "gold.dim_sibh_sensors", "gold.dim_inmet_stations"], "oficial", ref,
                     registros=len(estacoes) + len(sensores_nivel), granularidade={"espacial": "ponto", "temporal": None},
                     licenca="Dados públicos das redes de origem"),
        "data": {"chuva": estacoes, "nivel": [{k: v for k, v in s.items() if k != "nivel_base"} for s in sensores_nivel]},
    })

    publicar("chuva_diaria.json", {
        "meta": meta("chuva_diaria", "Chuva diária por estação", {"id": "multiplas", "nome": "CEMADEN, SIBH e INMET"},
                     ["gold.fact_cemaden_rainfall_hourly", "gold.fact_sibh_rainfall_hourly", "gold.fact_inmet_weather_hourly"],
                     "observado", ref, registros=sum(1 for v in serie_diaria.values() for x in v if x is not None),
                     periodo={"inicio": INICIO.isoformat(), "fim": ref.date().isoformat()}, unidade="mm",
                     granularidade={"espacial": "estação", "temporal": "dia (soma das horas no fuso de São Paulo)"},
                     referencia=ts(ref), licenca="Dados públicos das redes de origem",
                     formato="`series[id][i]` = chuva do dia `inicio + i`; nulo = sem leitura"),
        "data": {"inicio": INICIO.isoformat(), "series": serie_diaria},
    })

    publicar("chuva_horaria.json", {
        "meta": meta("chuva_horaria", "Chuva horária recente", {"id": "multiplas", "nome": "CEMADEN, SIBH e INMET"},
                     ["gold.fact_cemaden_rainfall_hourly", "gold.fact_sibh_rainfall_hourly", "gold.fact_inmet_weather_hourly"],
                     "observado", ref, registros=sum(1 for v in serie_horaria.values() for x in v if x is not None),
                     periodo={"inicio": ts(horas[0]), "fim": ts(horas[-1])}, unidade="mm",
                     granularidade={"espacial": "estação", "temporal": "hora (início do intervalo, fuso de São Paulo)"},
                     referencia=ts(ref), licenca="Dados públicos das redes de origem",
                     formato="`series[id][i]` = chuva da hora `inicio + i h`"),
        "data": {"inicio": ts(horas[0]), "series": serie_horaria},
    })

    publicar("nivel.json", {
        "meta": meta("nivel", "Nível de rios e córregos", FONTES["sibh"], ["gold.fact_sibh_level_hourly", "gold.dim_sibh_sensors"],
                     "observado", ref, registros=len(sensores_nivel),
                     periodo={"inicio": (ref.date() - timedelta(days=364)).isoformat(), "fim": ref.date().isoformat()},
                     unidade="m", unidade_origem="m",
                     granularidade={"espacial": "sensor", "temporal": "hora (7 dias) e média diária (365 dias)"},
                     referencia=ts(ref), licenca="Dados públicos do SIBH",
                     limiares="Não há limiares oficiais de transbordamento integrados nesta versão."),
        "data": {"horario_inicio": ts(horas[0]), "horario": nivel_horario,
                 "diario_inicio": (ref.date() - timedelta(days=364)).isoformat(), "diario": nivel_diario},
    })

    publicar("suscetibilidade.json", {
        "meta": meta("suscetibilidade", "Suscetibilidade a inundações", FONTES["sgb"],
                     ["gold.dim_sgb_susceptibility_areas", "gold.dim_sgb_susceptibility_class"], "derivado", ref,
                     registros=len(areas), data_referencia="2015", unidade="classe (Alta, Média, Baixa)",
                     granularidade={"espacial": "polígono, escala 1:75.000", "temporal": "estático (carta de 2015)"},
                     licenca="Dados públicos do SGB/CPRM", area_municipio_km2=AREA_MUNICIPIO_KM2,
                     area_uniao_km2={"3": uniao[3], "2": uniao[2], "1": uniao[1]},
                     observacao="Suscetibilidade não é previsão: indica a predisposição do terreno, sem considerar a chuva."),
        "data": {
            "classes": [{"grau": g, "classe": c, "camada": "inundacao", "caracteristicas": t} for g, (c, t) in CLASSES_SUSC.items()],
            "areas": [{"id": a["id"], "camada": a["camada"], "grau": a["grau"], "classe": a["classe"], "area_km2": a["area_km2"],
                       "geometria": [[[list(p) for p in a["poligono"]] + [list(a["poligono"][0])]]]} for a in areas],
        },
    })

    publicar("avisos_oficiais.json", {
        "meta": meta("avisos_oficiais", "Avisos oficiais", {"id": "nenhuma", "nome": "Nenhuma fonte de avisos integrada"}, [], "oficial", ref,
                     registros=0, estado_integracao="nao_integrado",
                     observacao="Avisos da Defesa Civil, CEMADEN ou INMET ainda não são integrados ao pipeline."),
        "data": [],
    })

    linhagem = []
    for fonte, tabelas in [
        ("GeoSampa", ["bronze.geosampa_history_flooding_events", "silver.geosampa_flooding_events", "gold.fact_geosampa_flooding_events"]),
        ("CEMADEN", ["bronze.cemaden_rainfall_readings", "silver.cemaden_rainfall_readings", "gold.fact_cemaden_rainfall_hourly"]),
        ("SIBH", ["bronze.sibh_measurements", "silver.sibh_measurements", "gold.fact_sibh_rainfall_hourly", "gold.fact_sibh_level_hourly"]),
        ("INMET", ["bronze.inmet_weather", "silver.inmet_weather_readings", "gold.fact_inmet_weather_hourly"]),
        ("SGB", ["bronze.sgb_susceptibility", "silver.sgb_susceptibility_areas", "gold.dim_sgb_susceptibility_areas"]),
        ("IBGE", ["bronze.ibge_census_tracts", "silver.ibge_census_tracts", "gold.fact_ibge_census_tract_population"]),
        ("OpenStreetMap", ["bronze.osm_roads", "silver.osm_roads", "gold.dim_osm_roads"]),
    ]:
        linhagem.append({"fonte": fonte, "tabelas": tabelas,
                         "publicado_na_web": fonte not in ("IBGE", "OpenStreetMap"),
                         "processado_em": ts(ref - timedelta(hours=rng.randint(1, 6))),
                         "qualidade": "simulado"})

    manifesto = {
        "produto": "ChuvaSegura-SP",
        "modo": "demo",
        "aviso": "Modo demonstração: todos os valores são sintéticos, sem conexão com dados reais.",
        "versao": VERSAO,
        "referencia": ts(ref),
        "publicado_em": ts(ref + timedelta(minutes=40)),
        "gerador": "app/scripts/gerar_dados_demo.py",
        "semente": args.semente,
        "datasets": datasets,
        "linhagem": linhagem,
        "nao_publicados": [
            {"id": "limites", "titulo": "Limites administrativos", "motivo": "O pacote de demonstração não inclui os limites das subprefeituras; eles vêm só da publicação real (GeoSampa)."},
            {"id": "setores", "titulo": "Setores censitários (IBGE)", "motivo": "O pacote de demonstração não inclui setores censitários; eles vêm só da publicação real (IBGE)."},
            {"id": "vias", "titulo": "Rede viária (OpenStreetMap)", "motivo": "O pacote de demonstração não inclui a rede viária; ela vem só da publicação real (OpenStreetMap)."},
        ],
    }
    gravar("manifest.json", manifesto)
    print(f"Referência: {ts(ref)}")
    for d in datasets:
        print(f"  {d['arquivo']:<24} {d['registros'] or 0:>8} registros  {d['bytes'] / 1024:>8.1f} KB")
    print(f"Ocorrências por ano: {dict(sorted(contador_ano.items()))}")
    print(f"União por grau (km²): {uniao}")


if __name__ == "__main__":
    main()
