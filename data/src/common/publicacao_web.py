"""Funções puras (sem Spark) para montar os artefatos do site a partir de linhas das tabelas Gold.

São usadas por data/src/shared/export/01_export_web.py e testadas em data/tests/.
Horas e dias seguem o fuso de São Paulo, sem horário de verão desde 2019 (UTC−03:00), como nas fatos.
"""

import hashlib
import json
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from common import geo

TZ_SP = timezone(timedelta(hours=-3))
DIA_INICIAL = date(2020, 1, 1)  # início da cobertura de chuva (CEMADEN, SIBH e INMET)
HORAS_RECENTES = 168  # 7 dias
DIAS_NIVEL = 365
FATOR_PARA_METRO = {"cm": 0.01, "m": 1.0}  # unidade da cota na fonte -> metros (unidade publicada)


def cod_para_data(cod_dim_tempo):
    """yyyymmdd (int) -> date."""
    c = int(cod_dim_tempo)
    return date(c // 10000, (c // 100) % 100, c % 100)


def data_para_cod(d):
    return d.year * 10000 + d.month * 100 + d.day


def hora_sp(cod_dim_tempo, hora):
    """Início da hora (datetime em UTC−03:00) de uma linha da fato."""
    d = cod_para_data(cod_dim_tempo)
    return datetime(d.year, d.month, d.day, int(hora), tzinfo=TZ_SP)


def iso_sp(dt):
    return dt.astimezone(TZ_SP).isoformat(timespec="seconds")


def json_bytes(valor):
    return json.dumps(valor, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def sha256_dados(valor):
    return hashlib.sha256(json_bytes(valor)).hexdigest()


def _arredonda(v, casas):
    return None if v is None else round(float(v), casas)


def serie_diaria(linhas, dia_final, casas=1, dia_inicial=DIA_INICIAL):
    """linhas: (id, cod_dim_tempo, valor) já somadas por dia. Retorna {id: [valor ou None]} de dia_inicial a dia_final.
    Dia sem linha fica None (sem leitura, não zero); linhas fora do intervalo são ignoradas."""
    n = (dia_final - dia_inicial).days + 1
    out = {}
    for id_, cod, valor in linhas:
        i = (cod_para_data(cod) - dia_inicial).days
        if 0 <= i < n and valor is not None:
            out.setdefault(id_, [None] * n)[i] = _arredonda(valor, casas)
    return out


def serie_horaria(linhas, hora_final, horas=HORAS_RECENTES, casas=1):
    """linhas: (id, cod_dim_tempo, hora, valor). Retorna (inicio, {id: [valor ou None] * horas}), com a janela
    terminando em hora_final (início do intervalo da última hora)."""
    inicio = hora_final - timedelta(hours=horas - 1)
    out = {}
    for id_, cod, hora, valor in linhas:
        i = int((hora_sp(cod, hora) - inicio).total_seconds() // 3600)
        if 0 <= i < horas and valor is not None:
            out.setdefault(id_, [None] * horas)[i] = _arredonda(valor, casas)
    return inicio, out


def pior_qualidade(estados):
    """aprovado < aprovado_com_avisos; qualquer outra coisa nunca chega aqui (a trava cancela antes)."""
    return "aprovado_com_avisos" if "aprovado_com_avisos" in estados else "aprovado"


# ---------- Geometria para a web ----------

# Escala local em São Paulo (cerca de 23,6° S): metros por grau, para medir em metros direto em longitude e latitude.
# O erro, no município, fica abaixo de 0,5% e não importa para simplificar nem para somar áreas.
M_POR_GRAU_LON = 102_050.0
M_POR_GRAU_LAT = 110_780.0


def simplificar(pts, tolerancia_m):
    """Douglas-Peucker iterativo sobre (lon, lat), com a tolerância em metros. Mantém o primeiro e o último ponto."""
    n = len(pts)
    if n < 3:
        return list(pts)
    manter = [False] * n
    manter[0] = manter[-1] = True
    tol2 = tolerancia_m * tolerancia_m
    pilha = [(0, n - 1)]
    while pilha:
        i, j = pilha.pop()
        ax, ay = pts[i][0] * M_POR_GRAU_LON, pts[i][1] * M_POR_GRAU_LAT
        dx, dy = pts[j][0] * M_POR_GRAU_LON - ax, pts[j][1] * M_POR_GRAU_LAT - ay
        l2 = dx * dx + dy * dy
        dmax, kmax = -1.0, -1
        for k in range(i + 1, j):
            px, py = pts[k][0] * M_POR_GRAU_LON - ax, pts[k][1] * M_POR_GRAU_LAT - ay
            t = 0.0 if l2 == 0 else max(0.0, min(1.0, (px * dx + py * dy) / l2))
            ex, ey = px - t * dx, py - t * dy
            d2 = ex * ex + ey * ey
            if d2 > dmax:
                dmax, kmax = d2, k
        if dmax > tol2:
            manter[kmax] = True
            pilha += [(i, kmax), (kmax, j)]
    return [p for p, m in zip(pts, manter) if m]


def _arredondar(pts, casas):
    out = []
    for x, y in pts:
        q = [round(x, casas), round(y, casas)]
        if not out or out[-1] != q:
            out.append(q)
    return out


def anel_web(anel, tolerancia_m, casas=5):
    """Anel simplificado, arredondado e fechado; None se virar menos de um triângulo."""
    anel = list(anel)
    if anel and anel[0] != anel[-1]:
        anel.append(anel[0])
    s = _arredondar(simplificar(anel, tolerancia_m), casas)
    if s and s[0] != s[-1]:
        s.append(list(s[0]))
    return s if len(s) >= 4 else None


def poligonos_web(geom, tolerancia_m, casas=5):
    """Polygon ou MultiPolygon (geo.de_wkb) para [[anel externo, buracos...], ...] simplificado (coordenadas do
    MultiPolygon do GeoJSON). Partes e buracos que somem na simplificação são descartados."""
    out = []
    for pol in geo.poligonos(geom):
        ext = anel_web(pol[0], tolerancia_m, casas) if pol else None
        if ext:
            out.append([ext] + [b for b in (anel_web(h, tolerancia_m, casas) for h in pol[1:]) if b])
    return out


def linhas_web(geom, tolerancia_m, casas=5):
    """LineString ou MultiLineString para [[[lon, lat], ...], ...] simplificado (MultiLineString do GeoJSON)."""
    out = []
    for linha in geo.linhas(geom):
        s = _arredondar(simplificar(linha, tolerancia_m), casas)
        if len(s) >= 2:
            out.append(s)
    return out


def ponto_no_poligono(x, y, poligono):
    """Dentro do anel externo e fora dos buracos."""
    return geo.ponto_no_anel(x, y, poligono[0]) and not any(geo.ponto_no_anel(x, y, b) for b in poligono[1:])


class IndicePoligonos:
    """Grade regular sobre os envelopes, para achar só os polígonos candidatos de cada ponto.
    itens: [(valor, poligonos)], com poligonos = geo.poligonos(geom) em (lon, lat)."""

    def __init__(self, itens, passo=0.01):
        self.passo = passo
        self.itens = []
        self.celulas = defaultdict(list)
        for valor, pols in itens:
            pts = [p for pol in pols if pol for p in pol[0]]
            if not pts:
                continue
            x0, x1 = min(p[0] for p in pts), max(p[0] for p in pts)
            y0, y1 = min(p[1] for p in pts), max(p[1] for p in pts)
            idx = len(self.itens)
            self.itens.append((valor, pols, (x0, y0, x1, y1)))
            for cx in range(math.floor(x0 / passo), math.floor(x1 / passo) + 1):
                for cy in range(math.floor(y0 / passo), math.floor(y1 / passo) + 1):
                    self.celulas[(cx, cy)].append(idx)

    def valores_no_ponto(self, x, y):
        for idx in self.celulas.get((math.floor(x / self.passo), math.floor(y / self.passo)), ()):
            valor, pols, (x0, y0, x1, y1) = self.itens[idx]
            if x0 <= x <= x1 and y0 <= y <= y1 and any(ponto_no_poligono(x, y, pol) for pol in pols if pol):
                yield valor

    def maior_valor(self, x, y, padrao=0):
        return max(self.valores_no_ponto(x, y), default=padrao)


def _intervalos_por_faixa(pols, y0, passo_m):
    """Por faixa horizontal (centro em y0 + (k + 0,5) * passo), os intervalos em x (metros) dentro dos polígonos,
    pela regra par-ímpar (buracos ficam de fora). Custo proporcional ao contorno, não à área."""
    faixas = defaultdict(list)
    for pol in pols:
        cruzamentos = defaultdict(list)
        for anel in pol:
            m = [(x * M_POR_GRAU_LON, y * M_POR_GRAU_LAT) for x, y in anel]
            for (xa, ya), (xb, yb) in zip(m, m[1:] + m[:1]):
                if ya == yb:
                    continue
                lo, hi = min(ya, yb), max(ya, yb)
                for k in range(math.ceil((lo - y0) / passo_m - 0.5), math.ceil((hi - y0) / passo_m - 0.5)):
                    yc = y0 + (k + 0.5) * passo_m
                    cruzamentos[k].append(xa + (yc - ya) * (xb - xa) / (yb - ya))
        for k, xs in cruzamentos.items():
            xs.sort()
            faixas[k].extend(zip(xs[0::2], xs[1::2]))
    return faixas


def _comprimento_uniao(intervalos):
    total, fim = 0.0, -math.inf
    for a, b in sorted(intervalos):
        if b <= fim:
            continue
        total += b - max(a, fim)
        fim = b
    return total


def area_uniao_por_grau(itens, passo_m=10.0):
    """Área (km²) de cada grau sem contar sobreposições: cada ponto conta uma vez, no maior grau que o cobre.
    itens: [(grau, poligonos)] em (lon, lat). Varredura em faixas de `passo_m` metros (erro desprezível em km²)."""
    if not itens:
        return {}
    y0 = min(p[1] for _, pols in itens for pol in pols if pol for p in pol[0]) * M_POR_GRAU_LAT
    por_grau = defaultdict(lambda: defaultdict(list))
    for grau, pols in itens:
        for k, ivs in _intervalos_por_faixa(pols, y0, passo_m).items():
            por_grau[grau][k].extend(ivs)
    area, acumulado = {}, defaultdict(list)
    anterior = 0.0
    for grau in sorted(por_grau, reverse=True):
        for k, ivs in por_grau[grau].items():
            acumulado[k].extend(ivs)
        total = sum(_comprimento_uniao(ivs) for ivs in acumulado.values()) * passo_m / 1e6
        area[grau] = total - anterior
        anterior = total
    return area
