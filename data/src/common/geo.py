"""Geometria em Python puro para as fontes geoespaciais do projeto ChuvaSegura (SGB, IBGE e OpenStreetMap).

Sem dependências externas, no mesmo espírito da Silver do GeoSampa:

- lê shapefile (.shp, .dbf, .prj e .cpg) e GeoPackage, devolvendo os atributos como texto e a geometria em WKB;
- lê e grava WKB (ISO e EWKB, 2D; Z e M são descartados);
- converte coordenadas entre SIRGAS 2000 geográfico (EPSG 4674, longitude e latitude em graus) e
  SIRGAS 2000 / UTM 23S (EPSG 31983, metros);
- calcula área, comprimento, centroide e envelope.

As funções rodam no driver: os recortes das fontes (milhares a centenas de milhares de feições) cabem na
memória, e assim não é preciso registrar UDF nem levar este módulo para os executores.

Convenção da Silver das fontes geoespaciais: geometria em EPSG 4674 (longitude, latitude), igual ao WGS84
para este uso, e medidas (área e comprimento) calculadas em UTM 23S. Ver `descrever`.
"""
import math
import mmap
import os
import sqlite3
import struct

# ---------- Sistemas de referência ----------

SRID_SIRGAS_GEO = 4674     # SIRGAS 2000, longitude e latitude em graus
SRID_WGS84 = 4326          # WGS84 (OpenStreetMap). Diferença para o SIRGAS 2000 abaixo de 1 m: tratado como igual
SRID_UTM23S = 31983        # SIRGAS 2000 / UTM zona 23S, metros (GeoSampa e SGB)
SRIDS_GEOGRAFICOS = (SRID_SIRGAS_GEO, SRID_WGS84)

# Elipsoide GRS80 (SIRGAS 2000) e parâmetros da zona 23S.
UTM_A = 6378137.0
UTM_F = 1 / 298.257222101
UTM_K0 = 0.9996
UTM_LON0 = math.radians(-45.0)
UTM_FALSO_LESTE, UTM_FALSO_NORTE = 500000.0, 10000000.0
_E2 = UTM_F * (2 - UTM_F)
_EP2 = _E2 / (1 - _E2)

# Área de São Paulo e entorno, a mesma das regras de qualidade das outras fontes.
AREA_SAO_PAULO = {"lat_min": -24.1, "lat_max": -23.3, "lon_min": -47.0, "lon_max": -46.3}


def srid_do_prj(texto):
    """SRID a partir do conteúdo de um arquivo .prj (WKT do ESRI). Devolve None se não reconhecer."""
    t = (texto or "").upper().replace(" ", "_")
    if "UTM" in t and "23S" in t and "SIRGAS" in t:
        return SRID_UTM23S
    if t.startswith("GEOGCS") and "SIRGAS" in t:
        return SRID_SIRGAS_GEO
    if t.startswith("GEOGCS") and ("WGS_1984" in t or "WGS_84" in t or "WGS84" in t):
        return SRID_WGS84
    return None


def utm23s_para_lonlat(x, y):
    """UTM 23S (metros) para (longitude, latitude) em graus. Fórmulas inversas de Snyder (USGS)."""
    e1 = (1 - math.sqrt(1 - _E2)) / (1 + math.sqrt(1 - _E2))
    m = (y - UTM_FALSO_NORTE) / UTM_K0
    mu = m / (UTM_A * (1 - _E2 / 4 - 3 * _E2 ** 2 / 64 - 5 * _E2 ** 3 / 256))
    phi1 = (mu + (3 * e1 / 2 - 27 * e1 ** 3 / 32) * math.sin(2 * mu)
            + (21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32) * math.sin(4 * mu)
            + (151 * e1 ** 3 / 96) * math.sin(6 * mu)
            + (1097 * e1 ** 4 / 512) * math.sin(8 * mu))
    sen, cos, tg = math.sin(phi1), math.cos(phi1), math.tan(phi1)
    c1 = _EP2 * cos ** 2
    t1 = tg ** 2
    n1 = UTM_A / math.sqrt(1 - _E2 * sen ** 2)
    r1 = UTM_A * (1 - _E2) / (1 - _E2 * sen ** 2) ** 1.5
    d = (x - UTM_FALSO_LESTE) / (n1 * UTM_K0)
    lat = phi1 - (n1 * tg / r1) * (
        d ** 2 / 2
        - (5 + 3 * t1 + 10 * c1 - 4 * c1 ** 2 - 9 * _EP2) * d ** 4 / 24
        + (61 + 90 * t1 + 298 * c1 + 45 * t1 ** 2 - 252 * _EP2 - 3 * c1 ** 2) * d ** 6 / 720
    )
    lon = UTM_LON0 + (
        d
        - (1 + 2 * t1 + c1) * d ** 3 / 6
        + (5 - 2 * c1 + 28 * t1 - 3 * c1 ** 2 + 8 * _EP2 + 24 * t1 ** 2) * d ** 5 / 120
    ) / cos
    return math.degrees(lon), math.degrees(lat)


def lonlat_para_utm23s(lon, lat):
    """(longitude, latitude) em graus para UTM 23S (metros). Fórmulas diretas de Snyder (USGS)."""
    phi = math.radians(lat)
    sen, cos, tg = math.sin(phi), math.cos(phi), math.tan(phi)
    n = UTM_A / math.sqrt(1 - _E2 * sen ** 2)
    t = tg ** 2
    c = _EP2 * cos ** 2
    a = cos * (math.radians(lon) - UTM_LON0)
    m = UTM_A * (
        (1 - _E2 / 4 - 3 * _E2 ** 2 / 64 - 5 * _E2 ** 3 / 256) * phi
        - (3 * _E2 / 8 + 3 * _E2 ** 2 / 32 + 45 * _E2 ** 3 / 1024) * math.sin(2 * phi)
        + (15 * _E2 ** 2 / 256 + 45 * _E2 ** 3 / 1024) * math.sin(4 * phi)
        - (35 * _E2 ** 3 / 3072) * math.sin(6 * phi)
    )
    x = UTM_K0 * n * (a + (1 - t + c) * a ** 3 / 6
                      + (5 - 18 * t + t ** 2 + 72 * c - 58 * _EP2) * a ** 5 / 120) + UTM_FALSO_LESTE
    y = UTM_K0 * (m + n * tg * (a ** 2 / 2 + (5 - t + 9 * c + 4 * c ** 2) * a ** 4 / 24
                               + (61 - 58 * t + t ** 2 + 600 * c - 330 * _EP2) * a ** 6 / 720)) + UTM_FALSO_NORTE
    return x, y


# ---------- Geometria em memória ----------
# Uma geometria é (tipo, coordenadas), com tipo em TIPOS e coordenadas aninhadas de pares (x, y):
#   Point: (x, y) | LineString: [pontos] | Polygon: [anéis] | Multi*: [partes do tipo simples]

TIPOS = {1: "Point", 2: "LineString", 3: "Polygon", 4: "MultiPoint", 5: "MultiLineString", 6: "MultiPolygon"}
CODIGOS = {v: k for k, v in TIPOS.items()}
_PROFUNDIDADE = {"Point": 0, "LineString": 1, "Polygon": 2, "MultiPoint": 1, "MultiLineString": 2, "MultiPolygon": 3}


def _mapear(coords, profundidade, funcao):
    if profundidade == 0:
        return funcao(*coords)
    return [_mapear(c, profundidade - 1, funcao) for c in coords]


def transformar(geom, funcao):
    """Aplica `funcao(x, y) -> (x, y)` a todos os pontos da geometria."""
    tipo, coords = geom
    return tipo, _mapear(coords, _PROFUNDIDADE[tipo], funcao)


def pontos(geom):
    """Todos os pontos da geometria, em sequência."""
    tipo, coords = geom
    if tipo == "Point":
        return [coords]
    pilha, saida = [(coords, _PROFUNDIDADE[tipo])], []
    while pilha:
        c, p = pilha.pop()
        if p == 1:
            saida.extend(c)
        else:
            pilha.extend((filho, p - 1) for filho in reversed(c))
    return saida


def poligonos(geom):
    """Lista de polígonos (cada um uma lista de anéis) de um Polygon ou MultiPolygon."""
    tipo, coords = geom
    if tipo == "Polygon":
        return [coords]
    if tipo == "MultiPolygon":
        return coords
    return []


def linhas(geom):
    """Lista de linhas de um LineString ou MultiLineString."""
    tipo, coords = geom
    if tipo == "LineString":
        return [coords]
    if tipo == "MultiLineString":
        return coords
    return []


def area_assinada(anel):
    """Área com sinal pela fórmula do laço: positiva no sentido anti-horário.

    Calculada em relação ao primeiro ponto do anel, para não perder precisão com coordenadas UTM grandes.
    """
    if not anel:
        return 0.0
    ox, oy = anel[0]
    soma = 0.0
    for (x1, y1), (x2, y2) in zip(anel, anel[1:] + anel[:1]):
        soma += (x1 - ox) * (y2 - oy) - (x2 - ox) * (y1 - oy)
    return soma / 2


def area(geom):
    """Área de um Polygon ou MultiPolygon nas unidades das coordenadas ao quadrado (anel externo menos buracos)."""
    total = 0.0
    for poligono in poligonos(geom):
        if not poligono:
            continue
        total += abs(area_assinada(poligono[0])) - sum(abs(area_assinada(b)) for b in poligono[1:])
    return total


def comprimento(geom):
    """Comprimento de um LineString ou MultiLineString nas unidades das coordenadas."""
    return sum(math.dist(a, b) for linha in linhas(geom) for a, b in zip(linha, linha[1:]))


def centroide(geom):
    """Centroide (x, y): ponderado pela área nos polígonos, pelo comprimento nas linhas, média nos pontos.

    Em polígonos côncavos o centroide pode cair fora da feição. As contas são feitas em relação ao primeiro
    ponto da geometria: com coordenadas UTM (milhões de metros), a fórmula direta perde precisão em polígonos
    de poucos metros quadrados.
    """
    pts = pontos(geom)
    if not pts:
        return None
    ox, oy = pts[0]

    def relativo(anel):
        return [(x - ox, y - oy) for x, y in anel]

    cx = cy = peso = 0.0
    for poligono in poligonos(geom):
        for i, anel in enumerate(poligono):
            anel = relativo(anel)
            a = area_assinada(anel)
            if a == 0:
                continue
            sx = sy = 0.0
            for (x1, y1), (x2, y2) in zip(anel, anel[1:] + anel[:1]):
                f = x1 * y2 - x2 * y1
                sx += (x1 + x2) * f
                sy += (y1 + y2) * f
            # Momento e área com o mesmo sinal: a razão (centroide do anel) não depende do sentido do anel.
            orientacao = 1 if a > 0 else -1
            sinal = 1 if i == 0 else -1  # buraco subtrai
            cx += sinal * orientacao * sx / 6
            cy += sinal * orientacao * sy / 6
            peso += sinal * abs(a)
    if peso > 0:
        return ox + cx / peso, oy + cy / peso
    cx = cy = peso = 0.0
    for linha in linhas(geom):
        linha = relativo(linha)
        for (x1, y1), (x2, y2) in zip(linha, linha[1:]):
            d = math.dist((x1, y1), (x2, y2))
            cx += (x1 + x2) / 2 * d
            cy += (y1 + y2) / 2 * d
            peso += d
    if peso > 0:
        return ox + cx / peso, oy + cy / peso
    return ox + sum(p[0] - ox for p in pts) / len(pts), oy + sum(p[1] - oy for p in pts) / len(pts)


def envelope(geom):
    """(xmin, ymin, xmax, ymax) da geometria, ou None se vazia."""
    pts = pontos(geom)
    if not pts:
        return None
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    return min(xs), min(ys), max(xs), max(ys)


def ponto_no_anel(x, y, anel):
    """Teste do raio: True se (x, y) está dentro do anel."""
    dentro = False
    for (x1, y1), (x2, y2) in zip(anel, anel[1:] + anel[:1]):
        if (y1 > y) != (y2 > y) and x < (x2 - x1) * (y - y1) / (y2 - y1) + x1:
            dentro = not dentro
    return dentro


# ---------- WKB ----------

def para_wkb(geom):
    """Geometria em memória para WKB ISO 2D, little-endian."""
    if geom is None:
        return None
    partes = []

    def escrever(tipo, coords):
        partes.append(struct.pack("<BI", 1, CODIGOS[tipo]))
        if tipo == "Point":
            partes.append(struct.pack("<2d", *coords))
        elif tipo == "LineString":
            partes.append(struct.pack("<I", len(coords)))
            partes.append(struct.pack(f"<{2 * len(coords)}d", *(v for p in coords for v in p)))
        elif tipo == "Polygon":
            partes.append(struct.pack("<I", len(coords)))
            for anel in coords:
                partes.append(struct.pack("<I", len(anel)))
                partes.append(struct.pack(f"<{2 * len(anel)}d", *(v for p in anel for v in p)))
        else:
            simples = tipo[len("Multi"):]
            partes.append(struct.pack("<I", len(coords)))
            for parte in coords:
                escrever(simples, parte)

    escrever(*geom)
    return b"".join(partes)


def de_wkb(wkb):
    """WKB (ISO ou EWKB, com ou sem Z/M) para geometria em memória 2D. None para vazio ou nulo."""
    if wkb is None:
        return None
    dados = bytes(wkb)
    if not dados:
        return None

    def ler(pos):
        ordem = "<" if dados[pos] == 1 else ">"
        (codigo,) = struct.unpack_from(ordem + "I", dados, pos + 1)
        pos += 5
        dims = 2
        if codigo & 0x20000000:  # EWKB com SRID embutido
            pos += 4
        if codigo & 0x80000000:
            dims += 1
        if codigo & 0x40000000:
            dims += 1
        codigo &= 0x0FFFFFFF
        if codigo >= 1000:
            dims += {1: 1, 2: 1, 3: 2}[codigo // 1000]
            codigo %= 1000
        tipo = TIPOS.get(codigo)
        if tipo is None:
            raise ValueError(f"Tipo de geometria WKB não suportado: {codigo}")

        def ler_pontos(p, n):
            valores = struct.unpack_from(f"{ordem}{n * dims}d", dados, p)
            return [(valores[i], valores[i + 1]) for i in range(0, n * dims, dims)], p + 8 * n * dims

        if tipo == "Point":
            pts, pos = ler_pontos(pos, 1)
            return (tipo, pts[0]), pos
        (n,) = struct.unpack_from(ordem + "I", dados, pos)
        pos += 4
        if tipo == "LineString":
            pts, pos = ler_pontos(pos, n)
            return (tipo, pts), pos
        if tipo == "Polygon":
            aneis = []
            for _ in range(n):
                (m,) = struct.unpack_from(ordem + "I", dados, pos)
                anel, pos = ler_pontos(pos + 4, m)
                aneis.append(anel)
            return (tipo, aneis), pos
        partes = []
        for _ in range(n):
            (_, coords), pos = ler(pos)
            partes.append(coords)
        return (tipo, partes), pos

    geom, _ = ler(0)
    if geom[0] == "Point" and any(math.isnan(v) for v in geom[1]):
        return None  # POINT EMPTY
    return geom


def descrever(wkb, srid):
    """Padroniza uma geometria para a Silver: devolve um dicionário com

    - `geom_wkb`: WKB em EPSG 4674 (longitude, latitude);
    - `tipo`, `pontos`: tipo da geometria e número de vértices;
    - `area_km2` (polígonos) e `comprimento_km` (linhas), calculados em UTM 23S;
    - `centroide_lon`, `centroide_lat` e o envelope `lon_min`, `lat_min`, `lon_max`, `lat_max`.

    Aceita geometrias em UTM 23S (31983) ou geográficas (4674 e 4326). Devolve None para geometria nula ou vazia.
    """
    geom = de_wkb(wkb)
    if geom is None or not pontos(geom):
        return None
    if srid == SRID_UTM23S:
        utm, geo = geom, transformar(geom, utm23s_para_lonlat)
    elif srid in SRIDS_GEOGRAFICOS:
        utm, geo = transformar(geom, lonlat_para_utm23s), geom
    else:
        raise ValueError(f"SRID {srid} não suportado (use 31983, 4674 ou 4326)")
    lon_min, lat_min, lon_max, lat_max = envelope(geo)
    c = centroide(utm)
    c_lon, c_lat = utm23s_para_lonlat(*c) if c else (None, None)
    poligonal, linear = geom[0].endswith("Polygon"), geom[0].endswith("LineString")
    return {
        "geom_wkb": para_wkb(geo),
        "tipo": geom[0],
        "pontos": len(pontos(geom)),
        "area_km2": area(utm) / 1e6 if poligonal else None,
        "comprimento_km": comprimento(utm) / 1e3 if linear else None,
        "centroide_lon": c_lon,
        "centroide_lat": c_lat,
        "lon_min": lon_min,
        "lat_min": lat_min,
        "lon_max": lon_max,
        "lat_max": lat_max,
    }


# ---------- Shapefile ----------

def _texto(valor):
    valor = valor.strip()
    return valor or None


def _encoding_dbf(caminho_shp, padrao):
    cpg = os.path.splitext(caminho_shp)[0] + ".cpg"
    if os.path.exists(cpg):
        with open(cpg, encoding="ascii", errors="ignore") as f:
            nome = f.read().strip().lower()
        return {"utf-8": "utf-8", "utf8": "utf-8", "65001": "utf-8", "1252": "cp1252"}.get(nome, nome or padrao)
    return padrao


def _aneis_para_poligonos(aneis):
    """Anéis do shapefile para polígonos: anel horário é externo, anti-horário é buraco do externo que o contém."""
    externos, buracos = [], []
    for anel in aneis:
        if len(anel) < 4:
            continue
        (externos if area_assinada(anel) <= 0 else buracos).append(anel)
    if not externos:  # arquivo sem a orientação do padrão: cada anel vira um polígono
        return [[a] for a in buracos]
    poligonos_ = [[e] for e in externos]
    for b in buracos:
        x, y = b[0]
        dono = next((p for p in poligonos_ if ponto_no_anel(x, y, p[0])), poligonos_[-1])
        dono.append(b)
    return poligonos_


def ler_shapefile(caminho_shp, encoding="latin-1", filtro_envelope=None):
    """Lê um shapefile e gera (indice, atributos, wkb) para cada registro.

    - `indice`: posição do registro no arquivo (1, 2, 3...), que identifica a feição dentro da versão do arquivo.
    - `atributos`: dicionário com os campos do .dbf em texto (sem espaços nas pontas; vazio vira None).
      O encoding vem do .cpg quando existe; senão usa `encoding`.
    - `wkb`: geometria em WKB ISO 2D, nas coordenadas do arquivo (ver `srid_do_prj`). None para registro sem geometria.
    - `filtro_envelope`: (xmin, ymin, xmax, ymax) no sistema do arquivo. Registros cujo envelope não cruza
      o filtro são pulados sem ler o .dbf (útil para recortar arquivos grandes, como os do OpenStreetMap).

    Tipos suportados: ponto, multiponto, linha e polígono, nas variantes 2D, Z e M (Z e M são descartados).
    """
    base = os.path.splitext(caminho_shp)[0]
    encoding = _encoding_dbf(caminho_shp, encoding)
    with open(base + ".dbf", "rb") as fdbf, open(caminho_shp, "rb") as fshp:
        dbf = mmap.mmap(fdbf.fileno(), 0, access=mmap.ACCESS_READ)
        shp = mmap.mmap(fshp.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            n_dbf, tam_cab, tam_reg = struct.unpack_from("<IHH", dbf, 4)
            campos, pos = [], 32
            while dbf[pos] != 0x0D:
                nome = dbf[pos:pos + 11].split(b"\0")[0].decode("ascii", "replace")
                campos.append((nome, dbf[pos + 16]))
                pos += 32

            (cod,) = struct.unpack_from(">i", shp, 0)
            if cod != 9994:
                raise ValueError(f"{caminho_shp} não é um shapefile")
            tamanho = struct.unpack_from(">i", shp, 24)[0] * 2
            pos, indice = 100, 0
            while pos < tamanho and indice < n_dbf:
                _, palavras = struct.unpack_from(">2i", shp, pos)
                inicio, pos = pos + 8, pos + 8 + palavras * 2
                indice += 1
                (tipo,) = struct.unpack_from("<i", shp, inicio)
                base_tipo = tipo % 10 if tipo < 30 else None
                if tipo != 0 and base_tipo not in (1, 3, 5, 8):
                    raise ValueError(f"Tipo de shape não suportado: {tipo}")

                if filtro_envelope is not None and tipo != 0:
                    if base_tipo == 1:
                        x, y = struct.unpack_from("<2d", shp, inicio + 4)
                        caixa = (x, y, x, y)
                    else:
                        caixa = struct.unpack_from("<4d", shp, inicio + 4)
                    fx1, fy1, fx2, fy2 = filtro_envelope
                    if caixa[2] < fx1 or caixa[0] > fx2 or caixa[3] < fy1 or caixa[1] > fy2:
                        continue

                reg = tam_cab + (indice - 1) * tam_reg
                if dbf[reg:reg + 1] == b"*":  # registro apagado no .dbf
                    continue
                atributos, p = {}, reg + 1
                for nome, tam in campos:
                    atributos[nome] = _texto(dbf[p:p + tam].decode(encoding, "replace"))
                    p += tam

                if tipo == 0:
                    geom = None
                elif base_tipo == 1:
                    geom = ("Point", struct.unpack_from("<2d", shp, inicio + 4))
                elif base_tipo == 8:
                    (n,) = struct.unpack_from("<i", shp, inicio + 36)
                    v = struct.unpack_from(f"<{2 * n}d", shp, inicio + 40)
                    geom = ("MultiPoint", [(v[i], v[i + 1]) for i in range(0, 2 * n, 2)])
                else:
                    n_partes, n_pontos = struct.unpack_from("<2i", shp, inicio + 36)
                    partes = list(struct.unpack_from(f"<{n_partes}i", shp, inicio + 44)) + [n_pontos]
                    v = struct.unpack_from(f"<{2 * n_pontos}d", shp, inicio + 44 + 4 * n_partes)
                    pts = [(v[i], v[i + 1]) for i in range(0, 2 * n_pontos, 2)]
                    trechos = [pts[a:b] for a, b in zip(partes, partes[1:])]
                    if base_tipo == 3:
                        trechos = [t for t in trechos if len(t) >= 2]
                        if not trechos:
                            geom = None
                        elif len(trechos) == 1:
                            geom = ("LineString", trechos[0])
                        else:
                            geom = ("MultiLineString", trechos)
                    else:
                        polis = _aneis_para_poligonos(trechos)
                        if not polis:
                            geom = None
                        elif len(polis) == 1:
                            geom = ("Polygon", polis[0])
                        else:
                            geom = ("MultiPolygon", polis)
                yield indice, atributos, para_wkb(geom)
        finally:
            shp.close()
            dbf.close()


def srid_shapefile(caminho_shp):
    """SRID do shapefile lido do .prj, ou None se não houver .prj ou o sistema não for reconhecido."""
    prj = os.path.splitext(caminho_shp)[0] + ".prj"
    if not os.path.exists(prj):
        return None
    with open(prj, encoding="latin-1") as f:
        return srid_do_prj(f.read())


# ---------- GeoPackage ----------

_TAMANHO_ENVELOPE = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}


def gpkg_para_wkb(blob):
    """Remove o cabeçalho da geometria do GeoPackage e devolve o WKB padrão (None se vazia)."""
    if blob is None:
        return None
    dados = bytes(blob)
    if dados[:2] != b"GP":
        return None
    flags = dados[3]
    if (flags >> 4) & 1:  # geometria vazia
        return None
    return dados[8 + _TAMANHO_ENVELOPE[(flags >> 1) & 7]:]


def camadas_gpkg(caminho):
    """[(camada, coluna_geometria, srs_id, [colunas de atributos])] das camadas de feições do GeoPackage."""
    conn = sqlite3.connect(f"file:{caminho}?mode=ro", uri=True)
    try:
        saida = []
        for (camada,) in conn.execute("SELECT table_name FROM gpkg_contents WHERE data_type = 'features'"):
            geom = conn.execute("SELECT column_name, srs_id FROM gpkg_geometry_columns WHERE table_name = ?",
                                (camada,)).fetchone()
            coluna, srs = geom if geom else (None, -1)
            attrs = [r[1] for r in conn.execute(f'PRAGMA table_info("{camada}")') if r[1] != coluna]
            saida.append((camada, coluna, int(srs), attrs))
        return saida
    finally:
        conn.close()


def ler_gpkg(caminho, camada, where=None, parametros=()):
    """Gera (atributos, wkb) de uma camada do GeoPackage. Atributos em texto; `where` é um filtro SQL opcional."""
    _, coluna, _, attrs = next(c for c in camadas_gpkg(caminho) if c[0] == camada)
    colunas = [f'"{c}"' for c in attrs] + [f'"{coluna}"' if coluna else "NULL"]
    sql = f'SELECT {", ".join(colunas)} FROM "{camada}"' + (f" WHERE {where}" if where else "")
    conn = sqlite3.connect(f"file:{caminho}?mode=ro", uri=True)
    try:
        for linha in conn.execute(sql, parametros):
            atributos = {c: (None if v is None else str(v)) for c, v in zip(attrs, linha[:-1])}
            yield atributos, gpkg_para_wkb(linha[-1])
    finally:
        conn.close()
