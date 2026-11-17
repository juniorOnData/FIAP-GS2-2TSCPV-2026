"""Funções puras (sem Spark) para montar os artefatos do site a partir de linhas das tabelas Gold.

São usadas por data/src/shared/export/01_export_web.py e testadas em data/tests/.
Horas e dias seguem o fuso de São Paulo, sem horário de verão desde 2019 (UTC−03:00), como nas fatos.
"""

import hashlib
import json
from datetime import date, datetime, timedelta, timezone

TZ_SP = timezone(timedelta(hours=-3))
DIA_INICIAL = date(2020, 1, 1)  # início da cobertura de chuva (CEMADEN, SIBH e INMET)
HORAS_RECENTES = 168  # 7 dias
DIAS_NIVEL = 365


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
