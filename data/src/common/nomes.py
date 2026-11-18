"""Casamento de nomes entre fontes (sem Spark): por exemplo, as subprefeituras do shapefile do GeoSampa com a
dimensão do Anexo IX, que escrevem o mesmo nome de formas diferentes ("M'BOI MIRIM", "M Boi Mirim")."""

import unicodedata


def normalizar_nome(texto):
    """Maiúsculas, sem acentos e só letras e números: "Freguesia/Brasilândia" -> "FREGUESIABRASILANDIA"."""
    if texto is None:
        return ""
    sem_acento = unicodedata.normalize("NFD", str(texto)).encode("ascii", "ignore").decode("ascii")
    return "".join(c for c in sem_acento.upper() if c.isalnum())


def campo_que_melhor_casa(registros, chaves):
    """Entre os campos de `registros` (lista de dicionários de atributos), o que mais casa com `chaves`
    (conjunto de valores já normalizados). Devolve (campo, quantos casaram); (None, 0) se nenhum casar.
    Serve para achar a coluna de nome ou sigla sem depender de como a fonte batizou os campos."""
    melhor, n_melhor = None, 0
    campos = sorted({k for r in registros for k in r})
    for campo in campos:
        n = sum(1 for r in registros if normalizar_nome(r.get(campo)) in chaves)
        if n > n_melhor:
            melhor, n_melhor = campo, n
    return melhor, n_melhor
