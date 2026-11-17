"""Conversões de texto usadas nas Silvers do projeto ChuvaSegura.

As funções devolvem expressões do Spark (sem UDF), então rodam nos executores sem depender deste módulo.
Os notebooks que usam estas funções devem fixar o fuso da sessão em UTC antes de converter datas:

    spark.conf.set("spark.sql.session.timeZone", "UTC")
"""
from pyspark.sql import Column
from pyspark.sql import functions as F

FUSO_LOCAL = "America/Sao_Paulo"

# Formatos aceitos, do mais específico para o mais genérico. O primeiro que reconhecer o texto vale.
FORMATOS_DATA_HORA = (
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy-MM-dd HH:mm:ss.S",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    "yyyy-MM-dd'T'HH:mm:ssXXX",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm",
    "yyyy/MM/dd HH:mm:ss",
    "yyyy/MM/dd HH:mm",
    "dd/MM/yyyy HH:mm:ss",
    "dd/MM/yyyy HH:mm",
)
FORMATOS_DATA = ("yyyy-MM-dd", "yyyy/MM/dd", "dd/MM/yyyy")


def _texto(coluna: str) -> Column:
    return F.nullif(F.trim(F.col(coluna)), F.lit(""))


def texto_para_timestamp(coluna: str, formatos=FORMATOS_DATA_HORA) -> Column:
    """Texto para TIMESTAMP, interpretado no fuso da sessão (UTC). Vazio ou fora dos formatos vira nulo."""
    texto = _texto(coluna)
    return F.coalesce(*[F.try_to_timestamp(texto, F.lit(f)) for f in formatos])


def texto_para_data(coluna: str, formatos=FORMATOS_DATA) -> Column:
    """Texto para DATE. Aceita também data e hora (usa só a data). Vazio ou fora dos formatos vira nulo."""
    texto = _texto(coluna)
    data = F.coalesce(*[F.to_date(F.try_to_timestamp(F.substring(texto, 1, 10), F.lit(f))) for f in formatos])
    return F.coalesce(data, F.to_date(texto_para_timestamp(coluna)))


def texto_para_double(coluna: str) -> Column:
    """Texto para DOUBLE, aceitando vírgula decimal. Vazio ou não numérico vira nulo."""
    return F.expr(f"try_cast(replace(nullif(trim(`{coluna}`), ''), ',', '.') AS DOUBLE)")


def data_local(utc: Column) -> Column:
    """Data no fuso de São Paulo de um TIMESTAMP em UTC."""
    return F.to_date(F.from_utc_timestamp(utc, FUSO_LOCAL))


def hora_local(utc: Column) -> Column:
    """Hora (0 a 23) no fuso de São Paulo de um TIMESTAMP em UTC."""
    return F.hour(F.from_utc_timestamp(utc, FUSO_LOCAL))


def cod_dim_tempo(data: Column) -> Column:
    """Chave da dim_tempo (yyyyMMdd) a partir de uma data."""
    return F.date_format(data, "yyyyMMdd").cast("bigint")
