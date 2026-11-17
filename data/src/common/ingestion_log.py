"""Log de ingestão do projeto ChuvaSegura.

Cada origem tem a sua tabela (cemaden_ingestion_log, sibh_ingestion_log, geosampa_ingestion_log),
todas com o formato de LOG_SCHEMA. Status usados: SUCESSO, PARCIAL, SEM_DADOS e FALHA.

Uso nos notebooks:

    registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar
    registrar("202611", "SUCESSO", iniciado_em, registros=1200, arquivo=caminho)
"""
from datetime import datetime, timezone

from pyspark.sql import types as T

LOG_SCHEMA = T.StructType([
    T.StructField("batch_id", T.StringType()),
    T.StructField("fonte", T.StringType()),
    T.StructField("etapa", T.StringType()),
    T.StructField("janela", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("registros", T.LongType()),
    T.StructField("paginas", T.IntegerType()),
    T.StructField("duplicados", T.LongType()),
    T.StructField("arquivo", T.StringType()),
    T.StructField("sha256", T.StringType()),
    T.StructField("duracao_s", T.DoubleType()),
    T.StructField("iniciado_em", T.TimestampType()),
    T.StructField("finalizado_em", T.TimestampType()),
    T.StructField("mensagem", T.StringType()),
])

DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  batch_id      STRING    COMMENT 'Identificador da execução',
  fonte         STRING    COMMENT 'Conjunto de dados',
  etapa         STRING    COMMENT 'landing ou bronze',
  janela        STRING    COMMENT 'Período processado: mês, data da coleta ou ano do arquivo',
  status        STRING    COMMENT 'SUCESSO, PARCIAL, SEM_DADOS ou FALHA',
  registros     BIGINT,
  paginas       INT       COMMENT 'Chamadas à API',
  duplicados    BIGINT    COMMENT 'Duplicidades pela chave natural do conjunto',
  arquivo       STRING,
  sha256        STRING,
  duracao_s     DOUBLE,
  iniciado_em   TIMESTAMP,
  finalizado_em TIMESTAMP,
  mensagem      STRING
) COMMENT 'Controle e checkpoint das cargas do projeto ChuvaSegura, uma tabela por origem'
"""


class IngestionLog:
    """Log de uma etapa (landing ou bronze) de um conjunto de dados."""

    def __init__(self, spark, table: str, fonte: str, etapa: str, batch_id: str):
        self.spark = spark
        self.table = table
        self.fonte = fonte
        self.etapa = etapa
        self.batch_id = batch_id

        spark.sql(DDL.format(table=table))
        # Para antes de gravar se a tabela existente não estiver no formato padrão.
        formato_atual = [(f.name, f.dataType) for f in spark.table(table).schema]
        if formato_atual != [(f.name, f.dataType) for f in LOG_SCHEMA]:
            raise RuntimeError(f"{table} está fora do formato padrão: {formato_atual}")

    def registrar(self, janela, status, iniciado_em, registros=None, paginas=None, duplicados=None,
                  arquivo=None, sha256=None, mensagem=None) -> None:
        """Grava uma linha no log. Status: SUCESSO, PARCIAL, SEM_DADOS ou FALHA."""
        finalizado_em = datetime.now(timezone.utc)
        linha = {
            "batch_id": self.batch_id, "fonte": self.fonte, "etapa": self.etapa, "janela": janela,
            "status": status, "registros": registros, "paginas": paginas, "duplicados": duplicados,
            "arquivo": arquivo, "sha256": sha256, "duracao_s": (finalizado_em - iniciado_em).total_seconds(),
            "iniciado_em": iniciado_em, "finalizado_em": finalizado_em,
            "mensagem": str(mensagem)[:1000] if mensagem else None,
        }
        self.spark.createDataFrame([linha], LOG_SCHEMA).write.mode("append").saveAsTable(self.table)
