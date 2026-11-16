"""Etapas de qualidade de dados do projeto ChuvaSegura.

Cada regra é uma consulta SQL que devolve uma linha com duas colunas: `falhas` (linhas que violam a regra)
e `total` (linhas avaliadas). A regra passa quando `falhas / total` não passa do `limite` (com limite 0,
nenhuma falha é aceita). Regras de severidade ERRO que falham param a tarefa; AVISO só registra.

Os resultados de cada execução vão para a tabela de qualidade da origem (por exemplo,
fiap_analytics.bronze.geosampa_data_quality_log), uma linha por regra.

Uso nos notebooks:

    dq = DataQuality(spark, DQ_TABLE, FONTE, ETAPA, BATCH_ID)
    resultados = dq.executar([
        nao_vazia(SILVER),
        nao_nulo(SILVER, "dt_ocorrencia"),
        unico(SILVER, ["_source_year", "nr_id_ocorrencia"]),
    ])
    display(dq.resultados_df(resultados))
    dq.validar(resultados)  # levanta erro se alguma regra ERRO falhou
"""
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import types as T

SEVERIDADES = ("ERRO", "AVISO")

DQ_SCHEMA = T.StructType([
    T.StructField("batch_id", T.StringType()),
    T.StructField("fonte", T.StringType()),
    T.StructField("etapa", T.StringType()),
    T.StructField("tabela", T.StringType()),
    T.StructField("regra", T.StringType()),
    T.StructField("descricao", T.StringType()),
    T.StructField("severidade", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("falhas", T.LongType()),
    T.StructField("total", T.LongType()),
    T.StructField("percentual", T.DoubleType()),
    T.StructField("limite", T.DoubleType()),
    T.StructField("executado_em", T.TimestampType()),
    T.StructField("mensagem", T.StringType()),
])

DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  batch_id     STRING    COMMENT 'Identificador da execução',
  fonte        STRING    COMMENT 'Origem dos dados',
  etapa        STRING    COMMENT 'Camada avaliada (bronze, silver ou gold)',
  tabela       STRING    COMMENT 'Tabela avaliada',
  regra        STRING    COMMENT 'Nome da regra',
  descricao    STRING    COMMENT 'O que a regra verifica',
  severidade   STRING    COMMENT 'ERRO (para a tarefa) ou AVISO (só registra)',
  status       STRING    COMMENT 'SUCESSO ou FALHA',
  falhas       BIGINT    COMMENT 'Linhas que violam a regra',
  total        BIGINT    COMMENT 'Linhas avaliadas',
  percentual   DOUBLE    COMMENT 'falhas / total',
  limite       DOUBLE    COMMENT 'Proporção máxima de falhas aceita (0 = nenhuma)',
  executado_em TIMESTAMP,
  mensagem     STRING
) COMMENT 'Resultados das regras de qualidade de dados do projeto ChuvaSegura, uma tabela por origem'
"""


@dataclass(frozen=True)
class Regra:
    nome: str
    tabela: str
    descricao: str
    consulta: str  # SQL que devolve uma linha com as colunas falhas e total
    severidade: str = "ERRO"
    limite: float = 0.0

    def __post_init__(self):
        if self.severidade not in SEVERIDADES:
            raise ValueError(f"Severidade inválida em {self.nome}: {self.severidade}")
        if not 0 <= self.limite <= 1:
            raise ValueError(f"Limite inválido em {self.nome}: {self.limite} (use uma proporção entre 0 e 1)")


def _where(filtro):
    return f"WHERE {filtro}" if filtro else ""


# ---------- Regras prontas ----------

def nao_vazia(tabela, severidade="ERRO"):
    return Regra(
        nome="tabela_nao_vazia", tabela=tabela, severidade=severidade,
        descricao="A tabela tem pelo menos uma linha",
        consulta=f"SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS falhas, 1 AS total FROM {tabela}",
    )


def nao_nulo(tabela, coluna, severidade="ERRO", limite=0.0, filtro=None):
    return Regra(
        nome=f"{coluna}_nao_nulo", tabela=tabela, severidade=severidade, limite=limite,
        descricao=f"{coluna} preenchido",
        consulta=f"SELECT COUNT_IF({coluna} IS NULL) AS falhas, COUNT(*) AS total FROM {tabela} {_where(filtro)}",
    )


def condicao(nome, tabela, descricao, falha, severidade="ERRO", limite=0.0, filtro=None):
    """`falha` é uma expressão SQL verdadeira para as linhas que violam a regra (nulo não conta como falha)."""
    return Regra(
        nome=nome, tabela=tabela, severidade=severidade, limite=limite, descricao=descricao,
        consulta=f"SELECT COUNT_IF({falha}) AS falhas, COUNT(*) AS total FROM {tabela} {_where(filtro)}",
    )


def unico(tabela, colunas, severidade="ERRO", filtro=None, nome=None):
    """Falhas = linhas excedentes (uma chave repetida 3 vezes conta 2 falhas)."""
    chave = ", ".join(colunas)
    return Regra(
        nome=nome or f"{'_'.join(c.lower() for c in colunas)}_unico", tabela=tabela, severidade=severidade,
        descricao=f"Sem repetição de ({chave})",
        consulta=(
            f"SELECT COALESCE(SUM(n - 1), 0) AS falhas, (SELECT COUNT(*) FROM {tabela} {_where(filtro)}) AS total "
            f"FROM (SELECT COUNT(*) AS n FROM {tabela} {_where(filtro)} GROUP BY {chave} HAVING COUNT(*) > 1)"
        ),
    )


def existe_em(tabela, coluna, tabela_ref, coluna_ref=None, severidade="ERRO"):
    """Integridade referencial: todo valor de `coluna` existe em `tabela_ref`. Valor nulo conta como falha."""
    coluna_ref = coluna_ref or coluna
    return Regra(
        nome=f"{coluna.lower()}_existe_em_{tabela_ref.split('.')[-1]}", tabela=tabela, severidade=severidade,
        descricao=f"Todo {coluna} existe em {tabela_ref}.{coluna_ref}",
        consulta=(
            f"SELECT COUNT_IF(r.{coluna_ref} IS NULL) AS falhas, COUNT(*) AS total FROM {tabela} f "
            f"LEFT JOIN (SELECT DISTINCT {coluna_ref} FROM {tabela_ref}) r ON f.{coluna} = r.{coluna_ref}"
        ),
    )


def consulta(nome, tabela, descricao, sql, severidade="ERRO", limite=0.0):
    """Regra livre: `sql` precisa devolver uma linha com as colunas falhas e total."""
    return Regra(nome=nome, tabela=tabela, descricao=descricao, consulta=sql, severidade=severidade, limite=limite)


# ---------- Execução ----------

class DataQuality:
    """Executa as regras de uma etapa, grava os resultados e para a tarefa se alguma regra ERRO falhar."""

    def __init__(self, spark, table: str, fonte: str, etapa: str, batch_id: str):
        self.spark = spark
        self.table = table
        self.fonte = fonte
        self.etapa = etapa
        self.batch_id = batch_id

        spark.sql(DDL.format(table=table))
        formato_atual = [(f.name, f.dataType) for f in spark.table(table).schema]
        if formato_atual != [(f.name, f.dataType) for f in DQ_SCHEMA]:
            raise RuntimeError(f"{table} está fora do formato padrão: {formato_atual}")

    def executar(self, regras) -> list:
        nomes = [(r.tabela, r.nome) for r in regras]
        repetidas = {n for n in nomes if nomes.count(n) > 1}
        if repetidas:
            raise ValueError(f"Regras com o mesmo nome na mesma tabela: {sorted(repetidas)}")

        resultados = []
        for regra in regras:
            executado_em = datetime.now(timezone.utc)
            falhas = total = percentual = None
            mensagem = None
            try:
                linha = self.spark.sql(regra.consulta).first()
                falhas, total = int(linha["falhas"] or 0), int(linha["total"] or 0)
                percentual = falhas / total if total else 0.0
                passou = falhas == 0 if regra.limite == 0 else percentual <= regra.limite
            except Exception as exc:  # regra que não roda conta como falha, nunca como sucesso
                passou = False
                mensagem = f"erro ao executar a regra: {exc}"[:1000]
            resultados.append({
                "batch_id": self.batch_id, "fonte": self.fonte, "etapa": self.etapa,
                "tabela": regra.tabela, "regra": regra.nome, "descricao": regra.descricao,
                "severidade": regra.severidade, "status": "SUCESSO" if passou else "FALHA",
                "falhas": falhas, "total": total, "percentual": percentual, "limite": float(regra.limite),
                "executado_em": executado_em, "mensagem": mensagem,
            })
            marca = "ok   " if passou else ("ERRO " if regra.severidade == "ERRO" else "AVISO")
            detalhe = mensagem or f"{falhas}/{total} ({percentual:.2%}, limite {regra.limite:.2%})"
            print(f"[{marca}] {regra.tabela.split('.')[-1]}.{regra.nome}: {detalhe}")

        self.spark.createDataFrame(resultados, DQ_SCHEMA).write.mode("append").saveAsTable(self.table)
        return resultados

    def resultados_df(self, resultados):
        return self.spark.createDataFrame(resultados, DQ_SCHEMA).select(
            "tabela", "regra", "severidade", "status", "falhas", "total", "percentual", "limite", "mensagem")

    @staticmethod
    def validar(resultados) -> None:
        erros = [r for r in resultados if r["status"] == "FALHA" and r["severidade"] == "ERRO"]
        avisos = [r for r in resultados if r["status"] == "FALHA" and r["severidade"] == "AVISO"]
        print(f"Qualidade: {len(resultados)} regras | {len(erros)} erros | {len(avisos)} avisos")
        if erros:
            lista = "; ".join(f"{r['tabela'].split('.')[-1]}.{r['regra']}" for r in erros)
            raise RuntimeError(f"{len(erros)} regra(s) de severidade ERRO falharam: {lista}")
