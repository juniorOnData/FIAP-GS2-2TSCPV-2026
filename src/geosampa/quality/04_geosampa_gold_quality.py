# Databricks notebook source
# MAGIC %md
# MAGIC # GEO_DQ_002 04_geosampa_gold_quality
# MAGIC
# MAGIC ## 01. Identificação
# MAGIC
# MAGIC | Campo | Valor |
# MAGIC |---|---|
# MAGIC | Identificador | GEO_DQ_002 |
# MAGIC | Nome | 04_geosampa_gold_quality |
# MAGIC | Projeto | ChuvaSegura |
# MAGIC | Domínio de negócio | Ocorrências de alagamento (GeoSampa) |
# MAGIC | Camada | Qualidade |
# MAGIC | Linguagem | Python |
# MAGIC | Responsável técnico | juniorOnData |
# MAGIC | Responsável funcional | A definir |
# MAGIC | Status | Produção |
# MAGIC | Versão | 1.1 |
# MAGIC | Criticidade | Alta |
# MAGIC | Caminho no repositório | `src/geosampa/quality/04_geosampa_gold_quality.py` |
# MAGIC
# MAGIC ## 02. Objetivo
# MAGIC
# MAGIC Aplicar as regras de qualidade da Gold do GeoSampa e da `dim_tempo`, gravar os resultados e reprovar o job quando uma regra ERRO falhar.
# MAGIC
# MAGIC ## 03. Contexto funcional
# MAGIC
# MAGIC Garante que o modelo dimensional entregue às análises está íntegro: uma linha por ocorrência, chaves válidas e dimensões consistentes.
# MAGIC
# MAGIC ## 04. Arquitetura e fluxo
# MAGIC
# MAGIC | Posição | Item |
# MAGIC |---|---|
# MAGIC | Etapa anterior | `04_fact_geosampa_flooding_events` |
# MAGIC | Etapa atual | Avalia a fato, as dimensões e a `dim_tempo` |
# MAGIC | Etapa posterior | Fim do job |
# MAGIC
# MAGIC ## 05. Dependências
# MAGIC
# MAGIC | Tipo | Item | Uso |
# MAGIC |---|---|---|
# MAGIC | Módulo | `src/common/data_quality.py` | Regras prontas, execução e gravação |
# MAGIC | Tabela | Fato, dimensões do GeoSampa, `dim_tempo` e Silver | Tabelas avaliadas e conciliação |
# MAGIC | Permissão | USE CATALOG em `fiap_analytics`; SELECT nas tabelas avaliadas; CREATE TABLE e MODIFY em `fiap_analytics.bronze` (tabela de resultados) | Ler as tabelas e gravar os resultados |
# MAGIC
# MAGIC ## 06. Parâmetros de entrada
# MAGIC
# MAGIC Não aplicável. As regras estão no próprio notebook; não há widgets.
# MAGIC
# MAGIC ## 07. Fontes de dados
# MAGIC
# MAGIC | Fonte | Formato | Origem | Tipo de leitura | Frequência | Chave | Filtros |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | Fato, dimensões e `dim_tempo` em `fiap_analytics.gold`; Silver do GeoSampa | Delta | Gold e Silver | Completa | Diária (job) | Chaves de cada tabela | Nenhum |
# MAGIC
# MAGIC ## 08. Processamento técnico
# MAGIC
# MAGIC | Etapa | Linguagem | Célula | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | E01 | PY | Configuração | Importa o módulo de qualidade e define a tabela de resultados, a fonte, a etapa e as tabelas avaliadas. |
# MAGIC | E02 | PY | Regras | Função do membro desconhecido e lista das 17 regras da Gold (15 ERRO e 2 AVISO). |
# MAGIC | E03 | PY | Execução | Executa as regras, grava os resultados e mostra a tabela de resultados. |
# MAGIC | E04 | PY | Validação | Reprova a tarefa se alguma regra ERRO foi reprovada. |
# MAGIC
# MAGIC ## 09. Regras de negócio
# MAGIC
# MAGIC Não aplicável. O notebook aplica regras de qualidade, catalogadas na seção 11 e em `docs/02_catalogos/catalogo_regras_de_qualidade.csv`, e não regras de negócio.
# MAGIC
# MAGIC ## 10. Saídas de dados
# MAGIC
# MAGIC | Destino | Formato | Granularidade | Chave | Modo de escrita |
# MAGIC |---|---|---|---|---|
# MAGIC | `fiap_analytics.bronze.geosampa_data_quality_log` | Delta | Uma linha por regra e execução | `batch_id` + `tabela` + `regra` | `append` |
# MAGIC
# MAGIC As colunas de cada tabela estão no dicionário de dados (`docs/02_catalogos/dicionario_de_dados.csv`).
# MAGIC
# MAGIC ## 11. Qualidade dos dados
# MAGIC
# MAGIC Regras DQ_GEO_GLD_001 a DQ_GEO_GLD_017 do catálogo de regras de qualidade (`docs/02_catalogos/catalogo_regras_de_qualidade.csv`). ERRO: fato não vazia, mesmas linhas da Silver por ano, ocorrência única, integridade com a `dim_tempo` e as dimensões, ponto na área de São Paulo, chaves únicas e membro `-1` nas dimensões, `dim_tempo` sem chave repetida nem dia faltando. AVISO: mais de 1% das ocorrências com subprefeitura `-1` e ocorrências com tipo `-1`.
# MAGIC
# MAGIC ## 12. Tratamento de erros
# MAGIC
# MAGIC | Situação | Comportamento | Registro | Recuperação |
# MAGIC |---|---|---|---|
# MAGIC | Regra ERRO reprovada | Grava os resultados e reprova a tarefa | `geosampa_data_quality_log` com `status = FALHA` | Corrigir a causa e rodar o job de novo |
# MAGIC
# MAGIC ## 13. Segurança e governança
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Classificação dos dados | Pública (dados abertos da fonte; confirmar os termos de uso) |
# MAGIC | Dados sensíveis | Nenhum. As ocorrências têm coordenadas de logradouro, sem identificação de pessoas |
# MAGIC | Credenciais | Nenhuma |
# MAGIC | Permissões necessárias | USE CATALOG em `fiap_analytics`; SELECT nas tabelas avaliadas; CREATE TABLE e MODIFY em `fiap_analytics.bronze` (tabela de resultados) |
# MAGIC | Políticas aplicáveis | Termos de uso dos dados abertos da Prefeitura de São Paulo (confirmar) |
# MAGIC
# MAGIC ## 14. Monitoramento e operação
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Job e tarefa | `ChuvaSegura` (`jobs/chuvasegura_geosampa.job.yml`), tarefa `quality_gold_geosampa` |
# MAGIC | Frequência | Diária, 09:00:30 (America/Sao_Paulo) |
# MAGIC | SLA | A definir |
# MAGIC | Métricas | Regras reprovadas por severidade |
# MAGIC | Logs | `fiap_analytics.bronze.geosampa_data_quality_log` (etapa `gold`) |
# MAGIC | Alertas | Não configurado (recomendação: notificação de falha no job) |
# MAGIC | Reprocessamento | Rodar o notebook de novo |
# MAGIC
# MAGIC ## 15. Histórico de alterações
# MAGIC
# MAGIC | Versão | Data | Responsável | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | 1.0 | 2026-11-16 | juniorOnData | Criação das regras de qualidade da Gold (PR #14) |
# MAGIC | 1.1 | 2026-11-16 | juniorOnData | Documentação no padrão oficial (PR #15) |

# COMMAND ----------

# [PY] E01 Configuração
import os
import sys
import uuid

# Módulos de src/common. Sobe pelas pastas até achar src/ para importar.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common.data_quality import DataQuality, condicao, consulta, existe_em, nao_vazia, unico  # noqa: E402

DQ_TABLE = "fiap_analytics.bronze.geosampa_data_quality_log"
FONTE = "geosampa"
ETAPA = "gold"
BATCH_ID = str(uuid.uuid4())

SILVER = "fiap_analytics.silver.geosampa_flooding_events"
FACT = "fiap_analytics.gold.fact_geosampa_flooding_events"
DIM_TEMPO = "fiap_analytics.gold.dim_tempo"
DIM_TIPO = "fiap_analytics.gold.dim_geosampa_type_flooding_events"
DIM_SUBPREFEITURA = "fiap_analytics.gold.dim_geosampa_subprefecture_area"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras

# COMMAND ----------

# [PY] E02 Regras
def membro_desconhecido(dim, chave):
    return consulta(
        "membro_desconhecido", dim, "A dimensão tem exatamente uma linha -1 (não informado)",
        f"SELECT CASE WHEN COUNT_IF({chave} = -1) = 1 THEN 0 ELSE 1 END AS falhas, 1 AS total FROM {dim}",
    )


regras = [
    # Fato: volume e conciliação com a Silver
    nao_vazia(FACT),
    consulta(
        "linhas_iguais_silver_por_ano", FACT, "Por ano de origem, a fato tem as mesmas linhas da Silver",
        f"""SELECT COUNT_IF(NOT (s.n <=> g.n)) AS falhas, COUNT(*) AS total
            FROM (SELECT _source_year AS ano, COUNT(*) AS n FROM {SILVER} GROUP BY 1) s
            FULL OUTER JOIN (SELECT NUM_ANO_ORIGEM AS ano, COUNT(*) AS n FROM {FACT} GROUP BY 1) g
              ON s.ano <=> g.ano""",
    ),
    unico(FACT, ["NUM_ANO_ORIGEM", "NUM_ID_OCORRENCIA"], nome="ocorrencia_unica"),

    # Fato: integridade referencial (chave nula também conta como falha)
    existe_em(FACT, "COD_DIM_TEMPO", DIM_TEMPO),
    existe_em(FACT, "COD_DIM_OCORRENCIA", DIM_TIPO),
    existe_em(FACT, "COD_DIM_SUBPREFEITURA", DIM_SUBPREFEITURA),

    # Fato: ocorrências que caíram no membro desconhecido
    condicao("subprefeitura_nao_informada", FACT, "Ocorrências sem subprefeitura identificada (-1)",
             "COD_DIM_SUBPREFEITURA = -1", severidade="AVISO", limite=0.01),
    condicao("tipo_nao_informado", FACT, "Ocorrências sem tipo identificado (-1)",
             "COD_DIM_OCORRENCIA = -1", severidade="AVISO"),
    condicao("ponto_na_area_de_sao_paulo", FACT, "Coordenada dentro da área de São Paulo e entorno",
             "NUM_LATITUDE NOT BETWEEN -24.1 AND -23.3 OR NUM_LONGITUDE NOT BETWEEN -47.0 AND -46.3"),

    # Dimensões do GeoSampa
    unico(DIM_TIPO, ["COD_DIM_OCORRENCIA"]),
    unico(DIM_TIPO, ["DSC_TIPO_OCORRENCIA"]),
    membro_desconhecido(DIM_TIPO, "COD_DIM_OCORRENCIA"),
    unico(DIM_SUBPREFEITURA, ["COD_DIM_SUBPREFEITURA"]),
    unico(DIM_SUBPREFEITURA, ["SIG_SUBPREFEITURA"]),
    membro_desconhecido(DIM_SUBPREFEITURA, "COD_DIM_SUBPREFEITURA"),

    # dim_tempo
    unico(DIM_TEMPO, ["COD_DIM_TEMPO"]),
    consulta(
        "sem_dias_faltando", DIM_TEMPO, "Nenhum dia faltando entre o primeiro e o último dia da dimensão",
        f"SELECT datediff(MAX(DAT_DIA), MIN(DAT_DIA)) + 1 - COUNT(*) AS falhas, COUNT(*) AS total FROM {DIM_TEMPO}",
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução

# COMMAND ----------

# [PY] E03 Execução
dq = DataQuality(spark, DQ_TABLE, FONTE, ETAPA, BATCH_ID)
resultados = dq.executar(regras)
display(dq.resultados_df(resultados))

# COMMAND ----------

# [PY] E04 Validação
dq.validar(resultados)
