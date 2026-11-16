# Databricks notebook source
# MAGIC %md
# MAGIC # GEO_DQ_001 02_geosampa_silver_quality
# MAGIC
# MAGIC ## 01. Identificação
# MAGIC
# MAGIC | Campo | Valor |
# MAGIC |---|---|
# MAGIC | Identificador | GEO_DQ_001 |
# MAGIC | Nome | 02_geosampa_silver_quality |
# MAGIC | Projeto | ChuvaSegura |
# MAGIC | Domínio de negócio | Ocorrências de alagamento (GeoSampa) |
# MAGIC | Camada | Qualidade |
# MAGIC | Linguagem | Python |
# MAGIC | Responsável técnico | juniorOnData |
# MAGIC | Responsável funcional | A definir |
# MAGIC | Status | Produção |
# MAGIC | Versão | 1.1 |
# MAGIC | Criticidade | Alta |
# MAGIC | Caminho no repositório | `src/geosampa/quality/02_geosampa_silver_quality.py` |
# MAGIC
# MAGIC ## 02. Objetivo
# MAGIC
# MAGIC Aplicar as regras de qualidade da Silver do GeoSampa, gravar os resultados e parar o job quando uma regra de severidade ERRO for reprovada, impedindo que a Gold seja carregada com dados ruins.
# MAGIC
# MAGIC ## 03. Contexto funcional
# MAGIC
# MAGIC É o ponto de controle entre a Silver e a Gold. Garante que as análises usem apenas ocorrências identificadas, datadas e localizadas.
# MAGIC
# MAGIC ## 04. Arquitetura e fluxo
# MAGIC
# MAGIC | Posição | Item |
# MAGIC |---|---|
# MAGIC | Etapa anterior | `02_geosampa_flooding_events_silver` |
# MAGIC | Etapa atual | Avalia `fiap_analytics.silver.geosampa_flooding_events` |
# MAGIC | Etapa posterior | `03_dim_geosampa_type_flooding_events` e `04_fact_geosampa_flooding_events` (só rodam se aprovada) |
# MAGIC
# MAGIC ## 05. Dependências
# MAGIC
# MAGIC | Tipo | Item | Uso |
# MAGIC |---|---|---|
# MAGIC | Módulo | `src/common/data_quality.py` | Regras prontas, execução e gravação |
# MAGIC | Tabela | `fiap_analytics.silver.geosampa_flooding_events` | Tabela avaliada |
# MAGIC | Tabela | `fiap_analytics.bronze.geosampa_history_flooding_events` | Conciliação de linhas e regra de `name` |
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
# MAGIC | `fiap_analytics.silver.geosampa_flooding_events` | Delta | Silver do GeoSampa | Completa | Diária (job) | `_source_year` + `nr_id_ocorrencia` | Nenhum |
# MAGIC | `fiap_analytics.bronze.geosampa_history_flooding_events` | Delta | Bronze do GeoSampa | Completa | Diária (job) | Não se aplica | `name IS NOT NULL` na regra de `name` |
# MAGIC
# MAGIC ## 08. Processamento técnico
# MAGIC
# MAGIC | Etapa | Linguagem | Célula | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | E01 | PY | Configuração | Importa o módulo de qualidade e define a tabela de resultados, a fonte, a etapa e as tabelas avaliadas. |
# MAGIC | E02 | PY | Regras | Lista das 14 regras da Silver (9 ERRO e 5 AVISO). |
# MAGIC | E03 | PY | Execução | Executa as regras, grava os resultados e mostra a tabela de resultados. |
# MAGIC | E04 | PY | Validação | Para a tarefa se alguma regra ERRO foi reprovada. |
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
# MAGIC Regras DQ_GEO_SLV_001 a DQ_GEO_SLV_014 do catálogo de regras de qualidade (`docs/02_catalogos/catalogo_regras_de_qualidade.csv`). ERRO: tabela não vazia, mesmas linhas da Bronze, ano e identificador preenchidos e únicos, data preenchida e no período, SRID 31983, ponto na área de São Paulo. AVISO: `name` repetido no ano, data fora do ano do arquivo, mais de 1% sem coordenada ou sem subprefeitura, mais de 5% do tipo `Outros`. Uma regra que não consegue rodar conta como reprovada.
# MAGIC
# MAGIC ## 12. Tratamento de erros
# MAGIC
# MAGIC | Situação | Comportamento | Registro | Recuperação |
# MAGIC |---|---|---|---|
# MAGIC | Regra ERRO reprovada | Grava os resultados e para a tarefa (`RuntimeError` com a lista das regras) | `geosampa_data_quality_log` com `status = FALHA` | Corrigir a causa nos dados ou no código e rodar o job de novo |
# MAGIC | Silver inexistente | Todas as regras da Silver reprovam | `mensagem` com `TABLE_OR_VIEW_NOT_FOUND` | Rodar antes `02_geosampa_flooding_events_silver` |
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
# MAGIC | Job e tarefa | `ChuvaSegura` (`jobs/chuvasegura_geosampa.job.yml`), tarefa `quality_silver_geosampa` |
# MAGIC | Frequência | Diária, 09:00:30 (America/Sao_Paulo) |
# MAGIC | SLA | A definir |
# MAGIC | Métricas | Regras reprovadas por severidade |
# MAGIC | Logs | `fiap_analytics.bronze.geosampa_data_quality_log` (etapa `silver`) |
# MAGIC | Alertas | Não configurado (recomendação: notificação de falha no job) |
# MAGIC | Reprocessamento | Rodar o notebook de novo; cada execução grava um novo `batch_id` |
# MAGIC
# MAGIC ## 15. Histórico de alterações
# MAGIC
# MAGIC | Versão | Data | Responsável | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | 1.0 | 2026-11-16 | juniorOnData | Criação das regras de qualidade da Silver (PR #14) |
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
from common.data_quality import DataQuality, condicao, consulta, nao_nulo, nao_vazia, unico  # noqa: E402

DQ_TABLE = "fiap_analytics.bronze.geosampa_data_quality_log"
FONTE = "geosampa"
ETAPA = "silver"
BATCH_ID = str(uuid.uuid4())

SILVER = "fiap_analytics.silver.geosampa_flooding_events"
BRONZE = "fiap_analytics.bronze.geosampa_history_flooding_events"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras

# COMMAND ----------

# [PY] E02 Regras
regras = [
    # Volume e completude
    nao_vazia(SILVER),
    consulta(
        "linhas_iguais_bronze", SILVER, "A Silver tem as mesmas linhas da Bronze (nenhuma perdida ou duplicada nos joins)",
        f"""SELECT ABS((SELECT COUNT(*) FROM {SILVER}) - (SELECT COUNT(*) FROM {BRONZE})) AS falhas,
                   (SELECT COUNT(*) FROM {BRONZE}) AS total""",
    ),

    # Identificação da ocorrência
    nao_nulo(SILVER, "_source_year"),
    nao_nulo(SILVER, "nr_id_ocorrencia"),
    unico(SILVER, ["_source_year", "nr_id_ocorrencia"], nome="ocorrencia_unica_no_ano"),
    unico(BRONZE, ["_source_year", "name"], severidade="AVISO", filtro="name IS NOT NULL",
          nome="name_unico_no_ano"),

    # Data
    nao_nulo(SILVER, "dt_ocorrencia"),
    condicao("data_no_periodo", SILVER, "Data entre 2010-01-01 (início da dim_tempo) e hoje",
             "dt_ocorrencia < DATE'2010-01-01' OR dt_ocorrencia > current_date()"),
    condicao("data_no_ano_do_arquivo", SILVER, "Ano da data igual ao ano do arquivo de origem",
             "year(dt_ocorrencia) <> _source_year", severidade="AVISO"),

    # Localização
    condicao("srid_31983", SILVER, "Geometria em SIRGAS 2000 / UTM 23S (EPSG 31983), o sistema que a Silver converte",
             "_source_srs_id <> 31983"),
    nao_nulo(SILVER, "nr_latitude", severidade="AVISO", limite=0.01),
    condicao("ponto_na_area_de_sao_paulo", SILVER, "Coordenada dentro da área de São Paulo e entorno",
             "nr_latitude NOT BETWEEN -24.1 AND -23.3 OR nr_longitude NOT BETWEEN -47.0 AND -46.3"),

    # Classificação
    nao_nulo(SILVER, "nm_subprefeitura", severidade="AVISO", limite=0.01),
    condicao("tipo_outros", SILVER, "Ocorrências classificadas como 'Outros' (tipo desconhecido ou vazio)",
             "dc_tipo_ocorrencia = 'Outros'", severidade="AVISO", limite=0.05),
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
