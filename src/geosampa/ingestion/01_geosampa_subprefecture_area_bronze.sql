-- Databricks notebook source
-- MAGIC %md
-- MAGIC # GEO_BRZ_001 01_geosampa_subprefecture_area_bronze
-- MAGIC
-- MAGIC ## 01. Identificação
-- MAGIC
-- MAGIC | Campo | Valor |
-- MAGIC |---|---|
-- MAGIC | Identificador | GEO_BRZ_001 |
-- MAGIC | Nome | 01_geosampa_subprefecture_area_bronze |
-- MAGIC | Projeto | ChuvaSegura |
-- MAGIC | Domínio de negócio | Ocorrências de alagamento (GeoSampa) |
-- MAGIC | Camada | Bronze |
-- MAGIC | Linguagem | SQL |
-- MAGIC | Responsável técnico | juniorOnData |
-- MAGIC | Responsável funcional | A definir |
-- MAGIC | Status | Produção |
-- MAGIC | Versão | 1.4 |
-- MAGIC | Criticidade | Média |
-- MAGIC | Caminho no repositório | `src/geosampa/ingestion/01_geosampa_subprefecture_area_bronze.sql` |
-- MAGIC
-- MAGIC ## 02. Objetivo
-- MAGIC
-- MAGIC Recriar a tabela de referência das 33 unidades administrativas de São Paulo (32 subprefeituras e a Secretaria de Coordenação das Subprefeituras) com o agrupamento do Anexo IX da ata 41/14 e a sigla usada nas ocorrências do GeoSampa. O resultado é a base da dimensão de subprefeituras e do relacionamento das ocorrências com a subprefeitura.
-- MAGIC
-- MAGIC ## 03. Contexto funcional
-- MAGIC
-- MAGIC As ocorrências do GeoSampa trazem a subprefeitura como texto livre, e em alguns casos com sigla diferente da oficial (Freguesia/Brasilândia aparece como FO). Esta tabela é a referência oficial que padroniza a subprefeitura de cada ocorrência, permitindo análises por região da cidade.
-- MAGIC
-- MAGIC ## 04. Arquitetura e fluxo
-- MAGIC
-- MAGIC | Posição | Item |
-- MAGIC |---|---|
-- MAGIC | Etapa anterior | Nenhuma (dados de referência no próprio notebook) |
-- MAGIC | Etapa atual | Recria `fiap_analytics.bronze.geosampa_subprefecture_area` |
-- MAGIC | Etapa posterior | `02_geosampa_flooding_events_silver` e `03_dim_geosampa_subprefecture_area` |
-- MAGIC
-- MAGIC ## 05. Dependências
-- MAGIC
-- MAGIC | Tipo | Item | Uso |
-- MAGIC |---|---|---|
-- MAGIC | Arquivo | anexo_ix_ata_41-14_1468935749.pdf | Fonte dos dados digitados no `INSERT` (Anexo IX, Relação de Agrupamentos) |
-- MAGIC | Permissão | USE CATALOG em `fiap_analytics`; USE SCHEMA, CREATE TABLE e MODIFY em `fiap_analytics.bronze` | Criar e gravar a tabela |
-- MAGIC
-- MAGIC ## 06. Parâmetros de entrada
-- MAGIC
-- MAGIC Não aplicável. O notebook não recebe parâmetros: os dados de referência estão no próprio código.
-- MAGIC
-- MAGIC ## 07. Fontes de dados
-- MAGIC
-- MAGIC | Fonte | Formato | Origem | Tipo de leitura | Frequência | Chave | Filtros |
-- MAGIC |---|---|---|---|---|---|---|
-- MAGIC | Anexo IX da ata 41/14 (valores no `INSERT`) | SQL `VALUES` | Prefeitura de São Paulo | Completa | Quando o Anexo IX mudar | `sg_subprefeitura` | Nenhum |
-- MAGIC
-- MAGIC ## 08. Processamento técnico
-- MAGIC
-- MAGIC | Etapa | Linguagem | Célula | Descrição |
-- MAGIC |---|---|---|---|
-- MAGIC | E01 | SQL | Criação da tabela | `CREATE OR REPLACE TABLE` com as colunas comentadas e a chave primária informativa `sg_subprefeitura`. |
-- MAGIC | E02 | SQL | Carga das unidades | `INSERT` das 33 unidades com sigla, nome, endereço, agrupamento, tipo e sigla usada no GeoSampa. |
-- MAGIC | E03 | SQL | Consulta do resultado | Mostra a tabela gravada para conferência. |
-- MAGIC
-- MAGIC ## 09. Regras de negócio
-- MAGIC
-- MAGIC | ID | Regra | Lógica | Comportamento esperado |
-- MAGIC |---|---|---|---|
-- MAGIC | RN_GEO_003 | Unidades do Anexo IX | 32 subprefeituras (tipo `SUBPREFEITURA`) e a SPUA/NEC (tipo `SECRETARIA`), com o agrupamento I, II, III ou IV do Anexo IX. Sapopemba usa a sigla `SB`. Freguesia/Brasilândia tem sigla oficial `FB` e sigla `FO` no GeoSampa. | Uma linha por unidade; `sg_subprefeitura_geosampa` preenchida para todas as subprefeituras e nula para a secretaria. |
-- MAGIC
-- MAGIC ## 10. Saídas de dados
-- MAGIC
-- MAGIC | Destino | Formato | Granularidade | Chave | Modo de escrita |
-- MAGIC |---|---|---|---|---|
-- MAGIC | `fiap_analytics.bronze.geosampa_subprefecture_area` | Delta | Uma linha por unidade administrativa | `sg_subprefeitura` | `CREATE OR REPLACE` (recriada a cada execução) |
-- MAGIC
-- MAGIC As colunas de cada tabela estão no dicionário de dados (`docs/02_catalogos/dicionario_de_dados.csv`).
-- MAGIC
-- MAGIC ## 11. Qualidade dos dados
-- MAGIC
-- MAGIC Sem validação automática própria. A chave primária é informativa (o Unity Catalog não a impõe). A unicidade da sigla é verificada na Gold pela regra DQ_GEO_GLD_014 (`SIG_SUBPREFEITURA` único na dimensão).
-- MAGIC
-- MAGIC ## 12. Tratamento de erros
-- MAGIC
-- MAGIC | Situação | Comportamento | Registro | Recuperação |
-- MAGIC |---|---|---|---|
-- MAGIC | Erro de sintaxe ou permissão | A célula falha e a tarefa para | Saída da tarefa no Lakeflow Jobs | Corrigir e rodar de novo: a tabela é recriada do zero |
-- MAGIC
-- MAGIC ## 13. Segurança e governança
-- MAGIC
-- MAGIC | Item | Valor |
-- MAGIC |---|---|
-- MAGIC | Classificação dos dados | Pública (dados abertos da fonte; confirmar os termos de uso) |
-- MAGIC | Dados sensíveis | Nenhum. As ocorrências têm coordenadas de logradouro, sem identificação de pessoas |
-- MAGIC | Credenciais | Nenhuma |
-- MAGIC | Permissões necessárias | USE CATALOG em `fiap_analytics`; USE SCHEMA, CREATE TABLE e MODIFY em `fiap_analytics.bronze` |
-- MAGIC | Políticas aplicáveis | Termos de uso dos dados abertos da Prefeitura de São Paulo (confirmar) |
-- MAGIC
-- MAGIC ## 14. Monitoramento e operação
-- MAGIC
-- MAGIC | Item | Valor |
-- MAGIC |---|---|
-- MAGIC | Job e tarefa | `ChuvaSegura` (`jobs/chuvasegura_geosampa.job.yml`), tarefa `ingestion_subprefecture_area` |
-- MAGIC | Frequência | Diária, 09:00:30 (America/Sao_Paulo) |
-- MAGIC | SLA | A definir |
-- MAGIC | Métricas | 33 linhas na tabela |
-- MAGIC | Logs | Não grava log de ingestão (dados de referência fixos) |
-- MAGIC | Alertas | Não configurado (recomendação: notificação de falha no job) |
-- MAGIC | Reprocessamento | Rodar o notebook de novo. Ver reprocessamento (`docs/04_operacao/reprocessamento.md`) |
-- MAGIC
-- MAGIC ## 15. Histórico de alterações
-- MAGIC
-- MAGIC | Versão | Data | Responsável | Descrição |
-- MAGIC |---|---|---|---|
-- MAGIC | 1.0 | 2026-11-06 | juniorOnData | Criação da tabela de subprefeituras (PR #2) |
-- MAGIC | 1.1 | 2026-11-06 | juniorOnData | Ajuste de caminho e da carga (PR #4) |
-- MAGIC | 1.2 | 2026-11-16 | juniorOnData | Sigla SB direto nos dados, sem UPDATEs (PR #8) |
-- MAGIC | 1.3 | 2026-11-16 | juniorOnData | Formato de código fonte e novo nome do arquivo (PR #9) |
-- MAGIC | 1.4 | 2026-11-16 | juniorOnData | Documentação no padrão oficial (PR #15) |

-- COMMAND ----------

-- [SQL] E01 Criação da tabela
CREATE OR REPLACE TABLE fiap_analytics.bronze.geosampa_subprefecture_area (
  sg_subprefeitura          STRING    NOT NULL COMMENT 'Sigla da unidade conforme o Anexo IX',
  nm_subprefeitura          STRING    COMMENT 'Nome da unidade conforme o Anexo IX',
  ds_endereco               STRING    COMMENT 'Endereco da unidade conforme o Anexo IX',
  cd_agrupamento            STRING    COMMENT 'Agrupamento do Anexo IX (I, II, III ou IV)',
  tp_unidade                STRING    COMMENT 'SUBPREFEITURA ou SECRETARIA',
  sg_subprefeitura_geosampa STRING    COMMENT 'Sigla usada nas ocorrencias do GeoSampa (FO para FB); a Silver relaciona as ocorrencias por ela',
  ds_arquivo_origem         STRING    COMMENT 'Arquivo de origem dos dados',
  dt_carga                  TIMESTAMP COMMENT 'Data e hora da carga',
  CONSTRAINT pk_dim_subprefecture_area PRIMARY KEY (sg_subprefeitura)
)
COMMENT 'Dimensao de subprefeituras de Sao Paulo, com agrupamentos, a partir do Anexo IX (Relacao de Agrupamentos)';

-- COMMAND ----------

-- [SQL] E02 Carga das unidades
INSERT INTO fiap_analytics.bronze.geosampa_subprefecture_area
  SELECT
    v.sg_subprefeitura,
    v.nm_subprefeitura,
    v.ds_endereco,
    v.cd_agrupamento,
    v.tp_unidade,
    v.sg_subprefeitura_geosampa,
    'anexo_ix_ata_41-14_1468935749.pdf' AS ds_arquivo_origem,
    current_timestamp() AS dt_carga
  FROM
    VALUES
      ('AD', 'CIDADE ADEMAR', 'Rua Yervant Kissajikian, 416', 'II', 'SUBPREFEITURA', 'AD'),
      ('AF', 'ARICANDUVA/VILA FORMOSA', 'Rua Eponina, 82', 'IV', 'SUBPREFEITURA', 'AF'),
      ('BT', 'BUTANTÃ', 'Rua Ulpiano da Costa Manso, 201', 'III', 'SUBPREFEITURA', 'BT'),
      ('CL', 'CAMPO LIMPO', 'Av. Nossa Senhora do Bom Conselho, 59', 'III', 'SUBPREFEITURA', 'CL'),
      ('CS', 'CAPELA DO SOCORRO', 'Rua Cassiano dos Santos, 499', 'II', 'SUBPREFEITURA', 'CS'),
      ('CV', 'CASA VERDE', 'Rua Baroré, 284', 'I', 'SUBPREFEITURA', 'CV'),
      ('CT', 'CIDADE TIRADENTES', 'Estrada do Iguatemi, 2.751', 'IV', 'SUBPREFEITURA', 'CT'),
      ('EM', 'ERMELINO MATARAZZO', 'Av. São Miguel, 5977', 'IV', 'SUBPREFEITURA', 'EM'),
      ('FB', 'FREGUESIA/BRASILÂNDIA', 'Rua João Marcelino Branco, 95', 'I', 'SUBPREFEITURA', 'FO'),
      ('G', 'GUAIANAZES', 'Rua Prof. Cosme Deodato Tadeu, 136', 'IV', 'SUBPREFEITURA', 'G'),
      ('IP', 'IPIRANGA', 'Rua Lino Coutinho, 444', 'II', 'SUBPREFEITURA', 'IP'),
      ('IQ', 'ITAQUERA', 'Rua Gregório Ramalho, 103', 'IV', 'SUBPREFEITURA', 'IQ'),
      ('IT', 'ITAIM PAULISTA', 'Av. Marechal Tito, 3012', 'IV', 'SUBPREFEITURA', 'IT'),
      ('JA', 'JABAQUARA', 'Av. Eng. Armando de Arruda Pereira,2979', 'II', 'SUBPREFEITURA', 'JA'),
      ('JT', 'JAÇANÃ/TREMEMBÉ', 'Av. Luiz Stamatis, 300', 'I', 'SUBPREFEITURA', 'JT'),
      ('LA', 'LAPA', 'Rua Guaicurus, 1.000', 'III', 'SUBPREFEITURA', 'LA'),
      ('MB', 'M''BOI MIRIM', 'Estrada do Rivieira, 394', 'II', 'SUBPREFEITURA', 'MB'),
      ('MG', 'V.MARIA/V.GUILHERME', 'Rua General Mendes, 111', 'I', 'SUBPREFEITURA', 'MG'),
      ('MO', 'MOÓCA', 'Rua Taquari, 549', 'III', 'SUBPREFEITURA', 'MO'),
      (
        'MP',
        'SÃO MIGUEL PAULISTA',
        'Rua Ana Flora Pinheiro de Souza, 76',
        'IV',
        'SUBPREFEITURA',
        'MP'
      ),
      ('PA', 'PARELHEIROS', 'Av. Sadamus Inoue, 190', 'II', 'SUBPREFEITURA', 'PA'),
      ('PE', 'PENHA', 'Rua Candapui, 492', 'IV', 'SUBPREFEITURA', 'PE'),
      ('PI', 'PINHEIROS', 'Av. das Nações Unidas, 7123', 'III', 'SUBPREFEITURA', 'PI'),
      ('PJ', 'PIRITUBA/JARAGUÁ', 'Rua Luiz Carneiro, 193', 'I', 'SUBPREFEITURA', 'PJ'),
      ('PR', 'PERUS', 'Av. Ylidio Figueiredo, 349', 'I', 'SUBPREFEITURA', 'PR'),
      ('SA', 'SANTO AMARO', 'Praça Floriano Peixoto, 54', 'II', 'SUBPREFEITURA', 'SA'),
      ('SB', 'SAPOPEMBA', 'Estrada do Oratório, 172', 'IV', 'SUBPREFEITURA', 'SB'),
      ('SE', 'SÉ', 'Av. do Estado, 900', 'III', 'SUBPREFEITURA', 'SE'),
      ('SM', 'SÃO MATEUS', 'Rua Ragueb Chohfi, 1500', 'IV', 'SUBPREFEITURA', 'SM'),
      ('ST', 'SANTANA', 'Av. Tucuruvi, 808', 'I', 'SUBPREFEITURA', 'ST'),
      ('VM', 'VILA MARIANA', 'Rua José de Magalhães, 500', 'III', 'SUBPREFEITURA', 'VM'),
      ('VP', 'VILA PRUDENTE', 'Estrada do Oratório, 172', 'IV', 'SUBPREFEITURA', 'VP'),
      (
        'SPUA/NEC',
        'SECRETARIA DE COORDENAÇÃO DAS SUBPREFEITURAS',
        'Rua do Bosque, 1088',
        'III',
        'SECRETARIA',
        CAST(NULL AS STRING)
      ) AS v(
      sg_subprefeitura,
      nm_subprefeitura,
      ds_endereco,
      cd_agrupamento,
      tp_unidade,
      sg_subprefeitura_geosampa
    );

-- COMMAND ----------

-- [SQL] E03 Consulta do resultado
SELECT
  *
FROM
  fiap_analytics.bronze.geosampa_subprefecture_area
