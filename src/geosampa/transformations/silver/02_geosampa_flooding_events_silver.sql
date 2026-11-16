-- Databricks notebook source
-- MAGIC %md
-- MAGIC # GEO_SLV_001 02_geosampa_flooding_events_silver
-- MAGIC
-- MAGIC ## 01. Identificação
-- MAGIC
-- MAGIC | Campo | Valor |
-- MAGIC |---|---|
-- MAGIC | Identificador | GEO_SLV_001 |
-- MAGIC | Nome | 02_geosampa_flooding_events_silver |
-- MAGIC | Projeto | ChuvaSegura |
-- MAGIC | Domínio de negócio | Ocorrências de alagamento (GeoSampa) |
-- MAGIC | Camada | Silver |
-- MAGIC | Linguagem | SQL e Python |
-- MAGIC | Responsável técnico | juniorOnData |
-- MAGIC | Responsável funcional | A definir |
-- MAGIC | Status | Produção |
-- MAGIC | Versão | 1.6 |
-- MAGIC | Criticidade | Alta |
-- MAGIC | Caminho no repositório | `src/geosampa/transformations/silver/02_geosampa_flooding_events_silver.sql` |
-- MAGIC
-- MAGIC ## 02. Objetivo
-- MAGIC
-- MAGIC Recriar a Silver das ocorrências: identificador da ocorrência, data tipada, tipo padronizado, subprefeitura relacionada à tabela de referência e localização (coordenadas UTM e latitude e longitude convertidas da geometria WKB).
-- MAGIC
-- MAGIC ## 03. Contexto funcional
-- MAGIC
-- MAGIC É a versão limpa e padronizada das ocorrências, independente das variações dos arquivos de cada ano. Alimenta a Gold e, no futuro, o cruzamento espacial com as estações de chuva.
-- MAGIC
-- MAGIC ## 04. Arquitetura e fluxo
-- MAGIC
-- MAGIC | Posição | Item |
-- MAGIC |---|---|
-- MAGIC | Etapa anterior | `01_geosampa_history_flooding_events_bronze` e `01_geosampa_subprefecture_area_bronze` |
-- MAGIC | Etapa atual | Recria `fiap_analytics.silver.geosampa_flooding_events` |
-- MAGIC | Etapa posterior | `02_geosampa_silver_quality` e, se aprovada, a Gold |
-- MAGIC
-- MAGIC ## 05. Dependências
-- MAGIC
-- MAGIC | Tipo | Item | Uso |
-- MAGIC |---|---|---|
-- MAGIC | Tabela | `fiap_analytics.bronze.geosampa_history_flooding_events` | Ocorrências brutas |
-- MAGIC | Tabela | `fiap_analytics.bronze.geosampa_subprefecture_area` | Referência de subprefeituras |
-- MAGIC | Objeto de sessão | UDF `geosampa_ponto_coordenadas` (criada na E01 em Python, usada na E02 em SQL) | Converter o WKB em coordenadas |
-- MAGIC | Permissão | USE CATALOG em `fiap_analytics`; SELECT em `fiap_analytics.bronze`; USE SCHEMA, CREATE TABLE e MODIFY em `fiap_analytics.silver` | Ler a Bronze e recriar a Silver |
-- MAGIC
-- MAGIC ## 06. Parâmetros de entrada
-- MAGIC
-- MAGIC Não aplicável. A Silver é recriada inteira a cada execução; não há widgets.
-- MAGIC
-- MAGIC ## 07. Fontes de dados
-- MAGIC
-- MAGIC | Fonte | Formato | Origem | Tipo de leitura | Frequência | Chave | Filtros |
-- MAGIC |---|---|---|---|---|---|---|
-- MAGIC | `fiap_analytics.bronze.geosampa_history_flooding_events` | Delta | GeoSampa | Completa | Diária (job) | `_source_year` + `id`/`primaryindex` | Nenhum |
-- MAGIC | `fiap_analytics.bronze.geosampa_subprefecture_area` | Delta | Anexo IX | Completa | Diária (job) | `sg_subprefeitura` | Nenhum |
-- MAGIC
-- MAGIC ## 08. Processamento técnico
-- MAGIC
-- MAGIC | Etapa | Linguagem | Célula | Descrição |
-- MAGIC |---|---|---|---|
-- MAGIC | E01 | PY | UDF de coordenadas | Decodifica o WKB (POINT 2D, Z, M e ZM, nos códigos ISO e EWKB) e converte UTM 23S em latitude e longitude pelas fórmulas inversas de Snyder. Registra a UDF `geosampa_ponto_coordenadas`. |
-- MAGIC | E02 | SQL | Criação da Silver | `CREATE OR REPLACE TABLE` com identificador, data, tipo, subprefeitura e coordenadas, mantendo o WKB e as colunas de linhagem. |
-- MAGIC | E03 | SQL | Conferência da localização | Conta ocorrências sem coordenada e fora da área de São Paulo, pontos distintos e extensão das coordenadas. |
-- MAGIC
-- MAGIC ## 09. Regras de negócio
-- MAGIC
-- MAGIC | ID | Regra | Lógica | Comportamento esperado |
-- MAGIC |---|---|---|---|
-- MAGIC | RN_GEO_004 | Identificador da ocorrência | `nr_id_ocorrencia = COALESCE(id, primaryindex)` (o arquivo de 2026 usa `primaryindex`). | Com `_source_year`, identifica a ocorrência de forma única. |
-- MAGIC | RN_GEO_005 | Tipo de ocorrência | `ALAGAMENTO` para Alagamento, `DESLIZAMENTO` para Deslizamento, `INUNDACAO` para Inundação, `QUEDA DE ARVORE` para Queda de Árvore; qualquer outro valor, inclusive vazio, para Outros. | Quatro tipos e `Outros`. A proporção de `Outros` é monitorada pela regra DQ_GEO_SLV_014. |
-- MAGIC | RN_GEO_006 | Subprefeitura da ocorrência | Sigla = dois primeiros caracteres de `subprefeit` ou `subpreit`; `G `, `G-` e `GU` viram `G`; `NA` vira nulo. Relaciona primeiro pela sigla do GeoSampa e, se não achar, pela sigla do Anexo IX. | Sigla e nome oficiais; sem correspondência, nome nulo (vai para o membro `-1` na Gold). |
-- MAGIC | RN_GEO_007 | Localização da ocorrência | Só geometria POINT em EPSG 31983 (SIRGAS 2000 / UTM 23S) é convertida. | `nr_x_utm` e `nr_y_utm` em metros, `nr_latitude` e `nr_longitude` em graus; outros tipos ou SRIDs ficam nulos. |
-- MAGIC
-- MAGIC ## 10. Saídas de dados
-- MAGIC
-- MAGIC | Destino | Formato | Granularidade | Chave | Modo de escrita |
-- MAGIC |---|---|---|---|---|
-- MAGIC | `fiap_analytics.silver.geosampa_flooding_events` | Delta | Uma linha por ocorrência | `_source_year` + `nr_id_ocorrencia` | `CREATE OR REPLACE TABLE` (recriada a cada execução) |
-- MAGIC
-- MAGIC As colunas de cada tabela estão no dicionário de dados (`docs/02_catalogos/dicionario_de_dados.csv`).
-- MAGIC
-- MAGIC ## 11. Qualidade dos dados
-- MAGIC
-- MAGIC A última célula mostra a conferência da localização. A aprovação da Silver é feita pelo notebook `02_geosampa_silver_quality` (regras DQ_GEO_SLV_001 a DQ_GEO_SLV_014): uma regra `ERRO` reprovada impede a carga da Gold.
-- MAGIC
-- MAGIC ## 12. Tratamento de erros
-- MAGIC
-- MAGIC | Situação | Comportamento | Registro | Recuperação |
-- MAGIC |---|---|---|---|
-- MAGIC | Tabela de origem inexistente | A célula falha e a tarefa para | Saída da tarefa no Lakeflow Jobs | Rodar antes os notebooks da Bronze |
-- MAGIC | Geometria em formato não previsto | Coordenadas ficam nulas, sem erro | Conferência da última célula e regra DQ_GEO_SLV_011 | Ajustar a UDF para o novo formato |
-- MAGIC
-- MAGIC ## 13. Segurança e governança
-- MAGIC
-- MAGIC | Item | Valor |
-- MAGIC |---|---|
-- MAGIC | Classificação dos dados | Pública (dados abertos da fonte; confirmar os termos de uso) |
-- MAGIC | Dados sensíveis | Nenhum. As ocorrências têm coordenadas de logradouro, sem identificação de pessoas |
-- MAGIC | Credenciais | Nenhuma |
-- MAGIC | Permissões necessárias | USE CATALOG em `fiap_analytics`; SELECT em `fiap_analytics.bronze`; USE SCHEMA, CREATE TABLE e MODIFY em `fiap_analytics.silver` |
-- MAGIC | Políticas aplicáveis | Termos de uso dos dados abertos da Prefeitura de São Paulo (confirmar) |
-- MAGIC
-- MAGIC ## 14. Monitoramento e operação
-- MAGIC
-- MAGIC | Item | Valor |
-- MAGIC |---|---|
-- MAGIC | Job e tarefa | `ChuvaSegura` (`jobs/chuvasegura_geosampa.job.yml`), tarefa `silver_geosampa_flooding_events` |
-- MAGIC | Frequência | Diária, 09:00:30 (America/Sao_Paulo) |
-- MAGIC | SLA | A definir |
-- MAGIC | Métricas | Linhas, ocorrências sem coordenada e fora da área |
-- MAGIC | Logs | Resultados das regras em `fiap_analytics.bronze.geosampa_data_quality_log` (etapa `silver`) |
-- MAGIC | Alertas | Não configurado (recomendação: notificação de falha no job) |
-- MAGIC | Reprocessamento | Rodar o notebook de novo (recriação completa) |
-- MAGIC
-- MAGIC ## 15. Histórico de alterações
-- MAGIC
-- MAGIC | Versão | Data | Responsável | Descrição |
-- MAGIC |---|---|---|---|
-- MAGIC | 1.0 | 2026-11-06 | juniorOnData | Criação da Silver de ocorrências (PR #4) |
-- MAGIC | 1.1 | 2026-11-16 | juniorOnData | Subprefeitura relacionada pela coluna `sg_subprefeitura_geosampa` (PR #8) |
-- MAGIC | 1.2 | 2026-11-16 | juniorOnData | Formato de código fonte e novo nome do arquivo (PR #9) |
-- MAGIC | 1.3 | 2026-11-16 | juniorOnData | Coordenadas UTM e latitude e longitude a partir do WKB (PR #11) |
-- MAGIC | 1.4 | 2026-11-16 | juniorOnData | Coordenadas também para POINT Z (PR #12) |
-- MAGIC | 1.5 | 2026-11-16 | juniorOnData | Tabela renomeada para `geosampa_flooding_events` e identificador da ocorrência (PR #14) |
-- MAGIC | 1.6 | 2026-11-16 | juniorOnData | Documentação no padrão oficial (PR #15) |

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # [PY] E01 UDF de coordenadas
-- MAGIC # O Spark SQL não tem função para ler os números de dentro do WKB, então a conversão é uma UDF Python.
-- MAGIC # A função fica no próprio notebook para ser enviada inteira aos executores.
-- MAGIC import math
-- MAGIC import struct
-- MAGIC
-- MAGIC # SIRGAS 2000 / UTM zona 23S (EPSG:31983): elipsoide GRS80, meridiano central -45°.
-- MAGIC UTM_A = 6378137.0
-- MAGIC UTM_F = 1 / 298.257222101
-- MAGIC UTM_K0 = 0.9996
-- MAGIC UTM_LON0 = math.radians(-45.0)
-- MAGIC UTM_FALSO_LESTE, UTM_FALSO_NORTE = 500000.0, 10000000.0
-- MAGIC
-- MAGIC
-- MAGIC def utm23s_para_latlon(x, y):
-- MAGIC     """Converte coordenadas UTM 23S (metros) em (latitude, longitude) em graus. Fórmulas inversas de Snyder (USGS)."""
-- MAGIC     e2 = UTM_F * (2 - UTM_F)
-- MAGIC     ep2 = e2 / (1 - e2)
-- MAGIC     e1 = (1 - math.sqrt(1 - e2)) / (1 + math.sqrt(1 - e2))
-- MAGIC     m = (y - UTM_FALSO_NORTE) / UTM_K0
-- MAGIC     mu = m / (UTM_A * (1 - e2 / 4 - 3 * e2 ** 2 / 64 - 5 * e2 ** 3 / 256))
-- MAGIC     phi1 = (mu + (3 * e1 / 2 - 27 * e1 ** 3 / 32) * math.sin(2 * mu)
-- MAGIC             + (21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32) * math.sin(4 * mu)
-- MAGIC             + (151 * e1 ** 3 / 96) * math.sin(6 * mu)
-- MAGIC             + (1097 * e1 ** 4 / 512) * math.sin(8 * mu))
-- MAGIC     sen, cos, tg = math.sin(phi1), math.cos(phi1), math.tan(phi1)
-- MAGIC     c1 = ep2 * cos ** 2
-- MAGIC     t1 = tg ** 2
-- MAGIC     n1 = UTM_A / math.sqrt(1 - e2 * sen ** 2)
-- MAGIC     r1 = UTM_A * (1 - e2) / (1 - e2 * sen ** 2) ** 1.5
-- MAGIC     d = (x - UTM_FALSO_LESTE) / (n1 * UTM_K0)
-- MAGIC     lat = phi1 - (n1 * tg / r1) * (
-- MAGIC         d ** 2 / 2
-- MAGIC         - (5 + 3 * t1 + 10 * c1 - 4 * c1 ** 2 - 9 * ep2) * d ** 4 / 24
-- MAGIC         + (61 + 90 * t1 + 298 * c1 + 45 * t1 ** 2 - 252 * ep2 - 3 * c1 ** 2) * d ** 6 / 720
-- MAGIC     )
-- MAGIC     lon = UTM_LON0 + (
-- MAGIC         d
-- MAGIC         - (1 + 2 * t1 + c1) * d ** 3 / 6
-- MAGIC         + (5 - 2 * c1 + 28 * t1 - 3 * c1 ** 2 + 8 * ep2 + 24 * t1 ** 2) * d ** 5 / 120
-- MAGIC     ) / cos
-- MAGIC     return math.degrees(lat), math.degrees(lon)
-- MAGIC
-- MAGIC
-- MAGIC def ponto_wkb_para_coordenadas(wkb, srs_id):
-- MAGIC     """WKB de um POINT em EPSG:31983 -> (x, y, latitude, longitude). Outros tipos ou SRIDs devolvem None.
-- MAGIC
-- MAGIC     Aceita POINT 2D, Z, M e ZM, nos códigos ISO (1, 1001, 2001, 3001) e nas flags do EWKB
-- MAGIC     (Z, M e SRID embutido). Os arquivos do GeoSampa variam por ano: há anos em POINT e anos em POINT Z.
-- MAGIC     X e Y são sempre as duas primeiras coordenadas; Z e M são ignorados.
-- MAGIC     """
-- MAGIC     if wkb is None or srs_id != 31983:
-- MAGIC         return None
-- MAGIC     dados = bytes(wkb)
-- MAGIC     if len(dados) < 21 or dados[0] not in (0, 1):
-- MAGIC         return None
-- MAGIC     ordem = "<" if dados[0] == 1 else ">"
-- MAGIC     (tipo,) = struct.unpack(ordem + "I", dados[1:5])
-- MAGIC     inicio = 9 if tipo & 0x20000000 else 5  # EWKB com SRID: 4 bytes antes das coordenadas
-- MAGIC     if (tipo & 0x0FFFFFFF) % 1000 != 1 or len(dados) < inicio + 16:  # não é POINT
-- MAGIC         return None
-- MAGIC     x, y = struct.unpack(ordem + "dd", dados[inicio:inicio + 16])
-- MAGIC     if math.isnan(x) or math.isnan(y):  # POINT EMPTY
-- MAGIC         return None
-- MAGIC     lat, lon = utm23s_para_latlon(x, y)
-- MAGIC     return x, y, lat, lon
-- MAGIC
-- MAGIC
-- MAGIC spark.udf.register(
-- MAGIC     "geosampa_ponto_coordenadas",
-- MAGIC     ponto_wkb_para_coordenadas,
-- MAGIC     "struct<x_utm:double, y_utm:double, latitude:double, longitude:double>",
-- MAGIC )

-- COMMAND ----------

-- [SQL] E02 Criação da Silver
CREATE OR REPLACE TABLE fiap_analytics.silver.geosampa_flooding_events AS
  WITH history_flooding_events AS (
    SELECT
      -- Número da ocorrência dentro do arquivo do ano. O arquivo de 2026 usa a coluna primaryindex no lugar de id.
      TRY_CAST(COALESCE(id, primaryindex) AS BIGINT) AS nr_id_ocorrencia,
      CAST(data AS DATE) AS dt_ocorrencia,
      CAST(DATE_FORMAT(CAST(data AS DATE), "yyyyMMdd") AS BIGINT) AS cod_dim_data,
      CASE
        WHEN ocorrencia IN ("ALAGAMENTO") THEN "Alagamento"
        WHEN ocorrencia IN ("DESLIZAMENTO") THEN "Deslizamento"
        WHEN ocorrencia IN ("INUNDACAO") THEN "Inundação"
        WHEN ocorrencia IN ("QUEDA DE ARVORE") THEN "Queda de Árvore"
        ELSE "Outros"
      END AS dc_tipo_ocorrencia,
      LEFT(COALESCE(subprefeit, subpreit), 2) AS sg_subprefeitura,
      COALESCE(subprefeit, subpreit) AS nm_subprefeitura,
      geom_wkb,
      geosampa_ponto_coordenadas(geom_wkb, _source_srs_id) AS coordenadas,
      _source_file,
      _source_layer,
      _source_year,
      _source_sha256,
      _source_srs_id,
      _ingested_at
    FROM
      fiap_analytics.bronze.geosampa_history_flooding_events
  ),
  ajuste_subprefecture AS (
    SELECT
      nr_id_ocorrencia,
      dt_ocorrencia,
      cod_dim_data,
      dc_tipo_ocorrencia,
      -- Só corrige variações de grafia. A troca da sigla do GeoSampa pela do Anexo IX (ex.: FO para FB)
      -- vem da coluna sg_subprefeitura_geosampa da tabela de subprefeituras.
      CASE
        WHEN sg_subprefeitura IN ("G ", "G-", "GU") THEN "G"
        WHEN sg_subprefeitura IN ("NA") THEN NULL
        ELSE sg_subprefeitura
      END AS sg_subprefeitura_geosampa,
      nm_subprefeitura,
      geom_wkb,
      coordenadas,
      _source_file,
      _source_layer,
      _source_year,
      _source_sha256,
      _source_srs_id,
      _ingested_at
    FROM
      history_flooding_events
  )
  SELECT
    f.nr_id_ocorrencia,
    f.dt_ocorrencia,
    f.cod_dim_data,
    f.dc_tipo_ocorrencia,
    COALESCE(g.sg_subprefeitura, a.sg_subprefeitura, f.sg_subprefeitura_geosampa) AS sg_subprefeitura,
    COALESCE(g.nm_subprefeitura, a.nm_subprefeitura) AS nm_subprefeitura,
    f.coordenadas.x_utm AS nr_x_utm,
    f.coordenadas.y_utm AS nr_y_utm,
    f.coordenadas.latitude AS nr_latitude,
    f.coordenadas.longitude AS nr_longitude,
    f.geom_wkb,
    f._source_file,
    f._source_layer,
    f._source_year,
    f._source_sha256,
    f._source_srs_id,
    f._ingested_at
  FROM
    ajuste_subprefecture f
      -- Primeiro pela sigla do GeoSampa; se não achar, pela sigla do Anexo IX (arquivos que já usam a sigla oficial).
      LEFT JOIN fiap_analytics.bronze.geosampa_subprefecture_area g
        ON f.sg_subprefeitura_geosampa = g.sg_subprefeitura_geosampa
      LEFT JOIN fiap_analytics.bronze.geosampa_subprefecture_area a
        ON g.sg_subprefeitura IS NULL AND f.sg_subprefeitura_geosampa = a.sg_subprefeitura

-- COMMAND ----------

-- [SQL] E03 Conferência da localização
-- Conferência da localização: pontos sem coordenada e fora da área de São Paulo e entorno.
SELECT
  COUNT(*)                                                         AS ocorrencias,
  COUNT_IF(nr_latitude IS NULL)                                    AS sem_coordenada,
  COUNT_IF(nr_latitude NOT BETWEEN -24.1 AND -23.3
           OR nr_longitude NOT BETWEEN -47.0 AND -46.3)            AS fora_da_area,
  COUNT(DISTINCT nr_x_utm, nr_y_utm)                               AS pontos_distintos,
  MIN(nr_latitude)                                                 AS lat_min,
  MAX(nr_latitude)                                                 AS lat_max,
  MIN(nr_longitude)                                                AS lon_min,
  MAX(nr_longitude)                                                AS lon_max
FROM fiap_analytics.silver.geosampa_flooding_events
