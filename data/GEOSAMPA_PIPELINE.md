# Documentação Técnica - GeoSampa

Este documento descreve como o código do projeto carrega o histórico de ocorrências do GeoSampa, padroniza os dados espaciais, monta o modelo Gold e valida o resultado. Os caminhos citados são relativos a `data/`.

## Visão geral

O pipeline parte de duas entradas. Os arquivos históricos de ocorrências são carregados no volume e processados para Bronze; a tabela de subprefeituras é recriada por SQL a partir da relação do Anexo IX. O notebook Silver padroniza as ocorrências e associa a sigla da subprefeitura. A qualidade Silver libera as transformações que dependem das ocorrências; a dimensão de subprefeituras pode ser atualizada assim que sua tabela Bronze estiver pronta. A qualidade Gold valida o modelo analítico ao final.

Na execução, o job realiza estas etapas:

1. Carrega os GeoPackages recebidos no volume e atualiza a tabela Bronze de ocorrências.
2. Recria a tabela Bronze de subprefeituras a partir da lista mantida no notebook SQL.
3. Gera a tabela Silver com campos tipados, tipos de ocorrência padronizados e coordenadas derivadas.
4. Executa as regras de qualidade Silver.
5. Atualiza a dimensão de subprefeituras após sua Bronze e a dimensão de tipos após a qualidade Silver.
6. Atualiza a fato por ano de origem e executa a qualidade Gold.

O job da fonte é definido em [`jobs/chuvasegura_geosampa.job.yml`](jobs/chuvasegura_geosampa.job.yml). Ele não tem agenda própria: é executado pelo job principal `ChuvaSegura`, depois da dimensão compartilhada de tempo.

## Entradas e pressupostos

### Ocorrências históricas

O notebook [`src/geosampa/ingestion/01_geosampa_history_flooding_events_bronze.py`](src/geosampa/ingestion/01_geosampa_history_flooding_events_bronze.py) procura recursivamente arquivos `.gpkg` e `.zip` no Volume Unity Catalog `fiap_analytics.bronze.geosampa_history_flooding_events`. Um ZIP pode conter GeoPackages; os caminhos internos são verificados antes da extração. O ano é identificado no nome do arquivo pelo padrão `20AA`. Quando o nome não contém ano, o caminho do arquivo é usado como chave de versão.

A entrada é preparada fora deste notebook: baixe os arquivos da camada de ocorrências no GeoSampa e envie-os ao Volume. Deve haver somente um arquivo por ano. Se houver mais de um, a tarefa registra falha e interrompe a carga para evitar alternância de versões a cada execução. Um arquivo corrigido substitui a versão carregada daquele ano.

### Relação de subprefeituras

O notebook SQL [`src/geosampa/ingestion/01_geosampa_subprefecture_area_bronze.sql`](src/geosampa/ingestion/01_geosampa_subprefecture_area_bronze.sql) cria `fiap_analytics.bronze.geosampa_subprefecture_area` com os nomes, siglas, endereços, agrupamentos e tipos de unidade do Anexo IX (ata 41/14). A tabela também mantém `sg_subprefeitura_geosampa`, a sigla que aparece nos arquivos de ocorrência. Por exemplo, o GeoSampa usa `FO` para a sigla oficial `FB`.

Esta relação está definida em `VALUES` no SQL. A tarefa não baixa o Anexo IX durante a execução; uma alteração na relação exige atualizar os valores e as descrições no próprio notebook.

## Bronze: leitura e controle de versões

Para cada GeoPackage, o notebook lê `gpkg_contents` para localizar as camadas vetoriais e `gpkg_geometry_columns` para obter a coluna de geometria e o SRID. Os atributos são gravados como texto e a geometria como bytes em `geom_wkb`. O cabeçalho binário GeoPackage é removido para manter somente o WKB; o SRID original é gravado em `_source_srs_id`.

Além dos atributos e da geometria, a Bronze registra `_source_file`, `_source_layer`, `_source_year`, `_source_sha256`, `_source_srs_id` e `_ingested_at`. O schema pode evoluir com os arquivos: a gravação usa `mergeSchema` e une camadas com colunas diferentes, permitindo colunas ausentes em determinada camada.

O SHA-256 evita reler um arquivo idêntico. Quando o conteúdo muda, a nova versão é anexada, as contagens lidas são comparadas com as contagens gravadas por hash e camada, e a versão anterior da mesma chave (ano ou caminho sem ano) é removida. Se a leitura ou a gravação falhar, a execução registra `FALHA` no log comum de ingestão e propaga o erro.

## Silver: padronização e localização

O notebook [`src/geosampa/transformations/silver/02_geosampa_flooding_events_silver.sql`](src/geosampa/transformations/silver/02_geosampa_flooding_events_silver.sql) recria `fiap_analytics.silver.geosampa_flooding_events` com uma linha por ocorrência da Bronze.

As principais transformações são:

- `id` vira `nr_id_ocorrencia`; nos arquivos em que `id` não existe, é usado `primaryindex`. O ano fica em `_source_year`, e a chave natural é a combinação ano + ID.
- `data` vira `dt_ocorrencia` e `cod_dim_data` no formato `yyyyMMdd`.
- `ocorrencia` é normalizada para `Alagamento`, `Deslizamento`, `Inundação` ou `Queda de Árvore`. Valores desconhecidos ou vazios ficam como `Outros`.
- A sigla e o nome da subprefeitura são lidos de `subprefeit` ou, como alternativa, `subpreit`. Variações conhecidas da sigla são normalizadas. O relacionamento tenta primeiro a sigla GeoSampa; quando não há correspondência, tenta a sigla oficial do Anexo IX.
- A geometria original permanece em `geom_wkb`. A UDF `geosampa_ponto_coordenadas`, registrada pelo notebook, interpreta o WKB e deriva `nr_x_utm`, `nr_y_utm`, `nr_latitude` e `nr_longitude`.

Os registros geográficos esperados são pontos em SIRGAS 2000 / UTM zona 23S (EPSG:31983). O código próprio da UDF aceita WKB de ponto 2D, Z, M e ZM, além do formato EWKB com SRID embutido. Z e M são ignorados; X e Y são as duas primeiras coordenadas. Geometrias nulas, vazias, inválidas para ponto ou com SRID diferente de 31983 não geram coordenadas derivadas. O WKB original não é descartado.

A conversão UTM para latitude/longitude usa os parâmetros do elipsoide GRS80 e as fórmulas inversas de Snyder implementadas no notebook. A instrução final consulta registros sem coordenada, pontos fora do envelope de São Paulo e a quantidade de pontos distintos para inspeção operacional.

## Gold: dimensões e fato

### Dimensões

- [`src/geosampa/transformations/gold/03_dim_geosampa_subprefecture_area.sql`](src/geosampa/transformations/gold/03_dim_geosampa_subprefecture_area.sql) faz `MERGE` pela sigla oficial. A chave substituta é identidade e permanece estável nas atualizações. Mudanças de nome, endereço, agrupamento ou sigla GeoSampa atualizam a linha existente. O membro `COD_DIM_SUBPREFEITURA = -1` representa dado não informado.
- [`src/geosampa/transformations/gold/03_dim_geosampa_type_flooding_events.sql`](src/geosampa/transformations/gold/03_dim_geosampa_type_flooding_events.sql) mantém uma linha por tipo encontrado na Silver e insere somente tipos novos. O membro `COD_DIM_OCORRENCIA = -1` representa tipo não informado.

### Fato de ocorrências

[`src/geosampa/transformations/gold/04_fact_geosampa_flooding_events.sql`](src/geosampa/transformations/gold/04_fact_geosampa_flooding_events.sql) grava uma linha por ocorrência em `fiap_analytics.gold.fact_geosampa_flooding_events`. A chave natural para reconhecer a ocorrência é `NUM_ANO_ORIGEM` + `NUM_ID_OCORRENCIA`; a tabela também tem chave técnica `COD_FATO_OCORRENCIA`.

A fato associa a ocorrência a `dim_tempo`, à dimensão de tipos e à dimensão de subprefeituras. Um relacionamento não encontrado recebe a chave `-1`, mantendo as chaves da fato preenchidas. Latitude e longitude ficam na fato para visualização e cruzamentos espaciais posteriores. As chaves estrangeiras estão declaradas no Unity Catalog; elas documentam o relacionamento, mas são informativas. A tabela usa liquid clustering por `COD_DIM_TEMPO` e `COD_DIM_SUBPREFEITURA`.

A atualização é incremental por ano de origem. O notebook compara Silver e Gold por ano, usando quantidade de linhas e uma assinatura calculada sobre os campos analíticos. Anos novos ou alterados são removidos da fato e inseridos novamente; anos que deixaram de existir na Silver também são removidos. Se a Silver estiver vazia, a rotina não apaga a Gold inteira, o que evita uma limpeza acidental após uma carga sem dados.

## Qualidade de dados

As regras são executadas pelos notebooks [`src/geosampa/quality/02_geosampa_silver_quality.py`](src/geosampa/quality/02_geosampa_silver_quality.py) e [`src/geosampa/quality/04_geosampa_gold_quality.py`](src/geosampa/quality/04_geosampa_gold_quality.py), usando o módulo comum [`src/common/data_quality.py`](src/common/data_quality.py). Todas as avaliações são registradas em `fiap_analytics.bronze.geosampa_data_quality_log`. Falhas de severidade `ERRO` interrompem a tarefa; `AVISO` registra o resultado sem interrompê-la.

| Etapa | Erros que bloqueiam a execução | Avisos registrados |
|---|---|---|
| Silver | Tabela vazia; contagem diferente da Bronze; ano ou ID nulo; ocorrência duplicada por ano + ID; data nula ou anterior a 2010/posterior a hoje; SRID de origem diferente de 31983; coordenada fora do envelope configurado para São Paulo e entorno. | `name` repetido no mesmo ano; data fora do ano do arquivo; coordenadas ausentes até 1% das linhas; subprefeitura sem correspondência até 1%; tipo `Outros` até 5%. |
| Gold | Fato vazia; diferença de contagem entre Silver e Gold por ano; chave natural duplicada; referências ausentes em `dim_tempo` ou nas dimensões de tipo/subprefeitura; coordenada fora do envelope; chave de dimensão ou descrição duplicada; dimensão sem exatamente um membro `-1`; dias faltando na `dim_tempo`. | Ocorrências sem subprefeitura (limite de 1%); ocorrências sem tipo reconhecido. |

O campo `limite` é a proporção máxima de falhas aceita pela regra. Para regras de aviso sem limite explícito, o padrão é zero; o resultado ainda é registrado como aviso e não bloqueia o job.

## Execução e diagnóstico

As tarefas do job não recebem widgets específicos. O arquivo de job define as dependências: ingestão das ocorrências e carga da relação de subprefeituras começam em paralelo; a Silver espera ambas; a qualidade Silver antecede a dimensão de tipos e a fato; a dimensão de subprefeituras pode ser atualizada após sua Bronze; a qualidade Gold encerra o fluxo.

Para atualizar um arquivo de ocorrências, mantenha uma única versão por ano no Volume e execute o job GeoSampa integrado ao `ChuvaSegura`. O código não baixa o histórico do portal automaticamente. Consulte o log de ingestão para situação do arquivo e o log de qualidade para regras reprovadas:

```sql
SELECT janela, status, registros, arquivo, sha256, finalizado_em, mensagem
FROM fiap_analytics.bronze.geosampa_ingestion_log
ORDER BY finalizado_em DESC;

SELECT executado_em, etapa, tabela, regra, severidade, status, falhas, total, percentual, mensagem
FROM fiap_analytics.bronze.geosampa_data_quality_log
ORDER BY executado_em DESC;
```

Se a tarefa Silver falhar, verifique primeiro a unicidade do ano/arquivo, o SRID no metadado do GeoPackage e as regras no log de qualidade. Uma execução com erro em regra Gold indica divergência entre dimensões, fato, Silver ou dimensão de tempo. O job principal também exige que a tarefa de qualidade Gold conclua antes de seguir para as tarefas posteriores.

## Referências

- [Catálogo de metadados GeoSampa — Alagamento (dados de Ocorrências)](https://metadados.geosampa.prefeitura.sp.gov.br/geonetwork/srv/api/records/432c06b1-03a1-4b1f-9210-423e1b58e869), mantido pela Prefeitura de São Paulo.
- [Portal GeoSampa](https://geosampa.prefeitura.sp.gov.br/), para consulta e obtenção das camadas geográficas.
- [OGC GeoPackage Encoding Standard](https://www.ogc.org/standards/geopackage/), especificação do formato baseado em SQLite usado pelos arquivos de entrada.
- [SIRGAS 2000 / UTM zona 23S, EPSG:31983](https://epsg.io/31983), referência do sistema de coordenadas esperado pelo processamento das ocorrências.

## Arquivos relacionados

- Job: [`jobs/chuvasegura_geosampa.job.yml`](jobs/chuvasegura_geosampa.job.yml).
- Log de ingestão: [`src/common/ingestion_log.py`](src/common/ingestion_log.py).
- Log e regras de qualidade: [`src/common/data_quality.py`](src/common/data_quality.py).
- Inventário e visão geral das fontes: [`README.md`](README.md#geosampa).

Os notebooks contêm notas junto às etapas e funções que executam. Este guia reúne o contexto do pipeline, as dependências entre as camadas e a operação da fonte.
