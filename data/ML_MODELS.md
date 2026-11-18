# Modelos de Machine Learning — ChuvaSegura

Este documento descreve o primeiro conjunto de modelos experimentais implementado para o projeto. Os notebooks são executados separadamente do job público de ingestão; as saídas ficam no schema `fiap_analytics.ml` e não são publicadas no site nem usadas para emitir alertas.

## Objetos no Unity Catalog

Execute primeiro [`00_create_schema.sql`](src/ml/00_create_schema.sql). Ele cria `fiap_analytics.ml`, as tabelas de rastreio e o Volume `model_artifacts`. São necessários `USE CATALOG` e `CREATE SCHEMA` no catálogo; a criação do Volume requer `CREATE VOLUME`.

| Objeto | Conteúdo |
|---|---|
| `ml.features_spatial_recurrence` | Painel de contagens reportadas por célula e ano, com histórico defasado. |
| `ml.predictions_spatial_recurrence` | Pontuação anual experimental para células com ocorrências históricas. |
| `ml.features_sibh_level_forecast` | Série de nível e variáveis de chuva agregada usadas no treino. |
| `ml.predictions_sibh_level` | Previsões de nível por sensor para 1 e 3 horas. |
| `ml.features_rainfall_nowcast` | Séries de chuva por estação e atributos defasados. |
| `ml.predictions_rainfall_nowcast` | Estimativa de chuva por estação para a hora seguinte. |
| `ml.model_runs` | Período de dados, versão de código, estado e caminho do artefato de cada execução. |
| `ml.model_metrics` | Métricas de validação temporal e baselines. |
| Volume `ml.model_artifacts` | Pipelines Spark ML versionados por identificador de execução. |

## Modelos

### Recorrência espacial de alagamentos e inundações

[`01_spatial_recurrence.py`](src/ml/01_spatial_recurrence.py) agrega ocorrências GeoSampa dos tipos Alagamento e Inundação em células de 0,01 grau, aproximadamente 1 km nesta latitude. Um regressor GBT usa coordenadas e contagens dos anos anteriores para estimar o volume anual de registros reportados. O último ano civil completo é reservado para teste.

O alvo é a quantidade de registros publicados, não a quantidade real de alagamentos. A ausência de registro não prova que não houve evento. A primeira versão não cruza as áreas do SGB, setores do IBGE nem vias do OSM: esses cruzamentos devem usar interseção espacial adequada, não atribuição pelo centroide. A saída é uma camada exploratória de recorrência.

### Previsão do nível do SIBH

[`02_sibh_level_forecast.py`](src/ml/02_sibh_level_forecast.py) prevê a variação do nível do mesmo sensor para horizontes de 1 e 3 horas. Usa variações recentes do próprio sensor, chuva horária média/máxima e quantidade de pluviômetros com leitura. A avaliação separa os 90 dias mais recentes e compara o erro com persistência.

Os valores absolutos de estações distintas não são interpretados em conjunto. Antes de qualquer uso público, confirmar a unidade e o datum de cada estação, a cobertura temporal, e obter limiares oficiais por sensor. Os níveis exibidos no pacote web têm indícios de unidades/escala heterogêneas; por isso a saída atual não contém classe de alerta.

### Agora-previsão de chuva

[`03_rainfall_nowcast.py`](src/ml/03_rainfall_nowcast.py) estima a chuva da próxima hora na própria estação, usando lags, distância temporal desde as últimas observações, localização e rede (CEMADEN, SIBH ou INMET). A avaliação usa os 90 dias mais recentes como teste e compara com persistência.

O modelo não usa radar, satélite nem previsão meteorológica futura. Representa uma estimativa de curto prazo baseada na chuva observada e não indica risco de alagamento.

## Execução e interpretação

O job manual [`chuvasegura_ml.job.yml`](jobs/chuvasegura_ml.job.yml) executa os três notebooks em paralelo depois de criar o schema e os objetos compartilhados. Os jobs das fontes Gold precisam ter terminado antes. Execute o YAML pelo mecanismo de jobs configurado no workspace ou crie as tarefas equivalentes no Databricks.

Todos os modelos usam divisão temporal; os modelos de chuva e nível comparam seus erros com uma previsão de persistência. As métricas devem ser revisadas por horizonte, estação, estação do ano e cobertura antes de qualquer integração à aplicação. Não usar divisão aleatória de linhas, pois medições vizinhas no espaço e no tempo são dependentes. Para previsões probabilísticas futuras, avaliar calibração além da discriminação; ver [Roberts et al. (2017)](https://nsojournals.onlinelibrary.wiley.com/doi/full/10.1111/ecog.02881) e a [documentação do scikit-learn sobre calibração](https://scikit-learn.org/stable/modules/calibration.html).

Os notebooks dependem de Spark ML (`pyspark.ml`), suportado em jobs serverless desde os ambientes Databricks que incluem a versão 4; o job existente deste repositório usa ambiente 6. Confira os limites de tamanho e compatibilidade na [documentação de serverless](https://docs.databricks.com/aws/en/compute/serverless/limitations).

## Limites desta entrega

- O repositório não tem conexão Databricks ativa nesta sessão. A criação do schema e a execução dos notebooks ainda precisam ocorrer no workspace com as permissões descritas acima.
- Os arquivos de treino são lidos do Unity Catalog em tempo de execução; os dados sintéticos de `app/data/demo/` não são usados.
- A primeira versão não integra previsões ao contrato web, ao assistente nem a avisos oficiais.
- O modelo de recorrência usa somente histórico e coordenadas. A engenharia de atributos espaciais do SGB, IBGE e OSM é uma etapa seguinte após validar os cruzamentos com polígonos.
- Os indicadores são protótipos de apoio à análise; não substituem órgãos oficiais nem orientam ações de emergência.
