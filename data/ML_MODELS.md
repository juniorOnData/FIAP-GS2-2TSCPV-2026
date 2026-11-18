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
| `ml.model_metrics` | Métricas de validação temporal e referências; `segment` traz o recorte (por exemplo, `sensor:123`), nulo para o conjunto inteiro. |
| Volume `ml.model_artifacts` | Pipelines Spark ML versionados por identificador de execução. |

## Modelos

### Recorrência espacial de alagamentos e inundações

[`01_spatial_recurrence.py`](src/ml/01_spatial_recurrence.py) (lógica em [`recorrencia.py`](src/ml/recorrencia.py)) agrega ocorrências GeoSampa dos tipos Alagamento e Inundação em células de 0,01 grau, aproximadamente 1 km nesta latitude. Um regressor GBT aprende log(1 + contagem) a partir da posição da célula e das contagens dos três anos anteriores. O último ano civil completo é reservado para teste, e a previsão final é para o ano seguinte a ele.

- O universo de células do teste sai só dos anos anteriores e inclui as células com qualquer registro (onde há cobertura), mesmo sem alagamento: o teste também cobra os acertos de "nenhum registro".
- Referências: o ano anterior e a média dos três anos anteriores (`skill_vs_mean3`). Como o uso é um ranking, `top10_capture` mede quanto dos registros do ano de teste cai nas 10% de células mais bem pontuadas, comparado com o mesmo cálculo para a média de três anos.

O alvo é a quantidade de registros publicados, não a quantidade real de alagamentos. A ausência de registro não prova que não houve evento. A primeira versão não cruza as áreas do SGB, setores do IBGE nem vias do OSM: esses cruzamentos devem usar interseção espacial adequada, não atribuição pelo centroide. A saída é uma camada exploratória de recorrência.

### Previsão do nível do SIBH

[`02_sibh_level_forecast.py`](src/ml/02_sibh_level_forecast.py) (lógica em [`nivel.py`](src/ml/nivel.py)) prevê a variação do nível do mesmo sensor para horizontes de 1 e 3 horas.

- A série fica numa grade horária completa por sensor: a defasagem k é sempre k horas, mesmo depois de uma falha do sensor.
- Variáveis: variações recentes do próprio sensor, horas desde a leitura anterior, chuva dos pluviômetros a até 10 km do sensor (média, máximo, acumulados de 3 e 6 h) e da cidade.
- Perda absoluta no GBT: os picos de uma hora que alguns sensores têm não dominam o ajuste.
- Teste nos últimos 90 dias, com um intervalo do tamanho do horizonte antes dele. Métricas gerais e por sensor, com `skill_vs_persistence` = 1 − MAE do modelo ÷ MAE da persistência.
- Só recebe previsão o sensor com leitura até 3 h antes da última hora da série.

A cota do SIBH está em centímetros na Gold (cerca de 71.000 a 79.000 nos rios da capital, a altitude da água). Cinco sensores têm valores de 0 a 400, provavelmente leitura de régua. Como o alvo é a variação do próprio sensor, as duas escalas convivem, mas valores absolutos de sensores diferentes não são comparáveis. Antes de qualquer uso público, obter o datum e os limiares oficiais por sensor; por isso a saída não tem classe de alerta.

### Agora-previsão de chuva

[`03_rainfall_nowcast.py`](src/ml/03_rainfall_nowcast.py) (lógica em [`chuva.py`](src/ml/chuva.py)) estima a chuva da próxima hora na própria estação, com as defasagens em horas da estação, os acumulados de 3, 6 e 24 h, a chuva das estações a até 15 km (na hora e na anterior), a localização e a rede.

- Usa SIBH e INMET. O CEMADEN fica fora até a revisão da chuva horária na Gold, que tem valores incompatíveis com as estações vizinhas (até 314 mm em 1 h e centenas de horas com exatamente 1,0 mm). Vale também para a chuva usada no modelo de nível (`REDES_EM_REVISAO` em [`features.py`](src/ml/features.py)).
- O treino usa os últimos `dias_treino` dias (730 por padrão), para caber no serverless; o teste, os últimos 90.
- A maioria das horas é seca, então o MAE sozinho engana. A avaliação compara com persistência e com "zero", mede o erro só nas horas com chuva (`wet_mae_mm`) e o acerto de evento em 1, 5 e 10 mm (POD, FAR e CSI), para o modelo e para a persistência.
- Só recebe previsão a estação com leitura até 3 h antes da última hora da série.

O modelo não usa radar, satélite nem previsão meteorológica futura. Representa uma estimativa de curto prazo baseada na chuva observada e não indica risco de alagamento.

## Execução e interpretação

O job manual [`chuvasegura_ml.job.yml`](jobs/chuvasegura_ml.job.yml) executa os três notebooks em paralelo depois de criar o schema e os objetos compartilhados. Os jobs das fontes Gold precisam ter terminado antes. Execute o YAML pelo mecanismo de jobs configurado no workspace ou crie as tarefas equivalentes no Databricks.

Todos os modelos usam divisão temporal; os modelos de chuva e nível comparam seus erros com uma previsão de persistência. As métricas devem ser revisadas por horizonte, estação, estação do ano e cobertura antes de qualquer integração à aplicação. Não usar divisão aleatória de linhas, pois medições vizinhas no espaço e no tempo são dependentes. Para previsões probabilísticas futuras, avaliar calibração além da discriminação; ver [Roberts et al. (2017)](https://nsojournals.onlinelibrary.wiley.com/doi/full/10.1111/ecog.02881) e a [documentação do scikit-learn sobre calibração](https://scikit-learn.org/stable/modules/calibration.html).

Os notebooks dependem de Spark ML (`pyspark.ml`), suportado em jobs serverless desde os ambientes Databricks que incluem a versão 4; o job existente deste repositório usa ambiente 6. Confira os limites de tamanho e compatibilidade na [documentação de serverless](https://docs.databricks.com/aws/en/compute/serverless/limitations).

## Testes

A lógica dos três modelos está em módulos (`features.py`, `recorrencia.py`, `nivel.py`, `chuva.py`, `avaliacao.py`) e é testada com Spark local e dados sintéticos em [`tests/test_ml_spark.py`](tests/test_ml_spark.py): defasagem em horas depois de falhas, pares de estações vizinhas, ano previsto, universo de células, ranking, horizontes, métricas por sensor e estação parada sem previsão. Os testes exigem Java 17+ e rodam com `CHUVASEGURA_SPARK_TESTS=1 python -m unittest discover -s data/tests -p "test_ml_spark.py"`; sem a variável, são pulados.

## Limites desta entrega

- O repositório não tem conexão Databricks ativa nesta sessão. A criação do schema e a execução dos notebooks ainda precisam ocorrer no workspace com as permissões descritas acima.
- Os arquivos de treino são lidos do Unity Catalog em tempo de execução; os dados sintéticos de `app/data/demo/` não são usados.
- A primeira versão não integra previsões ao contrato web, ao assistente nem a avisos oficiais.
- O modelo de recorrência usa somente histórico e coordenadas. A engenharia de atributos espaciais do SGB, IBGE e OSM é uma etapa seguinte após validar os cruzamentos com polígonos.
- Os indicadores são protótipos de apoio à análise; não substituem órgãos oficiais nem orientam ações de emergência.
