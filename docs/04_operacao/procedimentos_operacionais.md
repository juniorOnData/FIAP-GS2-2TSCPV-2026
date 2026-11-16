# Procedimentos operacionais

## PO01 Preparar um ambiente novo

1. Criar o catálogo e os schemas:

   ```sql
   CREATE CATALOG IF NOT EXISTS fiap_analytics;
   CREATE SCHEMA IF NOT EXISTS fiap_analytics.bronze;
   CREATE SCHEMA IF NOT EXISTS fiap_analytics.silver;
   CREATE SCHEMA IF NOT EXISTS fiap_analytics.gold;
   ```

2. Criar o secret scope do CEMADEN com a Databricks CLI:

   ```bash
   databricks secrets create-scope cemaden
   databricks secrets put-secret cemaden ped_email
   databricks secrets put-secret cemaden ped_password
   ```

3. Clonar o repositório como Git folder (*Workspace > Create > Git folder*).
4. Criar os três jobs a partir dos YAMLs de `jobs/`, ajustando o prefixo do caminho dos notebooks se o Git folder estiver em outro lugar (ver [jobs](../03_jobs/)).

Volumes, tabelas de log e tabelas de resultados de qualidade são criados pelos próprios notebooks.

## PO02 Atualizar o workspace depois de um merge

1. No Git folder do workspace, *Pull* na branch `main`.
2. Se o PR alterou um YAML de `jobs/`, aplicar a mudança no job correspondente (tarefas, dependências, parâmetros ou agenda).
3. Seguir a seção "Depois do merge" da descrição do PR, quando existir.

## PO03 Carregar arquivos do GeoSampa

1. Baixar os arquivos de ocorrências em GeoPackage, soltos ou em `.zip`, com o ano no nome (por exemplo, `SIRGAS_GPKG_riscoocorrencia_2026.gpkg`).
2. Enviar para *Catalog > fiap_analytics > bronze > Volumes > geosampa_history_flooding_events > Upload*.
3. Para corrigir um ano, **substituir** o arquivo do ano (apagar o antigo e enviar o novo). Dois arquivos do mesmo ano fazem a carga parar.
4. Rodar o job `ChuvaSegura` (*Run now*) ou aguardar a execução diária.

## PO04 Backfill inicial do CEMADEN

1. Rodar `01_cemaden_rainfall_readings_landing` com `modo = backfill`, `mes_inicio = 201401` e `mes_fim` vazio. A PED aceita 12 chamadas por minuto: o histórico desde 2014 leva horas. Se a execução parar, rodar de novo; o log serve de checkpoint.
2. Rodar `02_cemaden_rainfall_readings_bronze`.
3. Rodar `01_cemaden_stations_landing` e `02_cemaden_stations_bronze`.
4. Ativar a agenda do job `ChuvaSegura - CEMADEN` (PO06).

## PO05 Backfill inicial do SIBH

1. Rodar `01_sibh_stations_landing` e `02_sibh_stations_bronze`.
2. Rodar `03_sibh_measurements_landing` com `mode = probe` e conferir a resposta da API na saída.
3. Rodar `03_sibh_measurements_landing` com `mode = backfill` e `min_month = 2020-01`. Para um teste rápido, usar `min_month` no mês anterior.
4. Rodar `04_sibh_measurements_bronze` com `window = all`.
5. Ativar a agenda do job `ChuvaSegura - SIBH` (PO06).

## PO06 Ativar ou pausar a agenda de um job

1. No YAML do job, mudar `pause_status` para `UNPAUSED` (ativar) ou `PAUSED` (pausar), por PR.
2. Aplicar a mudança no job do workspace (*Jobs & Pipelines > job > Schedules & Triggers*).

## PO07 Incluir uma regra de qualidade

1. Acrescentar a regra no fim da lista `regras` do notebook de qualidade da etapa (`src/<fonte>/quality/`), usando as funções do módulo `src/common/data_quality.py`.
2. Acrescentar a linha correspondente em `docs/02_catalogos/catalogo_regras_de_qualidade.csv`, com o próximo identificador livre.
3. Atualizar as seções 08, 11 e 15 do notebook de qualidade.

## PO08 Remover a tabela de log antiga

A tabela `fiap_analytics.bronze.ingestion_log` foi substituída pelos logs por origem (PR #8) e já foi migrada. Quando não for mais necessária para consulta:

```sql
DROP TABLE IF EXISTS fiap_analytics.bronze.ingestion_log;
```
