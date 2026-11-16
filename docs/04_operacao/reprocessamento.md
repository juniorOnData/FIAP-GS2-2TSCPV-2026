# Reprocessamento

Todos os notebooks são idempotentes: rodar de novo a mesma janela produz o mesmo resultado, sem duplicar dados. A tabela abaixo resume como reprocessar cada caso.

| Código | Caso | O que rodar | Parâmetros | Efeito |
|---|---|---|---|---|
| RP01 | Corrigir o arquivo de um ano do GeoSampa | Trocar o arquivo no volume e rodar o job `ChuvaSegura` | Nenhum | A Bronze grava a versão nova e remove a anterior; a fato recarrega só o ano alterado |
| RP02 | Recriar a Silver do GeoSampa | `02_geosampa_flooding_events_silver` e depois `02_geosampa_silver_quality` | Nenhum | Silver recriada do zero a partir da Bronze |
| RP03 | Recarregar a fato do GeoSampa | `04_fact_geosampa_flooding_events` | Nenhum | Recarrega os anos novos ou alterados; os demais ficam como estão |
| RP04 | Recriar toda a Gold do GeoSampa | Os `DROP TABLE` abaixo e depois o job `ChuvaSegura` | Nenhum | Dimensões e fato recriadas; as chaves substitutas são renumeradas |
| RP05 | Recoletar meses de leituras do CEMADEN | `01_cemaden_rainfall_readings_landing` e depois `02_cemaden_rainfall_readings_bronze` | `modo = backfill`, `mes_inicio` e `mes_fim` no intervalo, `forcar_reprocesso = sim` | Os arquivos dos meses são sobrescritos e a Bronze substitui as fatias |
| RP06 | Recarregar o cadastro do CEMADEN na Bronze | `02_cemaden_stations_bronze` | `snapshot_date = all` ou a data | Fatias das datas substituídas |
| RP07 | Recoletar meses de medições do SIBH | `03_sibh_measurements_landing` e depois `04_sibh_measurements_bronze` | Landing: `mode = backfill`, `min_month` no primeiro mês, `force = true`. Bronze: `window` no mês ou `all` | Arquivos sobrescritos e fatias substituídas |
| RP08 | Recarregar o cadastro do SIBH na Bronze | `02_sibh_stations_bronze` | `snapshot_date = all` ou a data | Fatias das datas substituídas |
| RP09 | Recriar a `dim_tempo` | `00_dim_tempo` | Nenhum | Insere os dias que faltam e corrige atributos; as chaves não mudam |

## RP04 Recriar toda a Gold do GeoSampa

Necessário quando a estrutura das tabelas muda (por exemplo, o PR #14). A ordem respeita as chaves estrangeiras da fato:

```sql
DROP TABLE IF EXISTS fiap_analytics.gold.fact_geosampa_flooding_events;
DROP TABLE IF EXISTS fiap_analytics.gold.dim_geosampa_type_flooding_events;
DROP TABLE IF EXISTS fiap_analytics.gold.dim_geosampa_subprefecture_area;
```

Depois, rodar o job `ChuvaSegura` inteiro. Relatórios que guardam as chaves substitutas (`COD_DIM_*`, `COD_FATO_OCORRENCIA`) precisam ser atualizados, porque os valores mudam.

## Cuidados

1. Reprocessar sempre na ordem das camadas: landing, Bronze, Silver, qualidade, Gold.
2. No CEMADEN, o backfill sem `forcar_reprocesso = sim` pula os meses já concluídos no log.
3. No SIBH, o backfill sem `force = true` pula os meses com manifesto completo.
4. Os logs de ingestão e de qualidade nunca são apagados: cada reprocessamento acrescenta novas linhas com outro `batch_id`.
