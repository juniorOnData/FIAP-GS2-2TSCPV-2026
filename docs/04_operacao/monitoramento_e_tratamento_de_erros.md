# Monitoramento e tratamento de erros

## 1. O que acompanhar

| Item | Onde | Frequência |
|---|---|---|
| Situação das execuções dos jobs | Lakeflow Jobs (*Jobs & Pipelines > job > Runs*) | Diária |
| Janelas com falha ou parciais | Logs de ingestão (`fiap_analytics.bronze.<origem>_ingestion_log`) | Diária |
| Regras de qualidade reprovadas | `fiap_analytics.bronze.geosampa_data_quality_log` | Diária |
| Avisos de qualidade recorrentes | `geosampa_data_quality_log` com `severidade = 'AVISO'` | Semanal |
| Duração das execuções | Lakeflow Jobs ou tabelas de sistema do Lakeflow | Semanal |

## 2. Consultas de acompanhamento

Janelas com falha nos últimos 7 dias, por origem:

```sql
SELECT 'cemaden' AS origem, fonte, etapa, janela, finalizado_em, mensagem
FROM fiap_analytics.bronze.cemaden_ingestion_log
WHERE status = 'FALHA' AND finalizado_em >= current_timestamp() - INTERVAL 7 DAYS
UNION ALL
SELECT 'sibh', fonte, etapa, janela, finalizado_em, mensagem
FROM fiap_analytics.bronze.sibh_ingestion_log
WHERE status = 'FALHA' AND finalizado_em >= current_timestamp() - INTERVAL 7 DAYS
UNION ALL
SELECT 'geosampa', fonte, etapa, janela, finalizado_em, mensagem
FROM fiap_analytics.bronze.geosampa_ingestion_log
WHERE status = 'FALHA' AND finalizado_em >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY finalizado_em DESC;
```

Meses ainda parciais (devem ser completados pelo incremental):

```sql
SELECT fonte, janela, max(finalizado_em) AS ultima_coleta
FROM fiap_analytics.bronze.cemaden_ingestion_log
WHERE etapa = 'landing'
GROUP BY fonte, janela
HAVING max_by(status, finalizado_em) = 'PARCIAL'
ORDER BY janela;
```

Resultado de qualidade da última execução de cada etapa:

```sql
WITH ultima AS (
  SELECT etapa, max_by(batch_id, executado_em) AS batch_id
  FROM fiap_analytics.bronze.geosampa_data_quality_log
  GROUP BY etapa
)
SELECT q.etapa, q.tabela, q.regra, q.severidade, q.status, q.falhas, q.total, q.percentual, q.mensagem
FROM fiap_analytics.bronze.geosampa_data_quality_log q
JOIN ultima u USING (etapa, batch_id)
ORDER BY q.status DESC, q.severidade, q.regra;
```

Quando as tabelas de sistema do Lakeflow estiverem habilitadas no workspace, o histórico de execuções também pode ser consultado em `system.lakeflow.job_run_timeline`.

## 3. Alertas

| Alerta | Situação | Recomendação |
|---|---|---|
| Falha de job | Não configurado | Notificação de falha (*Job notifications > Failure*) para o responsável técnico nos três jobs |
| Duração acima do normal | Não configurado | Aviso de duração (*Duration warning*) na landing de leituras do CEMADEN |
| Regra de qualidade reprovada | Coberto indiretamente pela falha da tarefa de qualidade | A notificação de falha do job já cobre |

As notificações no nível do job não são enviadas quando uma tarefa falha e é repetida. Se forem configuradas retentativas, usar notificações também no nível da tarefa.

## 4. Erros conhecidos

| Sintoma | Causa provável | Ação |
|---|---|---|
| `Pasta src/common não encontrada a partir de ...` | Notebook executado fora do Git folder do repositório | Executar a partir do Git folder ou ajustar o caminho no job |
| `<origem>_ingestion_log está fora do formato padrão` | Tabela de log criada com outro formato | Conferir com `DESCRIBE TABLE`; recriar a partir do formato de `src/common/ingestion_log.py` |
| Regras da Silver reprovadas com `TABLE_OR_VIEW_NOT_FOUND` | Notebook de qualidade executado antes da Silver | Rodar antes `02_geosampa_flooding_events_silver` ou o job inteiro |
| `N regra(s) de severidade ERRO falharam` | Dados reprovados nas regras de qualidade | Consultar a última execução no `geosampa_data_quality_log`, corrigir a causa e rodar o job de novo |
| `Mais de um arquivo para o mesmo ano no volume` | Versão antiga e nova do mesmo ano no volume do GeoSampa | Apagar a versão antiga do volume e rodar de novo |
| Erro de identidade ao inserir o membro `-1` | Dimensão criada antes do PR #14, com `GENERATED ALWAYS` | Recriar a Gold do GeoSampa (ver [reprocessamento](reprocessamento.md), RP04) |
| `SGAA respondeu status ...` ou `token rejeitado` | Credenciais erradas ou expiradas no secret scope `cemaden` | Conferir as chaves `ped_email` e `ped_password` |
| `Contagem diferente do manifesto` | Arquivo da landing alterado ou incompleto | Recoletar a janela na landing e rodar a Bronze de novo |
| Coordenadas nulas na Silver do GeoSampa | Geometria em formato não previsto pela UDF | Conferir uma amostra de `geom_wkb` e ajustar a UDF da Silver |
