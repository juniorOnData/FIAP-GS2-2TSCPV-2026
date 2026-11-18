"""Gravação das execuções e métricas nas tabelas de rastreio do schema fiap_analytics.ml."""

ML = "fiap_analytics.ml"
METRICAS = ("run_id string, model_id string, horizon_hours int, split string, segment string, metric string, "
            "value double, computed_at timestamp")
EXECUCOES = ("run_id string, model_id string, model_version string, status string, started_at timestamp, "
             "training_start timestamp, training_end timestamp, test_start timestamp, test_end timestamp, "
             "artifact_path string, details string")


def linha_metrica(run_id, model_id, horizonte, split, metrica, valor, quando, segmento=None):
    return (run_id, model_id, horizonte, split, segmento, metrica, None if valor is None else float(valor), quando)


def gravar_metricas(spark, linhas):
    if linhas:
        spark.createDataFrame(linhas, METRICAS).write.mode("append").saveAsTable(f"{ML}.model_metrics")


def gravar_execucao(spark, run_id, model_id, versao, status, inicio, treino=(None, None), teste=(None, None),
                    artefato=None, detalhes=None):
    linha = [(run_id, model_id, versao, status, inicio, treino[0], treino[1], teste[0], teste[1], artefato,
              None if detalhes is None else str(detalhes)[:4000])]
    spark.createDataFrame(linha, EXECUCOES).write.mode("append").saveAsTable(f"{ML}.model_runs")
