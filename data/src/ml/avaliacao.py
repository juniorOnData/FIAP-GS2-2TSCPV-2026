"""Métricas de avaliação temporal dos modelos (Spark), além do MAE e do RMSE do avaliador padrão."""

from pyspark.sql import functions as F


def erros(df, rotulo, previsao):
    """MAE e RMSE de `previsao` contra `rotulo`."""
    r = df.select(F.avg(F.abs(F.col(rotulo) - F.col(previsao))).alias("mae"),
                  F.sqrt(F.avg((F.col(rotulo) - F.col(previsao)) ** 2)).alias("rmse")).first()
    return {"mae": r["mae"], "rmse": r["rmse"]}


def ganho(mae_modelo, mae_referencia):
    """1 − MAE do modelo / MAE da referência: positivo quando o modelo erra menos que a referência."""
    if mae_modelo is None or not mae_referencia:
        return None
    return 1.0 - mae_modelo / mae_referencia


def deteccao(df, rotulo, previsao, limiar):
    """Acerto de evento (rótulo >= limiar): taxa de detecção (POD), taxa de falso alarme (FAR) e índice crítico (CSI)."""
    ev, prev = F.col(rotulo) >= limiar, F.col(previsao) >= limiar
    r = df.select(F.sum((ev & prev).cast("int")).alias("acertos"), F.sum((ev & ~prev).cast("int")).alias("perdas"),
                  F.sum((~ev & prev).cast("int")).alias("falsos")).first()
    a, p, f = r["acertos"] or 0, r["perdas"] or 0, r["falsos"] or 0
    return {"pod": a / (a + p) if a + p else None, "far": f / (a + f) if a + f else None,
            "csi": a / (a + p + f) if a + p + f else None, "eventos": a + p}


def captura_no_topo(df, pontuacao, rotulo, fracao=0.1):
    """Parte do total de `rotulo` que cai nas `fracao` linhas de maior `pontuacao` (por exemplo, as 10% de células
    mais bem ranqueadas). Mede a utilidade de um ranking, que é como o modelo de recorrência é usado."""
    total = df.agg(F.sum(rotulo)).first()[0] or 0
    n = df.count()
    if not total or not n:
        return None
    k = max(1, int(round(n * fracao)))
    topo = df.orderBy(F.desc(pontuacao), F.asc(F.monotonically_increasing_id())).limit(k).agg(F.sum(rotulo)).first()[0] or 0
    return topo / total


def por_segmento(df, segmento, rotulo, previsao, referencia):
    """MAE do modelo e da referência e o ganho, por segmento (por exemplo, por sensor)."""
    return (df.groupBy(segmento)
            .agg(F.avg(F.abs(F.col(rotulo) - F.col(previsao))).alias("mae"),
                 F.avg(F.abs(F.col(rotulo) - F.col(referencia))).alias("mae_referencia"), F.count("*").alias("n"))
            .withColumn("ganho", F.when(F.col("mae_referencia") > 0, 1 - F.col("mae") / F.col("mae_referencia"))))
