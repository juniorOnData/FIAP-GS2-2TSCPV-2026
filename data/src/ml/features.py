"""Leitura e preparo das séries horárias Gold usadas pelos modelos.

As funções de leitura recebem a sessão Spark; as de preparo recebem e devolvem DataFrames, para poderem ser testadas
com dados sintéticos fora do Databricks (data/tests/test_ml_spark.py).
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

GOLD = "fiap_analytics.gold"
REDES_CHUVA = ("cemaden", "sibh", "inmet")
# Fora dos modelos até a revisão da Gold: a chuva horária do CEMADEN tem valores incompatíveis com as estações vizinhas
# (até 314 mm em 1 h e centenas de horas com exatamente 1,0 mm). Mesmo critério do app (app/assets/js/regras-alerta.js).
REDES_EM_REVISAO = ("cemaden",)
REDES_PADRAO = tuple(r for r in REDES_CHUVA if r not in REDES_EM_REVISAO)

# Escala local de São Paulo (km por grau), suficiente para distâncias de poucos quilômetros.
KM_POR_GRAU_LON = 102.05
KM_POR_GRAU_LAT = 110.78


def _hora_local(fato, dim_tempo):
    return fato.join(dim_tempo, "COD_DIM_TEMPO", "inner").withColumn(
        "ts",
        F.to_timestamp(
            F.concat_ws(" ", F.date_format("DAT_DIA", "yyyy-MM-dd"), F.format_string("%02d:00:00", F.col("NUM_HORA"))),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )


def _chuva_da_rede(spark, rede, tempo):
    if rede == "cemaden":
        return (
            _hora_local(spark.table(f"{GOLD}.fact_cemaden_rainfall_hourly"), tempo)
            .groupBy("COD_ESTACAO", "ts").agg(F.max("NUM_CHUVA_MM").alias("rain_mm"))  # sensores da mesma estação: maior valor
            .join(spark.table(f"{GOLD}.dim_cemaden_stations").select("COD_ESTACAO", "NUM_LATITUDE", "NUM_LONGITUDE"), "COD_ESTACAO", "left")
            .select(F.concat(F.lit("cemaden:"), "COD_ESTACAO").alias("station_id"), "ts", "NUM_LATITUDE", "NUM_LONGITUDE", "rain_mm")
        )
    if rede == "sibh":
        return (
            _hora_local(spark.table(f"{GOLD}.fact_sibh_rainfall_hourly"), tempo)
            .join(spark.table(f"{GOLD}.dim_sibh_sensors").where(F.col("DSC_GRANDEZA") == "Chuva")
                  .select("COD_SENSOR", "NUM_LATITUDE", "NUM_LONGITUDE"), "COD_SENSOR", "left")
            .select(F.concat(F.lit("sibh:"), "COD_SENSOR").alias("station_id"), "ts", "NUM_LATITUDE", "NUM_LONGITUDE",
                    F.col("NUM_CHUVA_MM").alias("rain_mm"))
        )
    if rede == "inmet":
        return (
            _hora_local(spark.table(f"{GOLD}.fact_inmet_weather_hourly"), tempo)
            .join(spark.table(f"{GOLD}.dim_inmet_stations").select("COD_ESTACAO", "NUM_LATITUDE", "NUM_LONGITUDE"), "COD_ESTACAO", "left")
            .select(F.concat(F.lit("inmet:"), "COD_ESTACAO").alias("station_id"), "ts", "NUM_LATITUDE", "NUM_LONGITUDE",
                    F.col("NUM_CHUVA_MM").alias("rain_mm"))
        )
    raise ValueError(f"Rede desconhecida: {rede}")


def hourly_rainfall(spark, redes=REDES_PADRAO):
    """Uma linha por estação e hora com leitura: station_id, network, ts, lat, lon, rain_mm."""
    tempo = spark.table(f"{GOLD}.dim_tempo").select("COD_DIM_TEMPO", "DAT_DIA")
    partes = [_chuva_da_rede(spark, r, tempo).withColumn("network", F.lit(r)) for r in redes]
    df = partes[0]
    for p in partes[1:]:
        df = df.unionByName(p)
    return (df.where(F.col("ts").isNotNull() & F.col("rain_mm").isNotNull() & F.col("NUM_LATITUDE").isNotNull() & F.col("NUM_LONGITUDE").isNotNull())
            .select("station_id", "network", "ts", F.col("NUM_LATITUDE").alias("lat"), F.col("NUM_LONGITUDE").alias("lon"), "rain_mm"))


def hourly_level(spark):
    """Uma linha por sensor e hora com leitura: sensor_id, ts, level (cota na unidade da Gold, cm no SIBH)."""
    tempo = spark.table(f"{GOLD}.dim_tempo").select("COD_DIM_TEMPO", "DAT_DIA")
    return (
        _hora_local(spark.table(f"{GOLD}.fact_sibh_level_hourly"), tempo)
        .where(F.col("COD_SENSOR").isNotNull() & (F.col("COD_DIM_SENSOR") != -1) & F.col("NUM_NIVEL_ULTIMO").isNotNull())
        .select(F.col("COD_SENSOR").cast("string").alias("sensor_id"), "ts", F.col("NUM_NIVEL_ULTIMO").alias("level"))
    )


def level_sensors(spark):
    """Coordenadas dos sensores de nível: sensor_id, lat, lon."""
    return (spark.table(f"{GOLD}.dim_sibh_sensors").where((F.col("DSC_GRANDEZA") == "Nível") & (F.col("COD_DIM_SENSOR") != -1))
            .select(F.col("COD_SENSOR").cast("string").alias("sensor_id"), F.col("NUM_LATITUDE").alias("lat"), F.col("NUM_LONGITUDE").alias("lon"))
            .where(F.col("lat").isNotNull() & F.col("lon").isNotNull()))


# ---------------------------------------------------------------- preparo (testável)

def completar_horas(df, chave, colunas):
    """Grade horária completa por `chave`, do primeiro ao último `ts` de cada uma; hora sem leitura fica nula.
    Com a grade, a defasagem k é sempre k horas, e não a k-ésima leitura anterior (que, depois de uma falha do sensor,
    pode ser de dias antes)."""
    limites = df.groupBy(chave).agg(F.min("ts").alias("_ini"), F.max("ts").alias("_fim"))
    grade = limites.select(chave, F.explode(F.sequence("_ini", "_fim", F.expr("INTERVAL 1 HOUR"))).alias("ts"))
    return grade.join(df.select(chave, "ts", *colunas), [chave, "ts"], "left")


def horas_desde_leitura(df, chave, coluna, nome="hours_since_reading"):
    """Horas desde a leitura anterior não nula de `coluna` (nula se não houver), sobre a grade horária."""
    janela = Window.partitionBy(chave).orderBy("ts").rowsBetween(Window.unboundedPreceding, -1)
    anterior = F.last(F.when(F.col(coluna).isNotNull(), F.col("ts")), ignorenulls=True).over(janela)
    return df.withColumn(nome, (F.unix_timestamp("ts") - F.unix_timestamp(anterior)) / 3600.0)


def pares_proximos(origem, destino, raio_km, id_origem="id", id_destino="vizinho"):
    """Pares (id_origem, id_destino, dist_km) a até `raio_km`, sem o próprio ponto. Os dois DataFrames têm id, lat, lon."""
    a = origem.select(F.col("id").alias(id_origem), F.col("lat").alias("_la"), F.col("lon").alias("_oa"))
    b = destino.select(F.col("id").alias(id_destino), F.col("lat").alias("_lb"), F.col("lon").alias("_ob"))
    dist = F.sqrt(((F.col("_oa") - F.col("_ob")) * KM_POR_GRAU_LON) ** 2 + ((F.col("_la") - F.col("_lb")) * KM_POR_GRAU_LAT) ** 2)
    return (a.crossJoin(b).where(F.col(id_origem) != F.col(id_destino)).withColumn("dist_km", dist)
            .where(F.col("dist_km") <= raio_km).select(id_origem, id_destino, "dist_km"))


def chuva_vizinha(chuva, pares, id_origem="id", prefixo="near"):
    """Por ponto de origem e hora: média, máximo e quantidade de estações vizinhas com leitura.
    chuva: station_id, ts, rain_mm; pares: saída de pares_proximos (vizinho = station_id)."""
    return (pares.join(chuva.select(F.col("station_id").alias("vizinho"), "ts", "rain_mm"), "vizinho")
            .groupBy(id_origem, "ts")
            .agg(F.avg("rain_mm").alias(f"{prefixo}_rain_mean_mm"), F.max("rain_mm").alias(f"{prefixo}_rain_max_mm"),
                 F.count("rain_mm").alias(f"{prefixo}_stations_reporting")))


def chuva_da_cidade(chuva):
    """Resumo horário das estações com leitura (sem tratar ausência como zero)."""
    return chuva.groupBy("ts").agg(F.avg("rain_mm").alias("city_rain_mean_mm"), F.max("rain_mm").alias("city_rain_max_mm"),
                                    F.countDistinct("station_id").alias("city_stations_reporting"))
