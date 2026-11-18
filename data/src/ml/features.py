"""Leitura e padronização das séries horárias Gold usadas pelos modelos."""

from pyspark.sql import functions as F


def _hora_local(fato, dim_tempo):
    return (
        fato.join(dim_tempo, "COD_DIM_TEMPO", "inner")
        .withColumn(
            "ts",
            F.to_timestamp(
                F.concat_ws(" ", F.date_format("DAT_DIA", "yyyy-MM-dd"), F.format_string("%02d:00:00", F.col("NUM_HORA"))),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
    )


def hourly_rainfall(spark):
    """Uma linha por estação, rede e hora, com chuva em mm e coordenadas."""
    tempo = spark.table("fiap_analytics.gold.dim_tempo").select("COD_DIM_TEMPO", "DAT_DIA")

    cemaden = (
        _hora_local(spark.table("fiap_analytics.gold.fact_cemaden_rainfall_hourly"), tempo)
        .groupBy("COD_ESTACAO", "COD_DIM_TEMPO", "NUM_HORA", "ts")
        .agg(F.max("NUM_CHUVA_MM").alias("rain_mm"))
        .join(
            spark.table("fiap_analytics.gold.dim_cemaden_stations").select(
                "COD_ESTACAO", "NUM_LATITUDE", "NUM_LONGITUDE"
            ),
            "COD_ESTACAO",
            "left",
        )
        .select(
            F.concat(F.lit("cemaden:"), "COD_ESTACAO").alias("station_id"), F.lit("cemaden").alias("network"),
            "ts", "NUM_LATITUDE", "NUM_LONGITUDE", "rain_mm",
        )
    )

    sibh = (
        _hora_local(spark.table("fiap_analytics.gold.fact_sibh_rainfall_hourly"), tempo)
        .join(
            spark.table("fiap_analytics.gold.dim_sibh_sensors")
            .where(F.col("DSC_GRANDEZA") == "Chuva")
            .select("COD_SENSOR", "NUM_LATITUDE", "NUM_LONGITUDE"),
            "COD_SENSOR",
            "left",
        )
        .select(
            F.concat(F.lit("sibh:"), "COD_SENSOR").alias("station_id"), F.lit("sibh").alias("network"),
            "ts", "NUM_LATITUDE", "NUM_LONGITUDE", F.col("NUM_CHUVA_MM").alias("rain_mm"),
        )
    )

    inmet = (
        _hora_local(spark.table("fiap_analytics.gold.fact_inmet_weather_hourly"), tempo)
        .join(
            spark.table("fiap_analytics.gold.dim_inmet_stations").select(
                "COD_ESTACAO", "NUM_LATITUDE", "NUM_LONGITUDE"
            ),
            "COD_ESTACAO",
            "left",
        )
        .select(
            F.concat(F.lit("inmet:"), "COD_ESTACAO").alias("station_id"), F.lit("inmet").alias("network"),
            "ts", "NUM_LATITUDE", "NUM_LONGITUDE", "NUM_CHUVA_MM",
        )
        .withColumnRenamed("NUM_CHUVA_MM", "rain_mm")
    )
    return (
        cemaden.unionByName(sibh).unionByName(inmet)
        .where(F.col("ts").isNotNull() & F.col("rain_mm").isNotNull())
        .where(F.col("NUM_LATITUDE").isNotNull() & F.col("NUM_LONGITUDE").isNotNull())
    )


def hourly_level(spark):
    """Uma linha por sensor e hora, preservando a unidade publicada para cada sensor."""
    tempo = spark.table("fiap_analytics.gold.dim_tempo").select("COD_DIM_TEMPO", "DAT_DIA")
    return (
        _hora_local(spark.table("fiap_analytics.gold.fact_sibh_level_hourly"), tempo)
        .where(F.col("COD_SENSOR").isNotNull() & (F.col("COD_DIM_SENSOR") != -1) & F.col("NUM_NIVEL_ULTIMO").isNotNull())
        .select("COD_SENSOR", "COD_DIM_TEMPO", "NUM_HORA", "ts", F.col("NUM_NIVEL_ULTIMO").alias("level"), "NUM_MEDICOES")
    )


def city_rainfall(spark):
    """Resumo horário da chuva da rede municipal; não interpreta ausência de leitura como zero."""
    return (
        hourly_rainfall(spark)
        .groupBy("ts")
        .agg(
            F.avg("rain_mm").alias("rain_mean_mm"), F.max("rain_mm").alias("rain_max_mm"),
            F.countDistinct("station_id").alias("rain_stations_reporting"),
        )
    )
