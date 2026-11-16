-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02_geosampa_flooding_events_silver
-- MAGIC
-- MAGIC Recria a Silver das ocorrências do GeoSampa: data tipada, tipo de ocorrência padronizado, subprefeitura relacionada pela sigla do GeoSampa (ou, se não houver, pela sigla do Anexo IX) e localização do ponto.
-- MAGIC
-- MAGIC Localização: a Bronze guarda a geometria em WKB (`geom_wkb`), um POINT em SIRGAS 2000 / UTM 23S (`_source_srs_id = 31983`, lido dos metadados do GeoPackage). A Silver mantém o WKB e acrescenta as coordenadas UTM (metros, boas para calcular distâncias) e a latitude e longitude em graus (SIRGAS 2000, equivalente ao WGS84 para este uso).

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # O Spark SQL não tem função para ler os números de dentro do WKB, então a conversão é uma UDF Python.
-- MAGIC # A função fica no próprio notebook para ser enviada inteira aos executores.
-- MAGIC import math
-- MAGIC import struct
-- MAGIC
-- MAGIC # SIRGAS 2000 / UTM zona 23S (EPSG:31983): elipsoide GRS80, meridiano central -45°.
-- MAGIC UTM_A = 6378137.0
-- MAGIC UTM_F = 1 / 298.257222101
-- MAGIC UTM_K0 = 0.9996
-- MAGIC UTM_LON0 = math.radians(-45.0)
-- MAGIC UTM_FALSO_LESTE, UTM_FALSO_NORTE = 500000.0, 10000000.0
-- MAGIC
-- MAGIC
-- MAGIC def utm23s_para_latlon(x, y):
-- MAGIC     """Converte coordenadas UTM 23S (metros) em (latitude, longitude) em graus. Fórmulas inversas de Snyder (USGS)."""
-- MAGIC     e2 = UTM_F * (2 - UTM_F)
-- MAGIC     ep2 = e2 / (1 - e2)
-- MAGIC     e1 = (1 - math.sqrt(1 - e2)) / (1 + math.sqrt(1 - e2))
-- MAGIC     m = (y - UTM_FALSO_NORTE) / UTM_K0
-- MAGIC     mu = m / (UTM_A * (1 - e2 / 4 - 3 * e2 ** 2 / 64 - 5 * e2 ** 3 / 256))
-- MAGIC     phi1 = (mu + (3 * e1 / 2 - 27 * e1 ** 3 / 32) * math.sin(2 * mu)
-- MAGIC             + (21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32) * math.sin(4 * mu)
-- MAGIC             + (151 * e1 ** 3 / 96) * math.sin(6 * mu)
-- MAGIC             + (1097 * e1 ** 4 / 512) * math.sin(8 * mu))
-- MAGIC     sen, cos, tg = math.sin(phi1), math.cos(phi1), math.tan(phi1)
-- MAGIC     c1 = ep2 * cos ** 2
-- MAGIC     t1 = tg ** 2
-- MAGIC     n1 = UTM_A / math.sqrt(1 - e2 * sen ** 2)
-- MAGIC     r1 = UTM_A * (1 - e2) / (1 - e2 * sen ** 2) ** 1.5
-- MAGIC     d = (x - UTM_FALSO_LESTE) / (n1 * UTM_K0)
-- MAGIC     lat = phi1 - (n1 * tg / r1) * (
-- MAGIC         d ** 2 / 2
-- MAGIC         - (5 + 3 * t1 + 10 * c1 - 4 * c1 ** 2 - 9 * ep2) * d ** 4 / 24
-- MAGIC         + (61 + 90 * t1 + 298 * c1 + 45 * t1 ** 2 - 252 * ep2 - 3 * c1 ** 2) * d ** 6 / 720
-- MAGIC     )
-- MAGIC     lon = UTM_LON0 + (
-- MAGIC         d
-- MAGIC         - (1 + 2 * t1 + c1) * d ** 3 / 6
-- MAGIC         + (5 - 2 * c1 + 28 * t1 - 3 * c1 ** 2 + 8 * ep2 + 24 * t1 ** 2) * d ** 5 / 120
-- MAGIC     ) / cos
-- MAGIC     return math.degrees(lat), math.degrees(lon)
-- MAGIC
-- MAGIC
-- MAGIC def ponto_wkb_para_coordenadas(wkb, srs_id):
-- MAGIC     """WKB de um POINT em EPSG:31983 -> (x, y, latitude, longitude). Outros tipos ou SRIDs devolvem None."""
-- MAGIC     if wkb is None or srs_id != 31983:
-- MAGIC         return None
-- MAGIC     dados = bytes(wkb)
-- MAGIC     if len(dados) < 21 or dados[0] not in (0, 1):
-- MAGIC         return None
-- MAGIC     ordem = "<" if dados[0] == 1 else ">"
-- MAGIC     (tipo,) = struct.unpack(ordem + "I", dados[1:5])
-- MAGIC     if tipo != 1:  # só POINT 2D
-- MAGIC         return None
-- MAGIC     x, y = struct.unpack(ordem + "dd", dados[5:21])
-- MAGIC     if math.isnan(x) or math.isnan(y):  # POINT EMPTY
-- MAGIC         return None
-- MAGIC     lat, lon = utm23s_para_latlon(x, y)
-- MAGIC     return x, y, lat, lon
-- MAGIC
-- MAGIC
-- MAGIC spark.udf.register(
-- MAGIC     "geosampa_ponto_coordenadas",
-- MAGIC     ponto_wkb_para_coordenadas,
-- MAGIC     "struct<x_utm:double, y_utm:double, latitude:double, longitude:double>",
-- MAGIC )

-- COMMAND ----------

CREATE OR REPLACE TABLE fiap_analytics.silver.fact_geosampa_flooding_events AS
  WITH history_flooding_events AS (
    SELECT
      CAST(data AS DATE) AS dt_ocorrencia,
      CAST(DATE_FORMAT(CAST(data AS DATE), "yyyyMMdd") AS BIGINT) AS cod_dim_data,
      CASE
        WHEN ocorrencia IN ("ALAGAMENTO") THEN "Alagamento"
        WHEN ocorrencia IN ("DESLIZAMENTO") THEN "Deslizamento"
        WHEN ocorrencia IN ("INUNDACAO") THEN "Inundação"
        WHEN ocorrencia IN ("QUEDA DE ARVORE") THEN "Queda de Árvore"
        ELSE "Outros"
      END AS dc_tipo_ocorrencia,
      LEFT(COALESCE(subprefeit, subpreit), 2) AS sg_subprefeitura,
      COALESCE(subprefeit, subpreit) AS nm_subprefeitura,
      geom_wkb,
      geosampa_ponto_coordenadas(geom_wkb, _source_srs_id) AS coordenadas,
      _source_file,
      _source_layer,
      _source_year,
      _source_sha256,
      _source_srs_id,
      _ingested_at
    FROM
      fiap_analytics.bronze.geosampa_history_flooding_events
  ),
  ajuste_subprefecture AS (
    SELECT
      dt_ocorrencia,
      cod_dim_data,
      dc_tipo_ocorrencia,
      -- Só corrige variações de grafia. A troca da sigla do GeoSampa pela do Anexo IX (ex.: FO para FB)
      -- vem da coluna sg_subprefeitura_geosampa da tabela de subprefeituras.
      CASE
        WHEN sg_subprefeitura IN ("G ", "G-", "GU") THEN "G"
        WHEN sg_subprefeitura IN ("NA") THEN NULL
        ELSE sg_subprefeitura
      END AS sg_subprefeitura_geosampa,
      nm_subprefeitura,
      geom_wkb,
      coordenadas,
      _source_file,
      _source_layer,
      _source_year,
      _source_sha256,
      _source_srs_id,
      _ingested_at
    FROM
      history_flooding_events
  )
  SELECT
    f.dt_ocorrencia,
    f.cod_dim_data,
    f.dc_tipo_ocorrencia,
    COALESCE(g.sg_subprefeitura, a.sg_subprefeitura, f.sg_subprefeitura_geosampa) AS sg_subprefeitura,
    COALESCE(g.nm_subprefeitura, a.nm_subprefeitura) AS nm_subprefeitura,
    f.coordenadas.x_utm AS nr_x_utm,
    f.coordenadas.y_utm AS nr_y_utm,
    f.coordenadas.latitude AS nr_latitude,
    f.coordenadas.longitude AS nr_longitude,
    f.geom_wkb,
    f._source_file,
    f._source_layer,
    f._source_year,
    f._source_sha256,
    f._source_srs_id,
    f._ingested_at
  FROM
    ajuste_subprefecture f
      -- Primeiro pela sigla do GeoSampa; se não achar, pela sigla do Anexo IX (arquivos que já usam a sigla oficial).
      LEFT JOIN fiap_analytics.bronze.geosampa_subprefecture_area g
        ON f.sg_subprefeitura_geosampa = g.sg_subprefeitura_geosampa
      LEFT JOIN fiap_analytics.bronze.geosampa_subprefecture_area a
        ON g.sg_subprefeitura IS NULL AND f.sg_subprefeitura_geosampa = a.sg_subprefeitura

-- COMMAND ----------

-- Conferência da localização: pontos sem coordenada e fora da área de São Paulo e entorno.
SELECT
  COUNT(*)                                                         AS ocorrencias,
  COUNT_IF(nr_latitude IS NULL)                                    AS sem_coordenada,
  COUNT_IF(nr_latitude NOT BETWEEN -24.1 AND -23.3
           OR nr_longitude NOT BETWEEN -47.0 AND -46.3)            AS fora_da_area,
  COUNT(DISTINCT nr_x_utm, nr_y_utm)                               AS pontos_distintos,
  MIN(nr_latitude)                                                 AS lat_min,
  MAX(nr_latitude)                                                 AS lat_max,
  MIN(nr_longitude)                                                AS lon_min,
  MAX(nr_longitude)                                                AS lon_max
FROM fiap_analytics.silver.fact_geosampa_flooding_events
