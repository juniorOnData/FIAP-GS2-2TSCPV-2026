"""Testes dos modelos com Spark local e dados sintéticos (exigem Java 17+). Rodar a partir da raiz:
    CHUVASEGURA_SPARK_TESTS=1 python -m unittest data.tests.test_ml_spark -v
Sem a variável de ambiente, são pulados (a suíte padrão continua rápida e sem Java)."""
import math
import os
import random
import sys
import unittest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

RODAR = os.environ.get("CHUVASEGURA_SPARK_TESTS") == "1"


@unittest.skipUnless(RODAR, "defina CHUVASEGURA_SPARK_TESTS=1 (exige Java 17+ e pyspark)")
class ModelosSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timeZone", "UTC").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # ------------------------------------------------------------ preparo
    def test_defasagem_em_horas_mesmo_com_falha(self):
        from pyspark.sql import Window
        from pyspark.sql import functions as F
        from ml.features import completar_horas, horas_desde_leitura
        t0 = datetime(2026, 1, 1)
        horas = [0, 1, 2, 10, 11]  # falha entre 2h e 10h
        df = self.spark.createDataFrame([("s", t0 + timedelta(hours=h), float(h)) for h in horas], "sensor_id string, ts timestamp, level double")
        g = horas_desde_leitura(completar_horas(df, "sensor_id", ["level"]), "sensor_id", "level")
        g = g.withColumn("lag3", F.lag("level", 3).over(Window.partitionBy("sensor_id").orderBy("ts")))
        linhas = {r["ts"]: r for r in g.collect()}
        self.assertEqual(len(linhas), 12)  # 0h a 11h
        self.assertIsNone(linhas[t0 + timedelta(hours=10)]["lag3"])  # 7h não tem leitura (antes era a leitura das 0h)
        self.assertEqual(linhas[t0 + timedelta(hours=10)]["hours_since_reading"], 8.0)

    def test_pares_proximos(self):
        from ml.features import pares_proximos
        pts = self.spark.createDataFrame([("a", -23.55, -46.63), ("b", -23.55, -46.62), ("c", -23.80, -46.63)], "id string, lat double, lon double")
        pares = {(r["id"], r["vizinho"]) for r in pares_proximos(pts, pts, 5).collect()}
        self.assertEqual(pares, {("a", "b"), ("b", "a")})  # ~1 km; c fica a ~28 km

    # ------------------------------------------------------------ modelo 1
    def test_recorrencia_preve_o_ano_seguinte_e_mede_o_ranking(self):
        from ml import recorrencia
        rng = random.Random(1)
        quentes = [(-23.505 + 0.01 * i, -46.605) for i in range(5)]
        frios = [(-23.705 + 0.01 * i, -46.805) for i in range(30)]
        enchentes, todos = [], []
        for ano in range(2015, 2026):
            for lat, lon in quentes:
                for _ in range(rng.randint(6, 10)):
                    enchentes.append((ano, lat, lon))
            for lat, lon in frios:
                todos.append((ano, lat, lon))  # só quedas de árvore: células com cobertura e sem alagamento
                if rng.random() < 0.05:
                    enchentes.append((ano, lat, lon))
        todos += enchentes
        esquema = "year int, lat double, lon double"
        r = recorrencia.treinar(self.spark.createDataFrame(enchentes, esquema), self.spark.createDataFrame(todos, esquema), 2025)
        self.assertEqual(r["ano_previsto"], 2026)
        self.assertEqual({x["year"] for x in r["previsao"].collect()}, {2026})
        self.assertEqual(r["metricas"]["cells"], 35)  # as células sem alagamento também entram no teste
        self.assertGreater(r["metricas"]["top10_capture"], 0.5)  # as 4 primeiras células (10% de 35) são quentes
        topo = [x["cell_id"] for x in r["previsao"].orderBy("rank").limit(5).collect()]
        self.assertTrue(all(c.startswith("-2351") or c.startswith("-2350") or c.startswith("-2349") or c.startswith("-2348") or c.startswith("-2347") for c in topo), topo)

    # ------------------------------------------------------------ modelos 2 e 3
    def _chuva_e_nivel(self):
        rng = random.Random(2)
        t0, n = datetime(2026, 1, 1), 24 * 140
        chuva_cidade = []
        for i in range(n):
            chuva_cidade.append(max(0.0, rng.gauss(-3, 4)) * (1 if (i // 24) % 5 == 0 else 0.2))
        estacoes = {"sibh:1": (-23.55, -46.63), "sibh:2": (-23.56, -46.64), "sibh:3": (-23.54, -46.62), "inmet:A": (-23.50, -46.60)}
        chuva = []
        for sid, (lat, lon) in estacoes.items():
            fim = n - 24 * 10 if sid == "sibh:3" else n  # sibh:3 parou 10 dias antes do fim
            for i in range(fim):
                if rng.random() < 0.03:
                    continue  # falhas
                chuva.append((sid, sid.split(":")[0], t0 + timedelta(hours=i), lat, lon, round(chuva_cidade[i] * rng.uniform(0.7, 1.3), 1)))
        niveis = []
        for sid in ("10", "20"):
            nivel = 72000.0
            for i in range(n):
                nivel += 6 * chuva_cidade[i - 1] if i else 0
                nivel -= (nivel - 72000.0) * 0.08
                if rng.random() < 0.02:
                    continue
                niveis.append((sid, t0 + timedelta(hours=i), round(nivel + rng.gauss(0, 0.5), 2)))
        sensores = [("10", -23.551, -46.631), ("20", -23.70, -46.80)]  # o 20 não tem pluviômetro a 10 km
        sp = self.spark
        return (sp.createDataFrame(chuva, "station_id string, network string, ts timestamp, lat double, lon double, rain_mm double"),
                sp.createDataFrame(niveis, "sensor_id string, ts timestamp, level double"),
                sp.createDataFrame(sensores, "sensor_id string, lat double, lon double"))

    def test_nivel_treina_os_dois_horizontes_e_mede_por_sensor(self):
        from ml import nivel
        chuva, niveis, sensores = self._chuva_e_nivel()
        r = nivel.treinar(nivel.montar_base(niveis, chuva, sensores), dias_teste=30)
        self.assertEqual([x["horizonte"] for x in r["resultados"]], [1, 3])
        for x in r["resultados"]:
            self.assertIsNotNone(x["metricas"]["mae_delta"])
            self.assertEqual({s["sensor_id"] for s in x["segmentos"]}, {"10", "20"})
            self.assertGreater(x["metricas"]["skill_vs_persistence"], 0)  # o nível responde à chuva: o modelo deve ganhar
            self.assertEqual({p["horizon_hours"] for p in x["previsao"].collect()}, {x["horizonte"]})

    def test_chuva_nao_preve_estacao_parada_e_traz_as_referencias(self):
        from ml import chuva as modelo_chuva
        chuva, _, _ = self._chuva_e_nivel()
        r = modelo_chuva.treinar(modelo_chuva.montar_base(chuva), dias_teste=30)
        estacoes = {p["station_id"] for p in r["previsao"].collect()}
        self.assertNotIn("sibh:3", estacoes)
        self.assertEqual(estacoes, {"sibh:1", "sibh:2", "inmet:A"})
        for k in ("mae_mm", "persistence_mae_mm", "zero_mae_mm", "wet_mae_mm", "model_csi_1mm", "persistence_csi_1mm"):
            self.assertIn(k, r["metricas"])
        self.assertTrue(0 <= r["metricas"]["wet_hour_rate"] <= 1)


if __name__ == "__main__":
    unittest.main()
