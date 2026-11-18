"""Testes das funções puras do export do site. Rodar a partir da raiz do repositório:
    python -m unittest discover -s data/tests -v
"""
import base64
import hashlib
import json
import os
import sys
import threading
import unittest
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from common import github_publish  # noqa: E402
from common.publicacao_web import (  # noqa: E402
    FATOR_PARA_METRO, M_POR_GRAU_LAT, M_POR_GRAU_LON, TZ_SP, IndicePoligonos, anel_web, area_uniao_por_grau, linhas_web,
    poligonos_web, simplificar, data_para_cod, hora_sp, iso_sp, pior_qualidade, serie_diaria, serie_horaria, sha256_dados,
)


class SeriesTest(unittest.TestCase):
    def test_diaria_preenche_lacunas_com_nulo(self):
        d0 = date(2026, 1, 1)
        out = serie_diaria([("a", 20260101, 1.234), ("a", 20260103, 0.0), ("b", 20260102, 5)], date(2026, 1, 4), dia_inicial=d0)
        self.assertEqual(out["a"], [1.2, None, 0.0, None])  # dia sem leitura é nulo; zero é zero
        self.assertEqual(out["b"], [None, 5.0, None, None])

    def test_diaria_ignora_fora_do_intervalo_e_nulos(self):
        out = serie_diaria([("a", 20251231, 9), ("a", 20260105, 9), ("a", 20260101, None)], date(2026, 1, 3), dia_inicial=date(2026, 1, 1))
        self.assertEqual(out, {})

    def test_horaria_janela_termina_na_hora_final(self):
        final = hora_sp(20260110, 9)
        inicio, out = serie_horaria([("a", 20260110, 9, 2.0), ("a", 20260110, 8, 1.0), ("a", 20260103, 9, 7.0), ("a", 20260103, 10, 3.0)], final, horas=168)
        self.assertEqual(inicio, final - timedelta(hours=167))
        s = out["a"]
        self.assertEqual(len(s), 168)
        self.assertEqual(s[-1], 2.0)
        self.assertEqual(s[-2], 1.0)
        self.assertIsNone(s[-3])
        # a janela começa em 03/01 10:00 (primeira posição); 03/01 09:00 já está fora
        self.assertEqual(s[0], 3.0)
        self.assertEqual(sum(v is not None for v in s), 3)

    def test_horaria_virada_de_dia(self):
        final = hora_sp(20260102, 1)
        _, out = serie_horaria([("a", 20260101, 23, 4.0), ("a", 20260102, 0, 5.0)], final, horas=4)
        self.assertEqual(out["a"], [None, 4.0, 5.0, None])

    def test_fuso_e_iso(self):
        self.assertEqual(iso_sp(hora_sp(20260117, 9)), "2026-01-17T09:00:00-03:00")
        self.assertEqual(data_para_cod(date(2026, 11, 17)), 20261117)

    def test_sha_independe_da_ordem_das_chaves_de_objeto_igual(self):
        self.assertEqual(sha256_dados({"a": [1, None]}), sha256_dados({"a": [1, None]}))
        self.assertNotEqual(sha256_dados({"a": [1, None]}), sha256_dados({"a": [1, 0]}))

    def test_nan_e_rejeitado(self):
        with self.assertRaises(ValueError):
            sha256_dados({"a": float("nan")})

    def test_cota_do_sibh_em_cm_vira_metro(self):
        self.assertAlmostEqual(72869.058 * FATOR_PARA_METRO["cm"], 728.69058)
        self.assertEqual(FATOR_PARA_METRO["m"], 1.0)

    def test_pior_qualidade(self):
        self.assertEqual(pior_qualidade(["aprovado", "aprovado_com_avisos"]), "aprovado_com_avisos")
        self.assertEqual(pior_qualidade(["aprovado"]), "aprovado")


def quadrado(x0, y0, lado_m):
    """Anel fechado de um quadrado com lado em metros, a partir de (lon, lat)."""
    dx, dy = lado_m / M_POR_GRAU_LON, lado_m / M_POR_GRAU_LAT
    return [(x0, y0), (x0 + dx, y0), (x0 + dx, y0 + dy), (x0, y0 + dy), (x0, y0)]


class GeometriaTest(unittest.TestCase):
    X, Y = -46.6, -23.6

    def test_simplificar_remove_pontos_quase_alinhados(self):
        reta = [(self.X + i * 1e-4, self.Y + (0.5e-6 if i % 2 else 0)) for i in range(11)]  # desvio de ~5 cm
        self.assertEqual(simplificar(reta, 1.0), [reta[0], reta[-1]])
        curva = [(self.X, self.Y), (self.X + 1e-3, self.Y + 1e-3), (self.X + 2e-3, self.Y)]  # ~110 m de desvio
        self.assertEqual(len(simplificar(curva, 10.0)), 3)

    def test_anel_web_fecha_arredonda_e_descarta_o_que_some(self):
        anel = anel_web(quadrado(self.X, self.Y, 200), 5.0)
        self.assertEqual(anel[0], anel[-1])
        self.assertEqual(len(anel), 5)
        self.assertTrue(all(len(str(v).split(".")[1]) <= 5 for p in anel for v in p))
        self.assertIsNone(anel_web(quadrado(self.X, self.Y, 0.3), 5.0))  # menor que a precisão de 5 casas

    def test_poligonos_web_mantem_buraco_e_descarta_buraco_minusculo(self):
        externo = quadrado(self.X, self.Y, 1000)
        buraco = quadrado(self.X + 0.002, self.Y + 0.002, 200)
        mini = quadrado(self.X + 0.005, self.Y + 0.005, 0.3)
        out = poligonos_web(("Polygon", [externo, buraco, mini]), 5.0)
        self.assertEqual(len(out), 1)
        self.assertEqual(len(out[0]), 2)

    def test_linhas_web(self):
        out = linhas_web(("MultiLineString", [[(self.X, self.Y), (self.X + 1e-3, self.Y)], [(self.X, self.Y)]]), 5.0)
        self.assertEqual(out, [[[self.X, self.Y], [round(self.X + 1e-3, 5), self.Y]]])

    def test_indice_respeita_buraco_e_devolve_maior_grau(self):
        baixa = [[quadrado(self.X, self.Y, 1000), quadrado(self.X + 0.003, self.Y + 0.003, 300)]]
        alta = [[quadrado(self.X + 0.001, self.Y + 0.001, 100)]]
        ind = IndicePoligonos([(1, baixa), (3, alta)])
        self.assertEqual(ind.maior_valor(self.X + 0.0015, self.Y + 0.0015), 3)  # dentro das duas
        self.assertEqual(ind.maior_valor(self.X + 0.006, self.Y + 0.006), 1)
        self.assertEqual(ind.maior_valor(self.X + 0.0045, self.Y + 0.0045), 0)  # no buraco
        self.assertEqual(ind.maior_valor(self.X - 0.01, self.Y), 0)  # fora

    def test_area_uniao_conta_sobreposicao_uma_vez_no_maior_grau(self):
        a = [[quadrado(self.X, self.Y, 1000)]]  # 1 km²
        b = [[quadrado(self.X + 500 / M_POR_GRAU_LON, self.Y, 1000)]]  # 1 km², metade sobre a
        alta = [[quadrado(self.X, self.Y, 500)]]  # 0,25 km² dentro de a
        area = area_uniao_por_grau([(1, a), (1, b), (3, alta)], passo_m=5.0)
        self.assertAlmostEqual(area[3], 0.25, places=2)
        self.assertAlmostEqual(area[1], 1.5 - 0.25, places=2)

    def test_area_uniao_desconta_buraco(self):
        pol = [[quadrado(self.X, self.Y, 1000), quadrado(self.X + 0.002, self.Y + 0.002, 500)]]
        self.assertAlmostEqual(area_uniao_por_grau([(2, pol)], passo_m=5.0)[2], 0.75, places=2)


class FakeGitHub(BaseHTTPRequestHandler):
    estado = {}

    def log_message(self, *a):
        pass

    def _enviar(self, codigo, obj):
        corpo = json.dumps(obj).encode()
        self.send_response(codigo)
        self.send_header("Content-Length", str(len(corpo)))
        self.end_headers()
        self.wfile.write(corpo)

    def do_GET(self):
        e = self.estado
        if self.headers.get("Authorization") != "Bearer tok":
            return self._enviar(401, {})
        if self.path.endswith("/git/ref/heads/main"):
            return self._enviar(200, {"object": {"sha": e["commit"]}})
        if "/git/commits/" in self.path:
            return self._enviar(200, {"tree": {"sha": e["arvore"]}})
        if "/contents/" in self.path:
            caminho = self.path.split("/contents/")[1].split("?")[0]
            if caminho in e["arquivos"]:
                return self._enviar(200, {"encoding": "base64", "content": base64.b64encode(e["arquivos"][caminho]).decode(), "sha": "x"})
            return self._enviar(404, {"message": "Not Found"})
        self._enviar(404, {})

    def do_POST(self):
        e = self.estado
        corpo = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path.endswith("/git/blobs"):
            conteudo = base64.b64decode(corpo["content"])
            sha = hashlib.sha1(conteudo).hexdigest()
            e["blobs"][sha] = conteudo
            return self._enviar(201, {"sha": sha})
        if self.path.endswith("/git/trees"):
            novo = dict(e["arvore_itens"])
            for it in corpo["tree"]:
                novo[it["path"]] = it["sha"]
            sha = hashlib.sha1(json.dumps(novo, sort_keys=True).encode()).hexdigest()
            e["arvores"][sha] = novo
            return self._enviar(201, {"sha": sha})
        if self.path.endswith("/git/commits"):
            e["commits_criados"].append(corpo)
            return self._enviar(201, {"sha": "c" + str(len(e["commits_criados"]))})
        self._enviar(404, {})

    def do_PATCH(self):
        corpo = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.estado["ref_atualizada"] = corpo
        self._enviar(200, {})


class GitHubTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = HTTPServer(("127.0.0.1", 0), FakeGitHub)
        cls.api = f"http://127.0.0.1:{cls.srv.server_port}"
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def setUp(self):
        itens = {"app/index.html": "sha-index"}
        FakeGitHub.estado = {"commit": "c0", "arvore": hashlib.sha1(json.dumps(itens, sort_keys=True).encode()).hexdigest(),
                             "arvore_itens": itens, "blobs": {}, "arvores": {}, "commits_criados": [], "arquivos": {}}

    def test_ler_json_ausente_retorna_none(self):
        self.assertIsNone(github_publish.ler_json("tok", "o/r", "main", "app/data/gold/manifest.json", api=self.api))

    def test_ler_json_existente(self):
        FakeGitHub.estado["arquivos"]["app/data/gold/manifest.json"] = json.dumps({"modo": "gold"}).encode()
        self.assertEqual(github_publish.ler_json("tok", "o/r", "main", "app/data/gold/manifest.json", api=self.api), {"modo": "gold"})

    def test_publica_e_nao_força(self):
        sha = github_publish.publicar("tok", "o/r", "main", {"app/data/gold/a.json": b'{"x":1}'}, "msg", api=self.api)
        self.assertEqual(sha, "c1")
        e = FakeGitHub.estado
        self.assertEqual(e["commits_criados"][0]["parents"], ["c0"])
        self.assertEqual(e["commits_criados"][0]["message"], "msg")
        self.assertEqual(e["ref_atualizada"], {"sha": "c1", "force": False})

    def test_sem_mudanca_nao_cria_commit(self):
        e = FakeGitHub.estado
        conteudo = b'{"x":1}'
        e["arvore_itens"]["app/data/gold/a.json"] = hashlib.sha1(conteudo).hexdigest()
        e["arvore"] = hashlib.sha1(json.dumps(e["arvore_itens"], sort_keys=True).encode()).hexdigest()
        sha = github_publish.publicar("tok", "o/r", "main", {"app/data/gold/a.json": conteudo}, "msg", api=self.api)
        self.assertIsNone(sha)
        self.assertEqual(e["commits_criados"], [])

    def test_token_invalido_levanta_erro_sem_vazar_token(self):
        with self.assertRaises(github_publish.GitHubErro) as c:
            github_publish.publicar("ruim", "o/r", "main", {"a": b"1"}, "m", api=self.api)
        self.assertNotIn("ruim", str(c.exception))


if __name__ == "__main__":
    unittest.main()
