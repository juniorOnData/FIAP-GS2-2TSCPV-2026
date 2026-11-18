"""Testes do casamento de nomes entre fontes. Rodar a partir da raiz: python -m unittest discover -s data/tests -v"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from common.nomes import campo_que_melhor_casa, normalizar_nome  # noqa: E402


class NomesTest(unittest.TestCase):
    def test_normaliza_acento_caixa_e_pontuacao(self):
        self.assertEqual(normalizar_nome("M'Boi Mirim"), normalizar_nome("M BOI MIRIM"))
        self.assertEqual(normalizar_nome("Freguesia/Brasilândia"), "FREGUESIABRASILANDIA")
        self.assertEqual(normalizar_nome(None), "")

    def test_escolhe_o_campo_que_casa_com_a_dimensao(self):
        dim = {normalizar_nome(n) for n in ("Sé", "Butantã", "Freguesia-Brasilândia")}
        registros = [{"sp_codigo": "1", "sp_nome": "SE"}, {"sp_codigo": "2", "sp_nome": "BUTANTA"},
                     {"sp_codigo": "3", "sp_nome": "FREGUESIA/BRASILANDIA"}]
        self.assertEqual(campo_que_melhor_casa(registros, dim), ("sp_nome", 3))

    def test_nenhum_campo_casa(self):
        self.assertEqual(campo_que_melhor_casa([{"a": "x"}], {"Y"}), (None, 0))


if __name__ == "__main__":
    unittest.main()
