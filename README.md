# ChuvaSegura-SP: FIAP GS2 2TSCPV 2026

Inteligência urbana para prevenção de alagamentos e apoio à mobilidade no município de São Paulo (IBGE 3550308). O projeto junta **chuva**, **nível de rios e córregos**, **ocorrências históricas**, **suscetibilidade do território**, **população** e **vias** em uma experiência centrada no mapa.

> Entender a chuva. Antecipar riscos. Proteger a cidade.

O repositório tem duas partes independentes:

| Pasta | O que tem | Detalhes |
|---|---|---|
| [`data/`](data) | Pipeline de dados no Databricks (Unity Catalog `fiap_analytics`, camadas Bronze, Silver e Gold): notebooks em `data/src/` e definições dos jobs em `data/jobs/`. Fontes: GeoSampa, CEMADEN, SIBH/SP Águas, SGB/CPRM, IBGE, OpenStreetMap e INMET. | [`data/README.md`](data/README.md) |
| [`app/`](app) | Aplicação web do MVP (Fase 1): portal, visão geral, mapa, monitoramento, histórico, análises, sobre os dados, base de conhecimento, assistente e perfis. Site estático, publicado na Vercel com `Root Directory = app`. | [`app/README.md`](app/README.md) |
| [`docs/`](docs) | Documento de produto: [PDR do ChuvaSegura-SP](docs/PDR_CHUVASEGURA_SP.md). | |

A aplicação não lê o Databricks diretamente: ela consome arquivos exportados da Gold, no contrato descrito em [`app/DATA_CONTRACT.md`](app/DATA_CONTRACT.md). Enquanto esses exports não existem, a aplicação roda em **modo demonstração**, com dados sintéticos sempre identificados como tais.

## Rodar a aplicação localmente

```bash
python -m http.server 8000 --directory app
```

Depois, abra <http://localhost:8000>.

## Limites

O ChuvaSegura-SP não é sistema oficial de alerta e não substitui Defesa Civil, CEMADEN, INMET, SAISP ou órgãos municipais. Não prevê alagamento por rua nem oferece rotas. Em emergência, ligue 199 (Defesa Civil).
