# Contrato dos artefatos web

A aplicação lê só arquivos estáticos em `app/<DATA_ROOT>/`, definido em `assets/js/config.js`:

| Pasta | Conteúdo |
|---|---|
| `data/demo/` | Pacote sintético do modo demonstração, gerado por `scripts/gerar_dados_demo.py`. |
| `data/gold/` | Exports reais da Gold (ainda não existe). |

As duas pastas nunca se misturam. Para publicar dados reais, gere os mesmos arquivos em `data/gold/` e troque `DATA_ROOT`. A interface mostra o banner de demonstração sempre que `manifest.modo === "demo"`.

## `manifest.json`

```json
{
  "produto": "ChuvaSegura-SP",
  "modo": "demo | gold",
  "versao": "1.0.0",
  "referencia": "2026-11-17T09:00:00-03:00",
  "publicado_em": "2026-11-17T09:40:00-03:00",
  "datasets": [{ "arquivo": "ocorrencias.json", "id": "ocorrencias", "titulo": "...", "fonte": "geosampa",
                 "natureza": "historico", "tabelas_origem": ["gold.fact_geosampa_flooding_events"],
                 "periodo": {"inicio": "2020-01-01", "fim": "2026-11-08"}, "processado_em": "...",
                 "publicado_em": "...", "registros": 5179, "bytes": 331776, "qualidade": "aprovado" }],
  "linhagem": [{ "fonte": "GeoSampa", "tabelas": ["bronze...", "silver...", "gold..."],
                 "publicado_na_web": true, "processado_em": "...", "qualidade": "aprovado | reprovado" }],
  "nao_publicados": [{ "id": "vias", "titulo": "Rede viária", "motivo": "..." }]
}
```

`referencia` é a última hora coberta pelos dados de monitoramento; os filtros de período usam o dia dela como "hoje".

## Metadados de cada arquivo (`meta`), seção 10.3 do PDR

Cada arquivo é `{ "meta": {...}, "data": ... }`. Campos de `meta`:

| Campo | Obrigatório | Descrição |
|---|:-:|---|
| `id`, `titulo`, `versao` | sim | Identificação. |
| `natureza` | sim | `observado`, `historico`, `oficial`, `derivado`, `estimado` ou `sintetico`. |
| `natureza_representada` | só em `sintetico` | O que o dado sintético simula. |
| `fonte` | sim | `{ id, nome, url }`. |
| `tabelas_origem`, `camada_origem` | sim | Tabelas do Unity Catalog `fiap_analytics` usadas. |
| `processado_em`, `publicado_em` | sim | ISO 8601 com fuso. |
| `periodo` ou `data_referencia` | sim | `{inicio, fim}` para séries; `data_referencia` para bases estáticas. |
| `referencia` | séries recentes | Última hora coberta (define o estado "desatualizado"). |
| `unidade`, `granularidade` | quando houver | `granularidade = { espacial, temporal }`. |
| `licenca` | sim | Termos da fonte. |
| `qualidade` | sim | `{ estado, descricao }`. |
| `registros` | sim | Contagem de registros publicados. |

## Conjuntos e mapeamento para a Gold

| Arquivo | `data` | Origem na Gold |
|---|---|---|
| `subprefeituras.json` | `[{ sigla, nome }]` | `dim_geosampa_subprefecture_area` (`SIG_SUBPREFEITURA_GEOSAMPA`, `NOM_SUBPREFEITURA`) |
| `ocorrencias.json` | colunar: `{ ano_origem[], id[], data[], tipo[], subprefeitura[], lat[], lon[], grau_susc_inundacao[] }` | `fact_geosampa_flooding_events` + `dim_tempo` + dimensões de tipo e subprefeitura; `grau_susc_inundacao` = maior `NUM_GRAU_SUSCETIBILIDADE` das áreas `inundacao` que contêm o ponto |
| `estacoes.json` | `{ chuva: [{ id, codigo, rede, nome, grandeza, instituicao, lat, lon, subprefeitura, ativa, tabela }], nivel: [... + curso_dagua, unidade] }` | `dim_cemaden_stations`, `dim_sibh_sensors`, `dim_inmet_stations`. `id` = `<rede>:<código>` |
| `chuva_diaria.json` | `{ inicio, series: { id: [mm \| null, ...] } }` | Soma diária de `NUM_CHUVA_MM` das três fatos horárias de chuva; `null` = dia sem leitura |
| `chuva_horaria.json` | `{ inicio, series: { id: [mm \| null] } }` (168 horas) | Fatos horárias de chuva; a hora é o início do intervalo, fuso de São Paulo |
| `nivel.json` | `{ horario_inicio, horario: { id: [m] }, diario_inicio, diario: { id: [m] } }` | `fact_sibh_level_hourly` (`NUM_NIVEL_MEDIO`), 7 dias horários e 365 dias de média diária |
| `suscetibilidade.json` | `{ classes: [{ grau, classe, camada, caracteristicas }], areas: [{ id, camada, grau, classe, area_km2, poligono: [[lon, lat], ...] }] }`; `meta.area_uniao_km2` e `meta.area_municipio_km2` | `dim_sgb_susceptibility_areas` (camada `inundacao`, geometria simplificada) + `dim_sgb_susceptibility_class` |
| `avisos_oficiais.json` | `[]` + `meta.estado_integracao` | Sem fonte integrada |

Para manter os arquivos leves, simplifique as geometrias para a escala de visualização e arredonde as coordenadas em 5 casas (cerca de 1 m).
