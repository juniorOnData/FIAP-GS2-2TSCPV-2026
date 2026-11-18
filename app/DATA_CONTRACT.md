# Contrato dos artefatos web

A aplicação lê só arquivos estáticos em `app/<DATA_ROOT>/`, definido em `assets/js/config.js`:

| Pasta | Conteúdo |
|---|---|
| `data/demo/` | Pacote sintético do modo demonstração, gerado por `scripts/gerar_dados_demo.py`. |
| `data/gold/` | Exports reais da Gold. A primeira publicação pode conter só GeoSampa; o manifesto lista os conjuntos disponíveis. |

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
| `subprefeituras.json` | `[{ sigla, nome }]` | `dim_geosampa_subprefecture_area` (`SIG_SUBPREFEITURA`, `NOM_SUBPREFEITURA`); `sigla` é o código canônico usado pela fato |
| `ocorrencias.json` | colunar: `{ ano_origem[], id[], data[], tipo[], subprefeitura[], lat[], lon[], grau_susc_inundacao[] }` | `fact_geosampa_flooding_events` + `dim_tempo` + dimensões de tipo e subprefeitura; `grau_susc_inundacao` é o maior grau (1 a 3) das áreas de inundação do SGB que contêm o ponto, `0` fora delas e nulo quando o cruzamento não foi feito |
| `estacoes.json` | `{ chuva: [{ id, codigo, rede, nome, grandeza, instituicao, lat, lon, subprefeitura, ativa, tabela }], nivel: [... + curso_dagua, unidade] }` | `dim_cemaden_stations`, `dim_sibh_sensors`, `dim_inmet_stations`. `id` = `<rede>:<código>` |
| `chuva_diaria.json` | `{ inicio, series: { id: [mm \| null, ...] } }` | Soma diária de `NUM_CHUVA_MM` das três fatos horárias de chuva; `null` = dia sem leitura |
| `chuva_horaria.json` | `{ inicio, series: { id: [mm \| null] } }` (168 horas) | Fatos horárias de chuva; a hora é o início do intervalo, fuso de São Paulo |
| `nivel.json` | `{ horario_inicio, horario: { id: [m] }, diario_inicio, diario: { id: [m] } }` | `fact_sibh_level_hourly` (`NUM_NIVEL_MEDIO`), 7 dias horários e 365 dias de média diária. Sensor sem leitura no período fica fora de `horario`/`diario` |
| `suscetibilidade.json` | `{ classes: [{ grau, classe, camada, caracteristicas, declividade }], areas: [{ id, camada, grau, classe, area_km2, geometria }] }`; `geometria` = coordenadas de um MultiPolygon GeoJSON (anéis fechados, buracos depois do anel externo); `meta.area_uniao_km2` por grau (cada ponto contado uma vez, no maior grau) e `meta.area_municipio_km2` | `dim_sgb_susceptibility_areas` (camada `inundacao`, geometria simplificada em 5 m) + `dim_sgb_susceptibility_class`; a área do município é a união da camada `movimento_massa`, que cobre a carta inteira |
| `setores.json` | colunar: `{ id[], distrito[], situacao[], favela[], pessoas[], domicilios[], area_km2[], densidade[], geometria[] }`; `geometria` como em `suscetibilidade.json` | `dim_ibge_census_tracts` + `fact_ibge_census_tract_population` (Censo 2022), geometria simplificada em 8 m. Arquivo grande: o app só baixa quando a camada é ligada |
| `vias.json` | colunar: `{ id[], nome[], ref[], classe[], hierarquia[], ponte[], tunel[], km[], geometria[] }`; `geometria` = coordenadas de um MultiLineString GeoJSON | `dim_osm_roads` + `dim_osm_road_class`, só hierarquia 1 a 5 (expressas a coletoras e acessos), geometria simplificada em 8 m; atribuição ODbL obrigatória |
| `avisos_oficiais.json` | `[]` + `meta.estado_integracao` | Sem fonte integrada |

Para manter os arquivos leves, simplifique as geometrias para a escala de visualização e arredonde as coordenadas em 5 casas (cerca de 1 m).

## Exportação automática

A tarefa `export_web` do job `ChuvaSegura` (`data/src/shared/export/01_export_web.py`) roda depois de todas as fontes, confere se a qualidade Gold de cada uma passou **na mesma execução** e publica o pacote em `/Volumes/fiap_analytics/gold/chuvasegura_web/` e, por commit, em `app/data/gold/`. O job roda a cada 2 horas. Se uma fonte falhar, as outras são publicadas e os conjuntos dela continuam com a versão anterior: a entrada no manifesto é copiada da publicação anterior com `mantido: true`, o arquivo não é reenviado, e `falhas` lista o motivo.

Particularidades dos dados reais:

- **Estações:** `id` = `<rede>:<código>` (`cemaden:`, `sibh:`, `inmet:`). `subprefeitura` é `null`, porque a Gold não tem essa informação das estações; o filtro por subprefeitura não as inclui. Só entram estações com coordenadas no envelope do município e que estejam ativas ou tenham leitura.
- **Chuva diária** (de 2020-01-01 até o dia de `meta.referencia`): soma das horas no fuso de São Paulo; dia sem leitura é `null`, nunca zero. Estação com mais de um sensor (CEMADEN): vale o maior valor de cada hora.
- **Chuva horária:** últimas 168 horas; `meta.referencia` é a última hora com leitura em qualquer rede.
- **Nível:** média por hora (7 dias) e média diária (365 dias), sempre em metros (`meta.unidade = "m"`). É a cota informada pela fonte (altitude da superfície da água na referência do sensor), não a profundidade. `meta.unidade_origem` diz em que unidade a fonte publica (`cm` no SIBH); sem esse campo, o app suspende as regras de alerta de nível.
- **Manifesto:** cada conjunto traz `sha256_dados`, usado para só gerar commit quando os dados mudam, e uma cópia do `meta` (as telas de metadados não precisam baixar arquivos grandes). `nao_publicados` lista o motivo de cada conjunto ausente, inclusive falhas da execução.
- **Ocorrências:** descartadas as sem data ou fora do envelope (`registros_descartados`). `grau_susc_inundacao`: maior grau das áreas de inundação do SGB que contêm o ponto, `0` fora delas, `null` quando o SGB não foi exportado na mesma publicação.
- **Geometrias:** simplificadas por Douglas-Peucker (tolerância em `meta.simplificacao_m`) e arredondadas em 5 casas. O cruzamento das ocorrências e as áreas por grau usam a geometria completa da Gold.
