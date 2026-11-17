# ChuvaSegura-SP: aplicação web (MVP, Fase 1)

> Esta pasta reúne a aplicação. O pipeline de dados (Databricks) fica em [`../data`](../data).

Frontend da plataforma descrita em [`docs/PDR_CHUVASEGURA_SP.md`](../docs/PDR_CHUVASEGURA_SP.md). É um site estático, sem etapa de build: HTML, CSS e módulos JavaScript nativos. O mapa usa [MapLibre GL JS 4.7.1](https://maplibre.org/) via CDN, sobre o mapa base Positron da CARTO.

> **Modo demonstração.** Esta versão usa dados **sintéticos** (`data/demo/`), gerados por [`scripts/gerar_dados_demo.py`](scripts/gerar_dados_demo.py). Um banner aparece em todas as telas. Os arquivos seguem o mesmo contrato dos exports da Gold ([`DATA_CONTRACT.md`](DATA_CONTRACT.md)).

## Rodar localmente

Comandos a partir da raiz do repositório. Os módulos e o `fetch` dos dados precisam de HTTP (abrir o `index.html` direto do disco não funciona):

```bash
python app/scripts/gerar_dados_demo.py    # opcional: regera o pacote com a hora atual como referência
python -m http.server 8000 --directory app
```

Depois, abra <http://localhost:8000>.

O pacote de demonstração tem uma data de referência fixa. Depois de algumas horas, as leituras de chuva e nível aparecem como **desatualizadas**: é o comportamento esperado (F10). Regere o pacote para atualizar a referência.

## Dados reais

O app usa a primeira pasta que tiver um `manifest.json`: `data/gold/` (dados reais) e, se ela ainda não existir, `data/demo/` (sintéticos, com banner permanente). Não é preciso trocar nenhuma configuração.

`data/gold/` é escrita pelo job `ChuvaSegura` no Databricks: a tarefa `export_web` (`data/src/shared/export/01_export_web.py`) faz o commit direto neste repositório, e a Vercel publica em seguida. **Não edite nem copie arquivos à mão para essa pasta.** Detalhes, segredo do token e travas de qualidade em `data/README.md`.

A aplicação desativa camadas não publicadas (o manifesto lista o motivo) e não apresenta o cruzamento com suscetibilidade como se estivesse disponível. Não misture arquivos de `data/demo/` com `data/gold/`.

## Publicar na Vercel

1. Importe o repositório na Vercel e defina **Root Directory = `app`**.
2. Framework preset: **Other**. Sem comando de build e sem diretório de saída (o próprio `app/`). O `.vercelignore` deixa `scripts/` e a documentação fora da publicação.
3. As rotas usam hash (`#/mapa`), então não há regras de reescrita.

## Estrutura

```
app/
├── index.html
├── vercel.json · .vercelignore
├── scripts/gerar_dados_demo.py # gera o pacote sintético (não é publicado)
├── DATA_CONTRACT.md            # formato dos arquivos publicados (demo e Gold)
├── data/demo/                  # pacote sintético (nunca misturar com data/gold/)
└── assets/
    ├── css/  tokens.css (Design System v1.0) · base.css · components.css · pages.css
    └── js/
        ├── main.js             # shell, navegação e rotas
        ├── config.js           # DATA_ROOT, fuso, limites de atualização, mapa base
        ├── store.js            # filtros compartilhados e perfil de uso
        ├── data.js             # carregamento, estados (ok/vazio/erro/não publicado), atualização
        ├── ui.js · charts.js · icons.js · util.js
        ├── insights.js         # insights descritivos (PDR 8.3)
        ├── content.js          # fontes, metodologia e base de conhecimento
        ├── regras-alerta.js    # regras de alerta da plataforma sobre chuva e nível observados
        └── pages/              # portal, mapa, indicadores, monitoramento, alertas, historico,
                                # analises (Previsões e Análises), dados, conhecimento, assistente, perfil
```

## Cobertura do PDR nesta versão

| Requisito | Situação |
|---|---|
| F01–F05 mapa, camadas, agrupamentos, filtros, detalhe | Feito: ocorrências agrupadas, chuva (1 h/24 h/72 h/período), nível, suscetibilidade; limites, setores e vias aparecem desabilitados com o motivo |
| F06 indicadores · F13 central de painéis | Feito. O mapa é a entrada da plataforma |
| Alertas da plataforma | Regras fixas sobre chuva e nível observados (não oficiais), com reconhecimento e tratamento salvos só no navegador. Envio por SMS ainda não configurado: exige serviço no servidor e login de operador |
| F07 chuva por estação e janela | Feito, com estado de atualização |
| F08 séries históricas | Feito (dia ou mês, conforme o período) |
| F09 sobre os dados · F11 qualidade e linhagem | Feito (qualidade simulada no pacote de demonstração) |
| F10 carregando, vazio, erro, desatualizado | Feito, em cada painel |
| F12 suscetibilidade | Feito, rotulada como não previsão |
| F14 base de conhecimento | Feito |
| F15 assistente | Modo demonstrativo sem LLM (busca na base e cálculos com fontes); integração com modelo é a decisão em aberto nº 3 |
| F16 perfis | Feito, sem login (decisão em aberto nº 1) |
| F17 demonstração | Feito |
| F18 avisos oficiais | Estado "não integrado" |
| F19 exportar | CSV das ocorrências filtradas |
| F20–F25 | Fora desta versão (gate de Data Science não aprovado; IBGE e OSM não publicados) |

No pacote Gold parcial, o manifesto determina quais fontes foram publicadas. CEMADEN, SIBH, INMET e SGB só aparecem como publicados quando seus próprios artefatos estiverem no pacote.

## Acessibilidade

- **Teclado:** link para pular ao conteúdo, foco visível, navegação por teclado (as abas usam as setas) e regiões `aria-live` para filtros e chat.
- **Gráficos:** cada um tem uma alternativa em tabela.
- **Sem depender só de cor:** os estados combinam rótulo, ícone e forma (círculo para chuva, losango para nível, contornos distintos para as classes de suscetibilidade).
- **Mapa:** os registros do recorte também aparecem em uma lista navegável no painel lateral.
