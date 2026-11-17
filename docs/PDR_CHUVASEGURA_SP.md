# PDR — ChuvaSegura-SP

**Versão:** 1.0  
**Status:** Primeira versão consolidada para orientar a construção do produto  
**Produto:** ChuvaSegura-SP — inteligência urbana para prevenção de alagamentos e apoio à mobilidade  
**Escopo geográfico inicial:** Município de São Paulo (IBGE 3550308)  
**Identidade visual:** Urban Intelligence

## 1. Resumo do produto

O ChuvaSegura-SP é uma plataforma de inteligência urbana que organiza dados de chuva, nível de rios e córregos, ocorrências históricas, suscetibilidade territorial, população e vias em uma experiência visual centrada no mapa de São Paulo.

A plataforma deve ajudar gestores públicos, equipes de Defesa Civil, analistas e moradores a explorar o que aconteceu, entender as condições observadas e localizar áreas que merecem atenção. O produto conecta dados que hoje estão distribuídos em diferentes fontes e apresenta sua origem, período, qualidade e limitações.

O mapa é a principal superfície de exploração. Dashboards, análises, histórico, base de conhecimento e assistente contextual complementam o mapa. Estimativas de risco e recursos de Data Science entram apenas quando houver dados adequados, validação suficiente e explicações compreensíveis.

**Posicionamento:** Mais do que saber quando vai chover, queremos entender onde a chuva pode se transformar em risco para a população.

**Mensagem institucional:** Entender a chuva. Antecipar riscos. Proteger a cidade.

## 2. Problema e oportunidade

Informações de precipitação, hidrologia, alagamentos, relevo, população e infraestrutura são publicadas por instituições diferentes e raramente aparecem juntas em uma visão territorial simples. Isso dificulta a identificação de padrões, a comparação entre áreas e a priorização de ações preventivas.

O ChuvaSegura-SP cria uma camada analítica que conecta essas fontes e apresenta evidências em contexto geográfico. A plataforma não substitui previsão do tempo nem órgãos oficiais; seu papel é organizar e explicar dados locais para apoiar exploração, planejamento e preparação.

## 3. Objetivos

- Reunir dados ambientais, históricos e territoriais em uma experiência integrada.
- Permitir explorar o município por mapa, período, tipo de dado e recorte territorial.
- Mostrar recorrência de ocorrências e sua relação espacial com chuva, hidrologia e características do território.
- Apresentar insights compreensíveis, com critérios e fontes rastreáveis.
- Tornar qualidade, atualização e limitações dos dados visíveis ao usuário.
- Criar uma base de produto que possa receber novas fontes e resultados de Data Science sem confundir observações com estimativas.

## 4. Não objetivos e limites do piloto

- Não é um sistema oficial de alerta nem substitui Defesa Civil, CEMADEN, INMET, SAISP ou órgãos municipais.
- Não prevê alagamentos por rua nem garante que uma via esteja transitável.
- Não oferece roteamento ou navegação em tempo real.
- Não cobre outros municípios no piloto.
- Não deve chamar um índice de suscetibilidade ou uma análise histórica de previsão.
- Não deve inferir risco apenas pela intensidade da chuva, sem considerar dados e critérios pertinentes.
- Não deve exibir estimativas de modelo sem validação, escopo e limitações documentados.
- Não deve sugerir exposição individual; dados populacionais são agregados por setor censitário.

## 5. Usuários e necessidades

| Perfil | Necessidade principal | Uso esperado |
|---|---|---|
| Gestor público / Defesa Civil | Localizar pontos críticos e priorizar prevenção | Consultar mapa, recorrência, chuva, território e exposição potencial |
| Analista / equipe técnica | Investigar padrões, qualidade e metodologia | Filtrar dados, comparar séries, consultar fontes e linhagem |
| Morador | Entender o contexto histórico e territorial do entorno | Explorar o mapa e consultar explicações em linguagem clara |
| Motorista | Identificar regiões com ocorrências ou condições relevantes | Consultar camadas e avisos; não receber promessa de rota segura |
| Administrador / curador | Manter conteúdo, perfis e configurações do produto | Gerenciar base de conhecimento, parâmetros e acesso, conforme implementação |

Os perfis definem a linguagem, os atalhos e, quando houver autenticação, as permissões. A existência de uma tela de perfis não deve ser apresentada como controle de acesso real até que autenticação e autorização sejam implementadas.

## 6. Princípios do produto

1. **Mapa primeiro:** a exploração geográfica conecta as áreas funcionais do produto.
2. **Evidência antes da conclusão:** mostrar dados e critérios que sustentam cada insight.
3. **Proveniência visível:** indicar fonte, período de referência, atualização e estado do dado.
4. **Observado, histórico, oficial e estimado são diferentes:** usar rótulos e convenções visuais distintos.
5. **Explicabilidade:** exibir fatores e limitações junto de scores ou classificações.
6. **Acessibilidade:** cor nunca deve ser o único meio de transmitir uma condição.
7. **Evolução modular:** adicionar fontes e modelos sem redesenhar a experiência central.

## 7. Escopo funcional

### 7.1. Núcleo do MVP

| ID | Requisito | Prioridade |
|---|---|---|
| F01 | Exibir mapa detalhado do município de São Paulo como tela central, com zoom, navegação, legenda e identificação do recorte territorial. | P0 |
| F02 | Permitir ligar e desligar camadas de ocorrências históricas, estações, chuva observada, nível hidrológico, suscetibilidade territorial e limites administrativos, conforme disponibilidade dos dados publicados. | P0 |
| F03 | Exibir ocorrências como marcadores agrupados/bolhas em escalas amplas e pontos individuais em maior zoom, sem ocultar a densidade ou a origem dos registros. | P0 |
| F04 | Oferecer filtros por período, tipo de ocorrência, subprefeitura/distrito e fonte, aplicados de forma consistente ao mapa e aos indicadores vinculados. | P0 |
| F05 | Ao selecionar feição, estação, área ou ocorrência, abrir detalhe com atributos disponíveis, fonte, data/período e link para metodologia quando aplicável. | P0 |
| F06 | Disponibilizar Overview com indicadores gerais da cidade e atalhos para mapa, monitoramento, histórico e análises. | P0 |
| F07 | Exibir chuva acumulada por estação e janela de tempo, com unidade em milímetros, timestamp e estado de atualização. | P0 |
| F08 | Exibir séries históricas de ocorrências, chuva e nível hidrológico, respeitando a granularidade e cobertura de cada fonte. | P0 |
| F09 | Exibir Sobre os dados com fontes, metodologia, frequência, cobertura, licenças e limitações conhecidas. | P0 |
| F10 | Exibir estado de carregamento, ausência de dados, indisponibilidade e dado desatualizado sem os confundir. | P0 |
| F11 | Exibir qualidade e linhagem das tabelas/publicações utilizadas nos painéis, incluindo camada de origem e data de processamento quando disponíveis. | P1 |
| F12 | Apresentar análise de suscetibilidade territorial com classe, metodologia e referência da fonte, sem rotular como previsão. | P0 |

### 7.2. Funcionalidades de continuidade do Data Galaxy

As funcionalidades abaixo são desejadas para preservar padrões úteis do projeto anterior, adaptadas ao domínio hidrometeorológico. A prioridade de entrega pode ser ajustada por prazo e viabilidade técnica, mas elas fazem parte da visão do produto.

| ID | Requisito | Prioridade |
|---|---|---|
| F13 | Central de dashboards com navegação entre visão geral, mapa de monitoramento, monitoramento de estações, histórico, análises e sobre os dados. | P0 |
| F14 | Base de conhecimento com conceitos, fontes, metodologia, perguntas frequentes, glossário e limitações do ChuvaSegura-SP. | P1 |
| F15 | Assistente de IA para responder perguntas sobre a plataforma, dados, metodologia e insights publicados, referenciando a base de conhecimento e os dados consultados. | P1 |
| F16 | Perfis de uso com experiência e linguagem adequadas a gestão/Defesa Civil, análise técnica e consulta pública. | P1 |
| F17 | Modo de demonstração com dados sintéticos identificados de forma persistente como demonstração e mantidos separados dos dados oficiais/importados. | P1 |
| F18 | Mostrar alertas ou avisos publicados por fontes oficiais com origem e validade, mantendo-os distintos de indicadores analíticos e de demonstrações. | P1 |
| F19 | Exportar ou compartilhar recortes e visualizações quando a fonte/licença e a implementação permitirem. | P2 |
| F20 | Configurações administrativas de parâmetros, conteúdo e regras de classificação, com histórico de alterações se houver persistência e autenticação. | P2 |

### 7.3. Recursos condicionados à validação ou integração

| ID | Requisito | Condição para ativação |
|---|---|---|
| F21 | Exibir classe ou score de risco por área. | Definição de alvo, cobertura espaço-temporal suficiente, baseline, validação e explicação dos fatores. |
| F22 | Exibir fatores explicativos do risco, como chuva antecedente, recorrência, relevo e proximidade de drenagem. | Fatores disponíveis, cálculo documentado e relação apresentada sem alegação causal indevida. |
| F23 | Exibir impacto potencial sobre população e vias. | Integrações IBGE/OSM processadas e método de sobreposição/estimativa documentado. |
| F24 | Consultar ocorrências recentes ou atualizações próximas de tempo real. | Frequência de atualização, fonte e mecanismo de publicação confirmados. |
| F25 | Gerar recomendações ou sugestões de atenção. | Regras transparentes, contexto suficiente, linguagem não alarmista e revisão das equipes do projeto. |

## 8. Mapa e experiência de exploração

### 8.1. Composição da tela

- **Navegação global:** acesso ao Overview, mapa, monitoramento, histórico, análises, base de conhecimento, assistente, perfil e configurações disponíveis.
- **Mapa principal:** ocupa a maior parte da área útil e permite navegar pelo município, com base cartográfica clara.
- **Painel de camadas:** alterna ocorrências, precipitação, sensores, nível hidrológico, suscetibilidade, setores censitários e vias, conforme cobertura e desempenho.
- **Filtros:** período, fonte, categoria, subprefeitura/distrito e outras dimensões disponíveis.
- **Legenda e contexto:** informa unidade, escala, significado das cores/símbolos, período, fonte e data de atualização.
- **Painel de detalhe:** apresenta atributos do elemento selecionado e permite abrir série temporal ou metodologia associada.
- **Painel de insights:** resume padrões encontrados nos dados filtrados, distinguindo fatos observados de estimativas.

### 8.2. Representação geográfica

- Usar pontos e agrupamento de pontos para registros discretos; polígonos para áreas de suscetibilidade e setores; superfície de calor apenas quando o método de agregação estiver definido e descrito.
- A intensidade visual deve refletir valor e unidade documentados. Uma área colorida não pode ser apresentada como risco de alagamento se representar apenas chuva, densidade de ocorrências ou suscetibilidade.
- Diferenciar os tipos de camada por símbolo, rótulo e legenda, além da cor.
- Exibir contagem e contexto ao ampliar um agrupamento; ao selecionar uma área, oferecer detalhamento do recorte e das fontes relacionadas.
- Respeitar o sistema de coordenadas de cada fonte na transformação e publicar os dados em formato adequado à renderização web.

### 8.3. Sugestões e insights

O produto pode destacar, por exemplo, áreas com maior recorrência histórica, estações com chuva acumulada elevada no período escolhido ou sobreposição entre ocorrências e suscetibilidade. Cada insight deve apresentar:

- descrição objetiva do padrão;
- área e período considerados;
- indicador, método ou regra usada;
- fontes e atualização;
- limitações e nível de evidência;
- ação de exploração sugerida, sem prescrever conduta de emergência.

## 9. Assistente de IA e base de conhecimento

O assistente é uma interface de consulta, não uma fonte independente de verdade. Deve priorizar conteúdo curado do projeto e resultados analíticos disponíveis, indicar as fontes usadas e responder quando não houver base suficiente.

Requisitos:

- Responder sobre funcionamento, conceitos, fontes, metodologia e dados publicados.
- Sempre que possível, citar nome da fonte, período de referência e filtros aplicados.
- Diferenciar informação oficial, registro histórico, resultado analítico e dado sintético.
- Não afirmar que uma rua está segura, emitir alerta oficial ou prever evento sem recurso validado e autorizado.
- Não inventar observações ausentes, limiares, datas de atualização ou conclusões causais.
- Permitir perguntas sugeridas relacionadas à tela atual, sem enviar dados pessoais desnecessários.
- Se a integração com modelo de linguagem não estiver disponível, apresentar estado de indisponibilidade ou modo demonstrativo claramente identificado.

A base de conhecimento deve incluir glossário, descrição de camadas, fontes e licenças, metodologia, atualização, cobertura, limites e perguntas frequentes. Conteúdo demonstrativo deve ser rotulado.

## 10. Dados, arquitetura e integrações

### 10.1. Fontes previstas

| Prioridade | Fonte | Uso no produto |
|---|---|---|
| P0 | GeoSampa | Ocorrências históricas e divisões administrativas |
| P0 | CEMADEN | Estações e leituras observadas de chuva |
| P1 | SIBH / SP Águas | Leituras de chuva e nível hidrológico e estações |
| P1 | SGB/CPRM | Classes e áreas de suscetibilidade territorial |
| P2 | IBGE — Censo 2022 | População e domicílios agregados por setor censitário |
| P2 | OpenStreetMap / Geofabrik | Rede viária, com atribuição conforme ODbL |
| P3 condicional | INMET | Complemento se houver cobertura ou variáveis que agreguem valor às fontes existentes |

A disponibilidade efetiva de cada fonte e tabela deve ser confirmada na publicação de cada versão; a priorização não garante completude ou atualização contínua.

### 10.2. Pipeline e publicação

- O processamento analítico ocorre no Databricks, no Unity Catalog `fiap_analytics`, seguindo Bronze, Silver e Gold.
- A camada web é independente dos notebooks e jobs de dados.
- A aplicação publicada na Vercel consome arquivos ou artefatos analíticos exportados do Databricks, conforme a arquitetura definida para o MVP.
- Os artefatos web devem ser otimizados para mapa, filtros e gráficos; cada publicação deve incluir versão, timestamp, origem e metadados de atualização.
- A interface deve tratar ausência de atualização, falha de exportação e falha de carregamento sem indicar informação atualizada incorretamente.
- A forma final dos arquivos publicados (por exemplo, GeoJSON, JSON ou outro formato) deve ser escolhida considerando volume, geometria, desempenho e limites da hospedagem.

### 10.3. Contrato mínimo de metadados

Cada conjunto de dados publicado deve trazer, sempre que aplicável: identificador da fonte; nome/camada; período observado ou data de referência; data de ingestão/processamento/publicação; unidade; granularidade espacial e temporal; licença; estado de qualidade; versão; e indicação de dado observado, histórico, oficial, derivado, estimado ou sintético.

## 11. Gate de Data Science

A previsão temporal supervisionada só entra no produto se a base de ocorrências tiver data/hora confiável e existir sobreposição espaço-temporal suficiente com medições de chuva. Antes da liberação, a equipe deve documentar:

1. definição do evento-alvo e unidade geográfica;
2. cobertura, vieses, dados faltantes e resolução temporal/espacial;
3. baseline comparável e estratégia de validação temporal;
4. métricas adequadas ao custo de falsos positivos e falsos negativos;
5. desempenho por área/período, limites e fatores explicativos;
6. linguagem e visualização coerentes com a evidência obtida.

Se o gate não for atendido, o produto entrega análise histórica e índice de suscetibilidade territorial claramente rotulados. Essas saídas não serão descritas como previsão nem alerta.

## 12. Identidade visual e design system

Aplicar o documento **CHUVASEGURA-SP — Design System & Visual Guidelines v1.0** como referência visual oficial:

- Conceito: **Urban Intelligence**; estilo editorial tecnológico; tema claro.
- Azul Royal `#155EEF` como cor institucional. Azul institucional não representa nível de risco.
- Cores semânticas: normal `#16A34A`, atenção `#EAB308`, alerta `#F97316`, crítico `#DC2626`; aplicá-las somente segundo critérios documentados.
- Neutros: fundo `#F5F8FC`, superfície `#FFFFFF`, texto primário `#0F172A`, bordas `#E2E8F0`.
- Manrope para títulos expressivos; Inter para corpo, tabelas, controles e dados.
- Grid de 12 colunas, espaçamento-base de 8 px, sidebar desktop de 248 px, header de 72 px e largura de conteúdo até 1440 px, adaptando os valores para telas menores.
- Cards claros, bordas discretas e sombras suaves. O dashboard pode ser mais denso que o portal institucional, mantendo os mesmos tokens.
- Gráficos devem identificar período, unidade e legenda. Chuva usa escala azul própria; alertas usam cores semânticas.
- Mapas usam base cartográfica clara, estações, camadas territoriais e ocorrências com legendas explícitas.

### 12.1. Biblioteca de componentes

- **Botões:** ação principal com fundo Royal Blue e texto branco; ação secundária com fundo Ice Blue e texto Royal Blue; ação textual sem fundo, em Royal Blue. Altura recomendada de 40–44 px. Reservar a cor azul para ações relevantes e manter estados de foco visíveis.
- **Cards de indicador:** mostrar rótulo, valor, unidade e ícone opcional. Incluir período e estado/origem do dado sempre que aplicável. Em modo demonstrativo, exibir uma indicação inequívoca como “Valor demonstrativo, sem conexão com dados reais”.
- **Badges de status:** Normal, Atenção, Alerta e Crítico. Usar cor semântica junto com texto e, quando necessário, ícone; a cor isolada não comunica o estado.
- **Campos de formulário:** label explícita, superfície branca, borda neutra e foco azul visível. O controle de período deve oferecer intervalos disponíveis e refletir o filtro ativo nas visualizações relacionadas.
- **Componentes reutilizáveis:** padronizar botões, cards, badges, campos, seletores, tooltips, legendas, estados vazios e painéis de detalhes para manter consistência entre portal e dashboard.

### 12.2. Portal institucional e mensagem de marca

A página institucional deve abrir com uma composição editorial que apresente a marca ChuvaSegura, o conceito **Urban Intelligence**, uma chamada curta e ações claras para explorar o monitoramento ou conhecer o projeto. A mensagem de referência é:

> Entender a chuva. Antecipar riscos. Proteger a cidade.

O texto de apoio apresenta a plataforma como inteligência de dados para monitoramento hidrometeorológico e prevenção de alagamentos em São Paulo. A chamada principal conduz ao monitoramento; a secundária explica o projeto. A composição deve seguir o tema claro definido pelo Design System.

## 13. Acessibilidade, usabilidade e estados da interface

- Adotar WCAG 2.2 nível AA como referência.
- Garantir contraste, foco visível, navegação por teclado e nomes acessíveis para controles e camadas.
- Não comunicar severidade apenas por cor; combinar rótulo, símbolo ou texto.
- Identificar unidades, período e fuso horário nos dados temporais.
- Prever estados de carregamento, sem resultados, fonte indisponível, erro de carregamento e dados desatualizados.
- Manter linguagem simples nas áreas públicas e explicações técnicas acessíveis nas áreas analíticas.
- Ajustar controles para desktop e dispositivos móveis; no móvel, priorizar mapa, filtros essenciais e painel de detalhes expansível.

## 14. Requisitos não funcionais

- **Rastreabilidade:** resultados apresentados devem ser ligados a artefatos e fontes versionados.
- **Desempenho:** carregar mapa, camadas e gráficos de modo progressivo, reduzindo geometria e volume para a escala de visualização.
- **Confiabilidade:** exibir falhas e atualizações com clareza; não apresentar dados de cache como atuais sem rótulo.
- **Privacidade:** não coletar dados pessoais sem necessidade; não inferir ou divulgar dados individuais a partir de agregados populacionais.
- **Segurança:** proteger segredos e credenciais fora do bundle público do frontend; restringir operações administrativas se existirem.
- **Licenças:** atribuir corretamente dados OpenStreetMap e respeitar os termos de cada fonte.
- **Manutenibilidade:** manter interface, transformação de dados e documentação separados; centralizar tokens e componentes.

## 15. Critérios de aceite do MVP

- O usuário consegue abrir o Overview e navegar até o mapa, monitoramento, histórico, análises e sobre os dados.
- O mapa cobre o município de São Paulo e permite alternar as camadas disponibilizadas para a versão.
- Filtros de período e território atualizam as visualizações que compartilham o mesmo recorte.
- Os detalhes de uma ocorrência, estação ou área mostram atributos disponíveis, fonte e período de referência.
- Chuva apresenta unidade e período; suscetibilidade é identificada como suscetibilidade; registros são identificados como históricos/observados.
- Não há apresentação de classe/score como previsão enquanto o gate de Data Science não for aprovado.
- Estado de dado indisponível ou desatualizado é perceptível ao usuário.
- Fontes, metodologia, limitações e atribuições de licença estão acessíveis.
- Componentes principais são navegáveis por teclado e não dependem apenas de cor para comunicar estado.
- Dados sintéticos, caso usados em demonstração, são explicitamente rotulados e separados dos dados das fontes.

## 16. Indicadores de sucesso

- Cobertura das fontes prioritárias publicadas e identificadas no produto.
- Proporção das camadas com fonte, período, unidade e estado de atualização visíveis.
- Conclusão das tarefas de explorar uma área, filtrar período, abrir detalhe e localizar metodologia.
- Qualidade e consistência entre filtros do mapa, indicadores e séries temporais.
- Compreensão correta pelos usuários da diferença entre ocorrência histórica, observação, suscetibilidade, aviso oficial e estimativa.
- Desempenho de carregamento adequado ao volume real dos arquivos publicados, medido durante validação do frontend.

Metas numéricas de desempenho e usabilidade serão definidas depois de uma primeira implementação e teste com usuários.

## 17. Roadmap recomendado

### Fase 1 — MVP analítico

- Implementar tokens, navegação, Overview e mapa.
- Publicar camadas e filtros disponíveis a partir das tabelas Gold.
- Entregar monitoramento, histórico, suscetibilidade, detalhes e páginas de fontes/metodologia.
- Identificar limitações, qualidade e atualização dos dados.

### Fase 2 — Continuidade e orientação

- Construir base de conhecimento, perfis de experiência e assistente contextual.
- Integrar painel de qualidade e linhagem de forma legível para usuários.
- Adicionar alertas oficiais somente com fonte e validade explícitas.
- Integrar impacto potencial de população e vias se a qualidade e o desempenho forem adequados.

### Fase 3 — Modelos e expansão

- Avaliar modelos de risco quando o gate de dados e validação for atendido.
- Adicionar explicações de fatores, comparações e recomendações de exploração.
- Avaliar novas fontes e expansão geográfica após consolidar o piloto municipal.

## 18. Decisões em aberto

Estas decisões não impedem a especificação funcional, mas precisam ser confirmadas antes das partes correspondentes da implementação:

1. Perfis exigirão login real ou serão modos demonstrativos na entrega acadêmica?
2. Quais permissões distintas existirão para cidadão, analista e administrador?
3. Qual serviço/modelo atenderá o assistente de IA, e qual conteúdo poderá ser enviado a ele?
4. Qual será o formato e o processo automatizado de publicação dos arquivos Databricks para a Vercel?
5. Quais fontes, camadas e períodos terão dados completos na data de apresentação?
6. Quais critérios técnicos definem as faixas de alerta para os dados que tenham limiares oficiais?
7. O painel será responsivo para consulta móvel no MVP ou otimizado primeiro para desktop?
8. Quais análises e recomendações cabem no prazo acadêmico após o mapa central?

## 19. Referências de produto

- [Repositório ChuvaSegura — pipeline e modelo de dados](https://github.com/juniorOnData/FIAP-GS2-2TSCPV-2026)
- [Data Galaxy — referência de funcionalidades e organização da aplicação](https://github.com/MarcosMenezesdaSilva/data-galaxy)
- [Data Galaxy — aplicação publicada](https://data-galaxy-nexusops.netlify.app/)
- Documento fornecido: **CHUVASEGURA-SP — Design System & Visual Guidelines v1.0**.

Do Data Galaxy, o PDR preserva como referências de produto a central de dashboards, o assistente, a base de conhecimento, perfis de uso, modo de demonstração claramente identificado, qualidade dos dados e linhagem. A estrutura e as regras foram adaptadas ao domínio do ChuvaSegura-SP e às limitações do seu MVP.
