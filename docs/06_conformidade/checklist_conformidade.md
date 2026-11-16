# Checklist de conformidade

O checklist é usado pelo revisor em todo pull request que cria ou altera notebook, job ou tabela. Os itens marcados como **Obrigatório** em um estágio impedem a aprovação do PR se não forem atendidos. A versão resumida está no `.github/pull_request_template.md`.

## A. Primeira célula do notebook

| Nº | Item | Homologação | Produção |
|---|---|---|---|
| A01 | A primeira célula é Markdown e tem as 15 seções do template, na ordem e com os títulos do template | Obrigatório | Obrigatório |
| A02 | Seções que não se aplicam dizem "Não aplicável" e trazem a justificativa | Obrigatório | Obrigatório |
| A03 | Identificador no formato `<FONTE>_<CAMADA>_<NNN>`, único e igual ao do catálogo | Obrigatório | Obrigatório |
| A04 | Camada, status e criticidade usam os valores padronizados do documento de arquitetura | Obrigatório | Obrigatório |
| A05 | Responsável técnico preenchido | Obrigatório | Obrigatório |
| A06 | Responsável funcional preenchido | Recomendado | Obrigatório para notebooks novos |
| A07 | Objetivo (02) explica o problema e o resultado, sem descrever o código | Obrigatório | Obrigatório |
| A08 | Fluxo (04) indica etapa anterior e posterior, coerentes com o job | Obrigatório | Obrigatório |
| A09 | Dependências (05) listam tabelas, volumes, módulos, secrets, permissões e objetos de sessão entre linguagens | Obrigatório | Obrigatório |
| A10 | Todos os widgets do código aparecem nos parâmetros (06), com valor padrão igual ao do código | Obrigatório | Obrigatório |
| A11 | Fontes (07) e saídas (10) com nome completo `catálogo.schema.tabela` ou caminho do volume, chave e modo de escrita | Obrigatório | Obrigatório |
| A12 | Processamento (08) tem uma linha por célula de código, na ordem, com a linguagem | Obrigatório | Obrigatório |
| A13 | Regras de negócio (09) com identificador `RN_`, lógica e comportamento esperado, separadas das transformações técnicas | Recomendado | Obrigatório |
| A14 | Qualidade (11) cita as validações do notebook e as regras `DQ_` que avaliam a saída | Recomendado | Obrigatório |
| A15 | Tratamento de erros (12) cobre as falhas previstas e a recuperação | Obrigatório | Obrigatório |
| A16 | Segurança (13) com classificação, credenciais e permissões | Recomendado | Obrigatório |
| A17 | Monitoramento (14) com job, tarefa, logs e reprocessamento | Recomendado | Obrigatório |
| A18 | Histórico (15) com uma linha nova citando o PR, e versão (01) igual à última linha | Obrigatório | Obrigatório |

## B. Código

| Nº | Item | Homologação | Produção |
|---|---|---|---|
| B01 | Toda célula de código começa com o comentário de etapa `[LING] Enn <título>`, igual à seção 08 | Obrigatório | Obrigatório |
| B02 | Nomes de notebook, tabelas, colunas e parâmetros seguem o padrão de nomenclatura | Obrigatório | Obrigatório |
| B03 | Nenhuma credencial, token ou senha no código; credenciais só por secret scope | Obrigatório | Obrigatório |
| B04 | O notebook é idempotente: rodar de novo a mesma janela não duplica dados | Recomendado | Obrigatório |
| B05 | Cargas gravam no log de ingestão da origem (landing e Bronze) | Recomendado | Obrigatório |
| B06 | Tabelas novas têm `COMMENT` na tabela e nas colunas | Recomendado | Obrigatório |

## C. Documentação central

| Nº | Item | Homologação | Produção |
|---|---|---|---|
| C01 | Linha do notebook no `catalogo_notebooks.csv` criada ou atualizada (versão e data) | Obrigatório | Obrigatório |
| C02 | Colunas de tabelas novas ou alteradas no `dicionario_de_dados.csv` | Recomendado | Obrigatório |
| C03 | Regras novas ou alteradas nos catálogos de regras de negócio e de qualidade | Recomendado | Obrigatório |
| C04 | Tarefa no YAML do job, no `catalogo_tarefas.csv` e na documentação do job em `docs/03_jobs/` | Não se aplica | Obrigatório |
| C05 | Fluxo de processamento atualizado quando o notebook cria uma tabela ou etapa nova | Recomendado | Obrigatório |
| C06 | Linha no `historico_de_alteracoes.md` do projeto | Obrigatório | Obrigatório |

## D. Pull request

| Nº | Item | Homologação | Produção |
|---|---|---|---|
| D01 | Descrição com problema, mudanças, testes realizados e passos depois do merge | Obrigatório | Obrigatório |
| D02 | Informação de que foi ou não executado no Databricks, e o resultado | Obrigatório | Obrigatório |
| D03 | Impacto em consumidores descrito quando a saída muda de forma incompatível (versão MAJOR) | Obrigatório | Obrigatório |
| D04 | Revisão por uma pessoa diferente do autor | Recomendado | Obrigatório |

## Critérios de aprovação

| Estágio | Condição para aprovar |
|---|---|
| Homologação | Todos os itens obrigatórios de homologação atendidos |
| Produção | Todos os itens obrigatórios de produção atendidos e o notebook executado com sucesso no job de destino |
