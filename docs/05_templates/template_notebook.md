# Template oficial de documentação de notebook

Este template vai na **primeira célula Markdown** de todo notebook do projeto. As 15 seções são obrigatórias. Quando uma seção não se aplicar, escreva `Não aplicável` seguido da justificativa.

Campos entre `<` e `>` devem ser substituídos. Os valores permitidos de cada campo estão no [padrão de nomenclatura](../01_arquitetura/padrao_de_nomenclatura.md) e no [documento de arquitetura](../01_arquitetura/documento_de_arquitetura.md).

## Como inserir no notebook

Os notebooks do repositório estão no formato de código fonte do Databricks. A primeira célula é Markdown e cada linha recebe o prefixo `MAGIC`:

| Linguagem do arquivo | Primeira linha do arquivo | Prefixo das linhas Markdown | Separador de células |
|---|---|---|---|
| Python (`.py`) | `# Databricks notebook source` | `# MAGIC` | `# COMMAND ----------` |
| SQL (`.sql`) | `-- Databricks notebook source` | `-- MAGIC` | `-- COMMAND ----------` |
| Scala (`.scala`) | `// Databricks notebook source` | `// MAGIC` | `// COMMAND ----------` |
| R (`.r`) | `# Databricks notebook source` | `# MAGIC` | `# COMMAND ----------` |

A primeira linha Markdown da célula é `%md` (por exemplo, `# MAGIC %md`). Nos notebooks editados pela interface do Databricks, basta criar uma célula Markdown no topo e colar o conteúdo abaixo.

## Template

```markdown
# <ID> <nome_do_notebook>

## 01. Identificação

| Campo | Valor |
|---|---|
| Identificador | <ID, por exemplo GEO_SLV_001> |
| Nome | <nome do arquivo sem extensão> |
| Projeto | ChuvaSegura |
| Domínio de negócio | <domínio> |
| Camada | <Landing, Bronze, Silver, Gold ou Qualidade> |
| Linguagem | <Python, SQL, Scala, R ou combinação, por exemplo SQL e Python> |
| Responsável técnico | <usuário ou nome> |
| Responsável funcional | <usuário ou nome, ou A definir> |
| Status | <Desenvolvimento, Homologação, Produção ou Descontinuado> |
| Versão | <MAJOR.MINOR, igual à última linha da seção 15> |
| Criticidade | <Alta, Média ou Baixa> |
| Caminho no repositório | <caminho a partir da raiz, com extensão> |

## 02. Objetivo

<Finalidade do notebook, problema que resolve e resultado esperado, em até cinco linhas.>

## 03. Contexto funcional

<Processo de negócio atendido e importância do notebook no projeto.>

## 04. Arquitetura e fluxo

| Posição | Item |
|---|---|
| Etapa anterior | <notebooks ou tarefas que precisam rodar antes> |
| Etapa atual | <o que este notebook faz, em uma linha> |
| Etapa posterior | <notebooks ou tarefas que consomem o resultado> |

## 05. Dependências

| Tipo | Item | Uso |
|---|---|---|
| <Notebook, Módulo, Biblioteca, Tabela, Volume, Arquivo, Job, Conexão, Secret ou Permissão> | <nome completo> | <para que é usado> |

## 06. Parâmetros de entrada

| Nome | Tipo | Obrigatório | Descrição | Valor padrão | Exemplo |
|---|---|---|---|---|---|
| <nome do widget> | <text ou dropdown, e o tipo do valor> | <Sim ou Não> | <descrição> | <valor padrão> | <exemplo> |

## 07. Fontes de dados

| Fonte | Formato | Origem | Tipo de leitura | Frequência | Chave | Filtros |
|---|---|---|---|---|---|---|
| <catálogo.schema.tabela, volume ou URL> | <Delta, JSON, GPKG...> | <sistema de origem> | <completa, incremental, por janela> | <frequência> | <chave> | <filtros aplicados> |

## 08. Processamento técnico

| Etapa | Linguagem | Célula | Descrição |
|---|---|---|---|
| E01 | <PY, SQL, SCALA ou R> | <título da célula> | <o que a célula faz> |

## 09. Regras de negócio

| ID | Regra | Lógica | Comportamento esperado |
|---|---|---|---|
| <RN_SIS_NNN> | <nome curto> | <condição ou fórmula> | <resultado esperado> |

## 10. Saídas de dados

| Destino | Formato | Granularidade | Chave | Modo de escrita |
|---|---|---|---|---|
| <catálogo.schema.tabela ou volume> | <Delta, JSON gzip...> | <uma linha por ...> | <colunas> | <append, overwrite com replaceWhere, MERGE, CREATE OR REPLACE> |

As colunas de cada tabela estão no [dicionário de dados](../02_catalogos/dicionario_de_dados.csv).

## 11. Qualidade dos dados

<Validações feitas no próprio notebook e regras do catálogo de qualidade que avaliam a saída. Critérios de aceitação, tratamento de inválidos, duplicidades e ausentes.>

## 12. Tratamento de erros

| Situação | Comportamento | Registro | Recuperação |
|---|---|---|---|
| <falha prevista> | <o que o notebook faz> | <onde fica registrado> | <como recuperar> |

## 13. Segurança e governança

| Item | Valor |
|---|---|
| Classificação dos dados | <Pública, Interna, Confidencial ou Restrita> |
| Dados sensíveis | <quais, ou Nenhum> |
| Credenciais | <secret scope e chaves, ou Nenhuma> |
| Permissões necessárias | <privilégios do Unity Catalog> |
| Políticas aplicáveis | <termos de uso, licenças, LGPD> |

## 14. Monitoramento e operação

| Item | Valor |
|---|---|
| Job e tarefa | <nome do job e task_key> |
| Frequência | <agenda> |
| SLA | <prazo, ou A definir> |
| Métricas | <o que acompanhar> |
| Logs | <tabelas de log> |
| Alertas | <notificações configuradas, ou Não configurado> |
| Reprocessamento | <como reprocessar, com referência ao procedimento> |

## 15. Histórico de alterações

| Versão | Data | Responsável | Descrição |
|---|---|---|---|
| 1.0 | <AAAA-MM-DD> | <responsável> | <descrição, com o número do PR> |
```

## Convenção para notebooks com mais de uma linguagem

1. A linguagem padrão do notebook é a do arquivo (`.py` para Python, `.sql` para SQL). Células em outra linguagem usam o comando mágico na primeira linha (`%sql`, `%python`, `%scala`, `%r`).
2. Cada célula de código começa com um comentário de etapa, no formato `<comentário> [<LING>] E<NN> <título>`, por exemplo `# [PY] E03 Registro da UDF de coordenadas` ou `-- [SQL] E04 Criação da tabela Silver`.
3. A seção 08 lista todas as etapas na ordem em que rodam, com a mesma numeração `E<NN>` e a linguagem. A descrição das transformações fica concentrada na seção 08 e nas regras da seção 09, e não espalhada em comentários longos dentro do código.
4. Objetos que passam de uma linguagem para outra (UDF registrada em Python e usada em SQL, view temporária criada em SQL e lida em Python, variável de sessão SQL) são declarados na seção 05 com o tipo `Objeto de sessão`, indicando a etapa que cria e a etapa que consome.
5. Comentários dentro do código explicam o porquê de uma decisão técnica. O que a etapa faz já está na seção 08.

O [exemplo prático](exemplo_notebook_etl.py) aplica todas essas regras num notebook com Python e SQL.
