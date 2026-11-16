# Catálogos

Os catálogos são arquivos CSV (UTF-8, separados por vírgula), versionados junto com o código. O GitHub mostra cada arquivo como tabela. No Excel, abra por *Dados > Obter dados > De Texto/CSV*, escolhendo a origem UTF-8 e o delimitador vírgula.

## catalogo_notebooks.csv

Uma linha por notebook de `src/`.

| Campo | Descrição |
|---|---|
| `id_notebook` | Identificador único (`<FONTE>_<CAMADA>_<NNN>`) |
| `nome` | Nome do arquivo sem extensão |
| `descricao` | Primeira frase do objetivo (seção 02 do notebook) |
| `dominio` | Domínio de negócio |
| `camada` | Landing, Bronze, Silver, Gold ou Qualidade |
| `linguagem` | Linguagens das células |
| `job` | Nome do job que executa o notebook |
| `tarefa` | `task_key` da tarefa no job |
| `origem_dados` | Fontes de dados (seção 07) |
| `destino_dados` | Saídas (seção 10) |
| `responsavel_tecnico` | Responsável técnico |
| `responsavel_funcional` | Responsável funcional |
| `status` | Desenvolvimento, Homologação, Produção ou Descontinuado |
| `criticidade` | Alta, Média ou Baixa |
| `versao` | Versão atual do notebook (seção 01) |
| `ultima_atualizacao_doc` | Data da última atualização da documentação do notebook |
| `caminho` | Caminho do arquivo no repositório |

## catalogo_jobs.csv

Uma linha por job.

| Campo | Descrição |
|---|---|
| `id_job` | Identificador (`JOB_CHUVASEGURA_<FONTE>`) |
| `nome_job` | Nome exibido no Lakeflow Jobs |
| `chave_recurso` | Chave do job no YAML (`resources.jobs.<chave>`) |
| `arquivo_definicao` | Arquivo YAML em `jobs/` |
| `objetivo` | O que o job entrega |
| `agenda_cron`, `fuso`, `horario` | Agenda (expressão Quartz), fuso horário e horário legível |
| `situacao_agenda` | Ativa ou Pausada |
| `tarefas` | Número de tarefas |
| `fila`, `compute`, `performance` | Configuração de execução |
| `retentativas`, `timeout`, `notificacoes` | Políticas configuradas no job |
| `responsavel_tecnico`, `responsavel_funcional` | Responsáveis |

## catalogo_tarefas.csv

Uma linha por tarefa de job.

| Campo | Descrição |
|---|---|
| `id_job` | Job da tarefa |
| `task_key` | Chave da tarefa |
| `id_notebook`, `notebook` | Notebook executado |
| `depende_de` | Tarefas que precisam terminar antes |
| `parametros` | Parâmetros passados ao notebook (`base_parameters`) |
| `retentativas`, `timeout` | Políticas da tarefa |
| `criterio_sucesso` | Condição para a tarefa ser considerada bem sucedida |

## catalogo_regras_de_negocio.csv

Uma linha por regra de negócio. O detalhe de cada regra fica na seção 09 dos notebooks listados.

| Campo | Descrição |
|---|---|
| `id_regra` | Identificador (`RN_<FONTE>_<NNN>`) |
| `nome` | Nome curto |
| `dominio` | Sigla da fonte |
| `camadas` | Camadas em que a regra é aplicada |
| `notebooks` | Notebooks que aplicam a regra |
| `logica` | Condição ou fórmula |
| `comportamento_esperado` | Resultado esperado |
| `status` | Vigente ou Revogada |

## catalogo_regras_de_qualidade.csv

Uma linha por regra de qualidade de dados, na ordem em que aparece no notebook de qualidade.

| Campo | Descrição |
|---|---|
| `id_regra` | Identificador (`DQ_<FONTE>_<CAMADA>_<NNN>`) |
| `nome_no_codigo` | Nome da regra no notebook e na coluna `regra` da tabela de resultados |
| `tabela` | Tabela avaliada |
| `etapa` | Etapa de qualidade (silver ou gold) |
| `severidade` | ERRO (reprova a tarefa) ou AVISO (só registra) |
| `limite` | Proporção máxima de falhas aceita (0 = nenhuma) |
| `descricao` | O que a regra verifica |
| `notebook` | Notebook de qualidade que executa a regra |

## dicionario_de_dados.csv

Uma linha por coluna de cada tabela do projeto.

| Campo | Descrição |
|---|---|
| `camada` | bronze, silver, gold ou controle |
| `tabela` | Nome completo da tabela |
| `coluna` | Nome da coluna |
| `tipo` | Tipo de dado |
| `descricao` | Significado da coluna |
| `chave` | PK, PK parcial, NK, NK parcial ou FK com a tabela de referência |
| `regra` | Regra de negócio que gera ou usa a coluna |
| `notebook_origem` | Notebook que grava a tabela |

Descrições marcadas como "a confirmar" dependem da documentação da fonte e devem ser revisadas pelo responsável funcional.

## Como manter os catálogos atualizados

1. O catálogo é atualizado **no mesmo PR** que altera o notebook, o job ou a tabela. O checklist do PR cobra isso.
2. Notebook novo: uma linha no catálogo de notebooks, as tarefas no catálogo de tarefas, as colunas no dicionário e as regras nos catálogos de regras.
3. Notebook alterado: atualizar `versao`, `ultima_atualizacao_doc` e os campos afetados.
4. Regra de qualidade nova: acrescentar no fim da lista do notebook de qualidade, com o próximo número livre. Regras existentes não mudam de número.
5. Na revisão semestral, conferir o catálogo de tarefas contra os YAMLs de `jobs/` e o dicionário contra o `DESCRIBE TABLE` de cada tabela.
