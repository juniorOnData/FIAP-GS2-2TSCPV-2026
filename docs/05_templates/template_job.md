# Template de documentação de job

Cada job do Lakeflow Jobs tem um arquivo `docs/03_jobs/<ID_DO_JOB>.md` com as seções abaixo. A definição técnica do job fica no YAML de `jobs/`; este documento explica o job e precisa ser atualizado no mesmo PR que alterar o YAML.

```markdown
# <ID_DO_JOB> <nome exibido>

## 1. Identificação

| Campo | Valor |
|---|---|
| Identificador | <JOB_CHUVASEGURA_FONTE> |
| Nome exibido | <nome no Lakeflow Jobs> |
| Arquivo de definição | <jobs/arquivo.job.yml> |
| Responsável técnico | <responsável> |
| Responsável funcional | <responsável ou A definir> |
| Situação | <Ativo, Pausado ou Descontinuado> |

## 2. Objetivo

<O que o job entrega e para quem.>

## 3. Frequência e horário

| Campo | Valor |
|---|---|
| Agenda (Quartz) | <expressão> |
| Fuso horário | <fuso> |
| Horário | <horário legível> |
| Situação da agenda | <Ativa ou Pausada> |
| Fila | <Habilitada ou Desabilitada> |

## 4. Tarefas executadas

| Ordem | Tarefa | Notebook | Depende de | Parâmetros |
|---|---|---|---|---|
| <n> | <task_key> | <ID e nome do notebook> | <tarefas> | <base_parameters> |

## 5. Sequência de execução

<Diagrama Mermaid com as tarefas e as dependências.>

## 6. Políticas de execução

| Política | Configuração |
|---|---|
| Retentativas | <número e intervalo, ou Não configurado> |
| Timeout | <duração, ou Não configurado> |
| Notificações | <destinatários e eventos, ou Não configurado> |
| Compute | <serverless ou cluster, ambiente> |

## 7. Critérios de sucesso e falha

<Quando o job é considerado bem sucedido e o que caracteriza falha.>

## 8. Estratégia de reprocessamento

<Como reprocessar uma janela ou o job inteiro, com referência a docs/04_operacao/reprocessamento.md.>

## 9. Histórico

| Data | Responsável | Descrição |
|---|---|---|
| <AAAA-MM-DD> | <responsável> | <descrição e PR> |
```
