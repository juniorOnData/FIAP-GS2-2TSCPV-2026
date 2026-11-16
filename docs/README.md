# Documentação do projeto ChuvaSegura

Esta pasta reúne a documentação oficial do projeto. Ela segue o padrão definido no [documento de arquitetura](01_arquitetura/documento_de_arquitetura.md) e vale para todos os notebooks, independentemente da linguagem.

## Onde fica cada informação

A documentação tem duas partes que não se repetem:

| Parte | Onde fica | O que contém |
|---|---|---|
| Documentação central | Pasta `docs/` | Padrões, arquitetura, fluxo completo, catálogos (notebooks, jobs, regras, dicionário de dados), procedimentos de operação, histórico do projeto |
| Documentação do notebook | Primeira célula Markdown de cada notebook em `src/` | As 15 seções do [template oficial](05_templates/template_notebook.md): o que aquele notebook faz, com quais dados, sob quais regras e como operá-lo |

Regra geral: se a informação vale para um notebook só, ela fica no notebook. Se vale para vários ou para o projeto, fica em `docs/` e o notebook faz referência a ela.

## Estrutura

```
docs/
  README.md                               Este índice
  historico_de_alteracoes.md              Histórico de alterações do projeto, por PR
  01_arquitetura/
    documento_de_arquitetura.md           Entregável 1: objetivo, escopo, premissas, padrões, responsabilidades
    fluxo_de_processamento.md             Fluxo ETL completo por fonte e por camada
    padrao_de_nomenclatura.md             Convenções de nomes de todos os objetos
    governanca_e_rastreabilidade.md       Identificação, versionamento, linhagem, classificação e controle de alterações
  02_catalogos/
    LEIAME.md                             Descrição dos campos e manutenção dos catálogos
    catalogo_notebooks.csv                Entregável 3: catálogo de notebooks
    catalogo_jobs.csv                     Entregável 5: catálogo de jobs
    catalogo_tarefas.csv                  Entregável 5: tarefas de cada job e suas dependências
    catalogo_regras_de_negocio.csv        Regras de negócio, com os notebooks que as aplicam
    catalogo_regras_de_qualidade.csv      Regras de qualidade de dados, com severidade e limite
    dicionario_de_dados.csv               Colunas de todas as tabelas do projeto
  03_jobs/
    JOB_CHUVASEGURA_GEOSAMPA.md           Documentação do job ChuvaSegura (GeoSampa)
    JOB_CHUVASEGURA_CEMADEN.md            Documentação do job do CEMADEN
    JOB_CHUVASEGURA_SIBH.md               Documentação do job do SIBH
  04_operacao/
    procedimentos_operacionais.md         Primeira execução, cargas manuais e rotinas
    monitoramento_e_tratamento_de_erros.md  Logs, consultas de acompanhamento e resposta a falhas
    reprocessamento.md                    Como reprocessar cada fonte e camada
  05_templates/
    template_notebook.md                  Entregável 2: template oficial de notebook
    template_job.md                       Template de documentação de job
    exemplo_notebook_etl.py               Entregável 4: notebook fictício documentado, com Python e SQL
  06_conformidade/
    checklist_conformidade.md             Entregável 6: checklist de revisão e aprovação
  07_implantacao/
    plano_de_implantacao.md               Entregável 7: plano de implantação do padrão
```

Fora desta pasta, o arquivo `.github/pull_request_template.md` traz o checklist resumido em todo pull request.

## Por onde começar

| Perfil | Leitura recomendada |
|---|---|
| Quem chega ao projeto | [Documento de arquitetura](01_arquitetura/documento_de_arquitetura.md), [fluxo de processamento](01_arquitetura/fluxo_de_processamento.md) e o [catálogo de notebooks](02_catalogos/catalogo_notebooks.csv) |
| Quem vai criar ou alterar um notebook | [Template](05_templates/template_notebook.md), [exemplo](05_templates/exemplo_notebook_etl.py), [nomenclatura](01_arquitetura/padrao_de_nomenclatura.md) e [checklist](06_conformidade/checklist_conformidade.md) |
| Quem opera os jobs | [Jobs](03_jobs/), [procedimentos](04_operacao/procedimentos_operacionais.md), [monitoramento](04_operacao/monitoramento_e_tratamento_de_erros.md) e [reprocessamento](04_operacao/reprocessamento.md) |
| Quem consome os dados | [Dicionário de dados](02_catalogos/dicionario_de_dados.csv), [regras de negócio](02_catalogos/catalogo_regras_de_negocio.csv) e [regras de qualidade](02_catalogos/catalogo_regras_de_qualidade.csv) |
