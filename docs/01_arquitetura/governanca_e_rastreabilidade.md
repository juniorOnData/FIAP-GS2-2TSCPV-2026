# Governança e rastreabilidade

## 1. Identificação dos notebooks

1. Todo notebook recebe um identificador no formato do [padrão de nomenclatura](padrao_de_nomenclatura.md), registrado na seção 01 da primeira célula e no [catálogo de notebooks](../02_catalogos/catalogo_notebooks.csv).
2. O identificador não muda quando o arquivo é renomeado ou movido. O catálogo é atualizado com o caminho novo.
3. Um identificador nunca é reaproveitado.

## 2. Versionamento no Git

| Item | Regra |
|---|---|
| Fonte oficial | A branch `main` do repositório `juniorOnData/FIAP-GS2-2TSCPV-2026` |
| Alterações | Sempre numa branch própria, integradas por pull request |
| Branch | `<tipo>/<descricao>`, com tipo `feat`, `fix`, `docs` ou `chore` |
| Commit | Mensagem em português, com título curto e corpo explicando o porquê |
| Pull request | Descrição do problema, do que mudou, de como foi testado e do que fazer depois do merge; checklist do `.github/pull_request_template.md` preenchido |
| Workspace | O Git folder do workspace é atualizado (*Pull*) depois do merge; ninguém edita a `main` direto pela interface |

## 3. Versão e histórico de cada notebook

1. A versão do notebook (seção 01) é igual à última linha do histórico (seção 15).
2. Cada PR que altera um notebook acrescenta uma linha no histórico, com a data, o responsável e a descrição, citando o número do PR.
3. O histórico do projeto como um todo fica em [historico_de_alteracoes.md](../historico_de_alteracoes.md).

## 4. Responsáveis

Todo notebook tem um responsável técnico e um responsável funcional na seção 01 e no catálogo. Enquanto o responsável funcional não for definido, o campo fica como "A definir" e o notebook não pode passar para Produção sem essa definição em notebooks novos (ver [checklist](../06_conformidade/checklist_conformidade.md)).

## 5. Rastreabilidade entre origem e destino

A rastreabilidade combina recursos nativos e controles desenvolvidos:

| Pergunta | Como responder | Tipo |
|---|---|---|
| Quais tabelas alimentam esta tabela, e quais notebooks e jobs a gravaram? | Aba *Lineage* da tabela no Catalog Explorer, ou as tabelas de sistema de linhagem (`system.access.table_lineage` e `system.access.column_lineage`) | Nativo (Unity Catalog) |
| De qual arquivo veio esta linha? | Colunas `_source_file`, `_source_sha256` e `_source_layer` (Bronze e Silver do GeoSampa) ou `_source_file` e `_landing_batch_id` (CEMADEN e SIBH) | Desenvolvido |
| Qual execução gravou esta linha? | `_batch_id` na Bronze, que aparece também no log de ingestão da origem | Desenvolvido |
| A janela foi completa ou parcial? | `status` no log de ingestão (`<origem>_ingestion_log`) | Desenvolvido |
| A tabela passou nas regras de qualidade na última execução? | `geosampa_data_quality_log` | Desenvolvido |
| Qual notebook aplica a regra de negócio X? | [Catálogo de regras de negócio](../02_catalogos/catalogo_regras_de_negocio.csv) e seção 09 do notebook | Desenvolvido |

A linhagem nativa do Unity Catalog é capturada automaticamente para leituras e gravações em tabelas e volumes do Unity Catalog. Ela não cobre as chamadas às APIs externas nem os arquivos enviados manualmente ao volume; essa parte é coberta pelos manifestos e pelos logs de ingestão.

## 6. Classificação e segurança

| Item | Situação atual | Recomendação |
|---|---|---|
| Classificação dos dados | Registrada na seção 13 de cada notebook como Pública (dados abertos), a confirmar com os termos de uso | Aplicar tags do Unity Catalog nas tabelas (por exemplo `classificacao = publica`, `dominio = geosampa`) |
| Credenciais | Secret scope `cemaden`; nunca impressas nem gravadas | Restringir a leitura do scope ao grupo que executa os jobs |
| Permissões | Não definidas no repositório | Grupos por perfil: engenharia (escrita em `bronze`, `silver` e `gold`), consumo (leitura em `gold`), operação (leitura nos logs) |
| Dados pessoais | Nenhum identificado | Reavaliar a cada nova fonte |

## 7. Revisão da documentação

| Momento | Revisão |
|---|---|
| Em todo PR que altera notebook, job ou tabela | O revisor confere o checklist do PR |
| A cada nova fonte ou camada | O responsável técnico revisa o fluxo de processamento e os catálogos |
| Semestral | O responsável funcional revisa objetivo, regras, criticidade e classificação dos notebooks em Produção |

## 8. Controle de alterações

1. Mudança de saída incompatível (nome de tabela, granularidade ou chave) é uma versão MAJOR: o PR descreve o impacto nos consumidores e o procedimento de migração.
2. Mudança em regra de negócio exige atualizar a seção 09 do notebook e o catálogo de regras no mesmo PR.
3. Mudança em regra de qualidade exige atualizar a lista do notebook de qualidade e o catálogo de regras de qualidade no mesmo PR.
4. Mudança na sequência de tarefas exige atualizar o YAML do job, a documentação do job e o catálogo de tarefas no mesmo PR.
