# Plano de implantação do padrão de documentação

O padrão foi aplicado a todos os notebooks existentes no PR #15. As fases seguintes completam o que depende de decisão da equipe ou de configuração no workspace.

| Fase | Prioridade | Atividades | Responsável | Resultado esperado |
|---|---|---|---|---|
| 1. Base documental | Alta | Template de notebook e de job, catálogos, dicionário de dados, documentos de arquitetura, nomenclatura, governança, operação, checklist e template de PR; primeira célula de todos os notebooks no padrão | juniorOnData | Concluída no PR #15 |
| 2. Validação funcional | Alta | Definir o responsável funcional de cada domínio; confirmar criticidade, SLA e classificação dos dados; revisar as descrições "a confirmar" do dicionário | Responsável técnico e responsável funcional | Campos "A definir" preenchidos nos notebooks e catálogos |
| 3. Adoção no fluxo de PR | Alta | Usar o checklist do PR em toda revisão; exigir revisão por outra pessoa da equipe; recusar PR que altere notebook sem atualizar a seção 15 e os catálogos | Equipe | Documentação sempre atualizada junto com o código |
| 4. Operação monitorada | Média | Configurar notificação de falha nos três jobs; avaliar timeout e retentativas nas tarefas que chamam APIs; acompanhar as consultas de monitoramento | Responsável técnico | Falhas percebidas no mesmo dia |
| 5. Governança no Unity Catalog | Média | Aplicar tags de classificação e domínio nas tabelas; definir grupos e permissões por perfil; restringir o secret scope `cemaden` | Responsável técnico | Classificação e acesso controlados pela plataforma |
| 6. Padronização de nomes | Baixa | Ajustar os nomes dos jobs e das tarefas do GeoSampa ao padrão; unificar o idioma dos parâmetros de CEMADEN e SIBH quando essas fontes forem refatoradas | Responsável técnico | Divergências do padrão de nomenclatura zeradas |
| 7. Implantação automatizada | Baixa | Criar o `databricks.yml` e implantar os jobs com Declarative Automation Bundles, vinculando os jobs existentes | Responsável técnico | Jobs do workspace iguais aos do repositório |
| 8. Novas camadas | Contínua | Silver e Gold de CEMADEN e SIBH já documentadas no padrão desde o primeiro PR, com regras de qualidade | Equipe | Novos notebooks nascem conformes |

## Indicadores de adoção

| Indicador | Meta |
|---|---|
| Notebooks com as 15 seções | 100% (atingido no PR #15) |
| Notebooks com responsável funcional definido | 100% ao fim da fase 2 |
| PRs com checklist preenchido | 100% a partir da fase 3 |
| Jobs com notificação de falha | 3 de 3 ao fim da fase 4 |
| Divergências de nomenclatura | Zero ao fim da fase 6 |
