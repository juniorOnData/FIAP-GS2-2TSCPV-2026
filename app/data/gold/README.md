# Publicação Gold

Esta pasta recebe os artefatos estáticos exportados do Databricks.

Para a primeira integração, copie para cá `manifest.json`, `ocorrencias.json` e `subprefeituras.json` gerados pela tarefa `export_web` do job `ChuvaSegura` (`data/src/shared/export/01_export_web.py`), no Volume `/Volumes/fiap_analytics/gold/chuvasegura_web/`. Não copie arquivos de `data/demo/` para esta pasta. Após incluir um manifesto válido, altere `DATA_ROOT` em `app/assets/js/config.js` para `data/gold`.
