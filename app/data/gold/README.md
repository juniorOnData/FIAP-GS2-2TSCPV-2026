# Publicação Gold

Esta pasta recebe os artefatos estáticos exportados do Databricks.

Para a primeira integração, copie para cá `manifest.json`, `ocorrencias.json` e `subprefeituras.json` gerados pelo notebook `data/src/geosampa/export/06_export_geosampa_web.py`. Não copie arquivos de `data/demo/` para esta pasta. Após incluir um manifesto válido, altere `DATA_ROOT` em `app/assets/js/config.js` para `data/gold`.
