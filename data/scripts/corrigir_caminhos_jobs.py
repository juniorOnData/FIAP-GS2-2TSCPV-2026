"""Corrige, nos jobs do workspace Databricks, os caminhos dos notebooks que ficaram na estrutura antiga.

Os notebooks saíram de  .../FIAP-GS2-2TSCPV-2026/src/...  para  .../FIAP-GS2-2TSCPV-2026/data/src/...
Os jobs já criados no workspace continuam com o caminho antigo e falham com
"Unable to access the notebook". Este script troca só o caminho de cada tarefa de notebook
(o resto do job, como agenda, parâmetros e dependências, não é tocado).

Uso (a partir da raiz do repositório):
    export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
    export DATABRICKS_TOKEN=<token pessoal>        # nunca versione o token
    python data/scripts/corrigir_caminhos_jobs.py              # simulação: só lista o que mudaria
    python data/scripts/corrigir_caminhos_jobs.py --aplicar    # grava as alterações

Só mexe em jobs cujo nome começa com "ChuvaSegura". Rodar de novo é seguro: o que já está certo é ignorado.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

PREFIXO_JOB = "ChuvaSegura"
RAIZ_REPO = "/FIAP-GS2-2TSCPV-2026/"
ANTIGO = RAIZ_REPO + "src/"
NOVO = RAIZ_REPO + "data/src/"


def api(host, token, metodo, caminho, corpo=None):
    req = urllib.request.Request(
        f"{host}{caminho}", method=metodo,
        data=json.dumps(corpo).encode("utf-8") if corpo is not None else None,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f"Erro {e.code} em {metodo} {caminho}: {e.read().decode('utf-8', 'replace')[:400]}")


def listar_jobs(host, token):
    jobs, token_pagina = [], None
    while True:
        q = "/api/2.1/jobs/list?limit=100" + (f"&page_token={token_pagina}" if token_pagina else "")
        r = api(host, token, "GET", q)
        jobs += r.get("jobs", [])
        token_pagina = r.get("next_page_token")
        if not token_pagina:
            return jobs


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--aplicar", action="store_true", help="grava as alterações (sem isso, só simula)")
    args = ap.parse_args()

    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not host or not token:
        sys.exit("Defina DATABRICKS_HOST e DATABRICKS_TOKEN.")
    if not host.startswith("https://"):
        host = "https://" + host

    total_jobs = total_tarefas = 0
    for resumo in listar_jobs(host, token):
        nome = resumo.get("settings", {}).get("name", "")
        if not nome.startswith(PREFIXO_JOB):
            continue
        job_id = resumo["job_id"]
        job = api(host, token, "GET", f"/api/2.1/jobs/get?job_id={job_id}")
        mudancas = []
        for t in job["settings"].get("tasks", []):
            nb = t.get("notebook_task")
            if nb and ANTIGO in nb.get("notebook_path", ""):
                novo = dict(nb, notebook_path=nb["notebook_path"].replace(ANTIGO, NOVO))
                mudancas.append({"task_key": t["task_key"], "notebook_task": novo})
        if not mudancas:
            print(f"  ok        {nome} (job {job_id})")
            continue
        total_jobs += 1
        total_tarefas += len(mudancas)
        print(f"  {'CORRIGIR' if args.aplicar else 'corrigiria'} {nome} (job {job_id}): {len(mudancas)} tarefa(s)")
        for m in mudancas:
            print(f"      {m['task_key']}: {m['notebook_task']['notebook_path']}")
        if args.aplicar:
            # jobs/update mescla as tarefas pela task_key: só as tarefas enviadas são alteradas.
            api(host, token, "POST", "/api/2.1/jobs/update", {"job_id": job_id, "new_settings": {"tasks": mudancas}})

    print(f"\n{total_tarefas} tarefa(s) em {total_jobs} job(s) {'corrigidas' if args.aplicar else 'a corrigir'}."
          + ("" if args.aplicar else " Rode com --aplicar para gravar."))


if __name__ == "__main__":
    main()
