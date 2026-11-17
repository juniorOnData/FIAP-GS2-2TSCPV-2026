"""Publicação de arquivos no GitHub por commit (Git Data API), sem clonar o repositório.

Usado pelo export do site: a Vercel republica o app a cada commit na branch de produção.
O token (permissão "Contents: read and write" só neste repositório) vem de um secret scope do Databricks
e nunca é gravado em arquivo, log ou notebook.
"""

import base64
import json
import time
import urllib.error
import urllib.request

API = "https://api.github.com"


class GitHubErro(RuntimeError):
    pass


def _chamar(token, metodo, caminho, corpo=None, api=API, tentativas=3):
    dados = json.dumps(corpo).encode("utf-8") if corpo is not None else None
    for tentativa in range(1, tentativas + 1):
        req = urllib.request.Request(
            f"{api}{caminho}", data=dados, method=metodo,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                     "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "chuvasegura-export",
                     "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            detalhe = e.read().decode("utf-8", "replace")[:300]
            if e.code in (500, 502, 503, 504) and tentativa < tentativas:
                time.sleep(2 * tentativa)
                continue
            raise GitHubErro(f"GitHub {metodo} {caminho} respondeu {e.code}: {detalhe}") from None
        except urllib.error.URLError as e:
            if tentativa < tentativas:
                time.sleep(2 * tentativa)
                continue
            raise GitHubErro(f"GitHub {metodo} {caminho} sem resposta: {e.reason}") from None


def ler_json(token, repo, branch, caminho, api=API):
    """Conteúdo JSON de um arquivo da branch, ou None se não existir."""
    try:
        r = _chamar(token, "GET", f"/repos/{repo}/contents/{caminho}?ref={branch}", api=api)
    except GitHubErro as e:
        if " 404:" in str(e):
            return None
        raise
    if r.get("encoding") == "base64" and r.get("content"):
        return json.loads(base64.b64decode(r["content"]).decode("utf-8"))
    # arquivos acima de 1 MB: a API de conteúdo não devolve o corpo; usa o blob
    blob = _chamar(token, "GET", f"/repos/{repo}/git/blobs/{r['sha']}", api=api)
    return json.loads(base64.b64decode(blob["content"]).decode("utf-8"))


def publicar(token, repo, branch, arquivos, mensagem, api=API):
    """Cria um commit na branch com os arquivos ({caminho: bytes}), sem tocar nos demais.
    Retorna o SHA do commit, ou None se nada mudou. Não força o push: se a branch andou, falha e o job tenta de novo."""
    ref = _chamar(token, "GET", f"/repos/{repo}/git/ref/heads/{branch}", api=api)
    base_commit = ref["object"]["sha"]
    base_tree = _chamar(token, "GET", f"/repos/{repo}/git/commits/{base_commit}", api=api)["tree"]["sha"]
    itens = []
    for caminho, conteudo in sorted(arquivos.items()):
        blob = _chamar(token, "POST", f"/repos/{repo}/git/blobs",
                       {"content": base64.b64encode(conteudo).decode("ascii"), "encoding": "base64"}, api=api)
        itens.append({"path": caminho, "mode": "100644", "type": "blob", "sha": blob["sha"]})
    arvore = _chamar(token, "POST", f"/repos/{repo}/git/trees", {"base_tree": base_tree, "tree": itens}, api=api)["sha"]
    if arvore == base_tree:
        return None
    commit = _chamar(token, "POST", f"/repos/{repo}/git/commits",
                     {"message": mensagem, "tree": arvore, "parents": [base_commit]}, api=api)["sha"]
    _chamar(token, "PATCH", f"/repos/{repo}/git/refs/heads/{branch}", {"sha": commit, "force": False}, api=api)
    return commit
