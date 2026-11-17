"""Download de arquivos publicados pelas fontes do projeto ChuvaSegura (SGB, IBGE, OpenStreetMap e INMET).

- `metadados_remotos`: tamanho, data de modificação e ETag do arquivo, sem baixá-lo (HEAD).
- `baixar`: download em partes, com novas tentativas para 429, 5xx e erros de rede, e SHA256 calculado na gravação.
- `ZipRemoto`: lê o índice e os membros de um zip publicado sem baixar o arquivo inteiro (requisições HTTP Range).
  Os pacotes dessas fontes têm centenas de MB ou alguns GB, e o projeto usa só algumas camadas de cada um.
- `extrair_do_zip`: grava num zip novo só os membros escolhidos, com o conteúdo exatamente como veio da fonte.

Os arquivos são gravados primeiro num diretório temporário local e depois copiados para o volume, como nas
outras landings do projeto.
"""
import glob
import hashlib
import io
import json
import os
import re
import shutil
import tempfile
import time
import zipfile

import requests

USER_AGENT = "ChuvaSegura-FIAP/1.0 (projeto academico)"
TENTATIVAS = 5
TIMEOUT_S = 120
PARTE_BYTES = 8 << 20  # 8 MB por requisição


def _cabecalhos(extra=None):
    return {"User-Agent": USER_AGENT, **(extra or {})}


def _requisitar(metodo, url, cabecalhos=None, stream=False, permitir=(200,)):
    """Requisição com novas tentativas e espera crescente para 429, 5xx e erros de rede."""
    ultimo = None
    for tentativa in range(1, TENTATIVAS + 1):
        try:
            r = requests.request(metodo, url, headers=_cabecalhos(cabecalhos), timeout=TIMEOUT_S,
                                 stream=stream, allow_redirects=True)
        except (requests.ConnectionError, requests.Timeout) as exc:
            ultimo = type(exc).__name__
        else:
            if r.status_code in permitir:
                return r
            if r.status_code != 429 and r.status_code < 500:
                raise RuntimeError(f"{url} respondeu {r.status_code}: {r.text[:200]}")
            ultimo = f"status {r.status_code}"
        espera = min(60, 5 * 2 ** (tentativa - 1))
        print(f"   {url}: {ultimo}. Nova tentativa em {espera}s ({tentativa}/{TENTATIVAS})", flush=True)
        time.sleep(espera)
    raise RuntimeError(f"{url} falhou após {TENTATIVAS} tentativas: {ultimo}")


def metadados_remotos(url):
    """{'url_final', 'tamanho', 'ultima_modificacao', 'etag', 'aceita_range'} do arquivo, via HEAD."""
    r = _requisitar("HEAD", url)
    tamanho = r.headers.get("Content-Length")
    return {
        "url_final": r.url,
        "tamanho": int(tamanho) if tamanho else None,
        "ultima_modificacao": r.headers.get("Last-Modified"),
        "etag": r.headers.get("ETag"),
        "aceita_range": r.headers.get("Accept-Ranges", "").lower() == "bytes",
    }


def sha256_arquivo(caminho):
    digest = hashlib.sha256()
    with open(caminho, "rb") as f:
        for parte in iter(lambda: f.read(1 << 20), b""):
            digest.update(parte)
    return digest.hexdigest()


def baixar(url, destino):
    """Baixa `url` para o caminho local `destino`. Devolve (sha256, bytes). Tamanho conferido com o Content-Length."""
    digest, total = hashlib.sha256(), 0
    r = _requisitar("GET", url, stream=True)
    esperado = r.headers.get("Content-Length")
    with open(destino, "wb") as f:
        for parte in r.iter_content(1 << 20):
            f.write(parte)
            digest.update(parte)
            total += len(parte)
    if esperado and int(esperado) != total:
        raise RuntimeError(f"{url}: baixados {total} bytes, o servidor anunciou {esperado}")
    return digest.hexdigest(), total


class _ArquivoHttp(io.RawIOBase):
    """Arquivo remoto somente leitura com posicionamento, lido por requisições HTTP Range."""

    def __init__(self, url, tamanho):
        self.url, self.tamanho, self.pos = url, tamanho, 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.pos

    def seek(self, deslocamento, origem=io.SEEK_SET):
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self.pos, io.SEEK_END: self.tamanho}[origem]
        self.pos = base + deslocamento
        return self.pos

    def readinto(self, buffer):
        n = min(len(buffer), self.tamanho - self.pos)
        if n <= 0:
            return 0
        r = _requisitar("GET", self.url, {"Range": f"bytes={self.pos}-{self.pos + n - 1}"}, permitir=(206,))
        dados = r.content
        buffer[:len(dados)] = dados
        self.pos += len(dados)
        return len(dados)


def nome_membro(info):
    """Nome do membro do zip. Zips gravados no Windows sem a marca UTF-8 usam a página de código 850
    (o Python lê como 437): 'PadrΣes' volta a ser 'Padrões'."""
    if info.flag_bits & 0x800:
        return info.filename
    try:
        return info.filename.encode("cp437").decode("cp850")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return info.filename


class ZipRemoto:
    """Zip publicado numa URL, lido sem baixar o arquivo inteiro quando o servidor aceita Range.

    Se o servidor não aceitar Range, baixa o arquivo para um diretório temporário e lê de lá.
    Use como gerenciador de contexto: `with ZipRemoto(url) as z: ...`.
    """

    def __init__(self, url):
        meta = metadados_remotos(url)
        self.url, self.metadados, self._tmp = meta["url_final"], meta, None
        if meta["aceita_range"] and meta["tamanho"]:
            bruto = _ArquivoHttp(self.url, meta["tamanho"])
            self.zip = zipfile.ZipFile(io.BufferedReader(bruto, buffer_size=PARTE_BYTES))
        else:
            self._tmp = tempfile.TemporaryDirectory()
            local = os.path.join(self._tmp.name, "arquivo.zip")
            print(f"O servidor não aceita Range: baixando o zip inteiro de {url}", flush=True)
            baixar(self.url, local)
            self.zip = zipfile.ZipFile(local)

    def membros(self):
        """[(nome, info)] de todos os membros, com o nome corrigido por `nome_membro`."""
        return [(nome_membro(i), i) for i in self.zip.infolist() if not i.is_dir()]

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.zip.close()
        if self._tmp:
            self._tmp.cleanup()


def extrair_do_zip(url, padrao, destino_zip):
    """Grava em `destino_zip` (local) os membros do zip remoto cujo nome casa com a expressão regular `padrao`.

    O conteúdo de cada membro é copiado sem alteração; o nome é o corrigido por `nome_membro`.
    Devolve {'membros': [(nome, bytes)], 'metadados': metadados_remotos, 'sha256': do zip gravado}.
    """
    regex = re.compile(padrao)
    with ZipRemoto(url) as remoto:
        escolhidos = [(n, i) for n, i in remoto.membros() if regex.search(n)]
        if not escolhidos:
            raise RuntimeError(f"Nenhum membro de {url} casa com {padrao!r}")
        with zipfile.ZipFile(destino_zip, "w", compression=zipfile.ZIP_DEFLATED) as saida:
            for nome, info in escolhidos:
                novo = zipfile.ZipInfo(nome, date_time=info.date_time)
                novo.compress_type = zipfile.ZIP_DEFLATED
                with remoto.zip.open(info) as origem, saida.open(novo, "w") as alvo:
                    shutil.copyfileobj(origem, alvo, PARTE_BYTES)
                print(f"   extraído: {nome} ({info.file_size:,} bytes)", flush=True)
        metadados = remoto.metadados
    return {
        "membros": [(n, i.file_size) for n, i in escolhidos],
        "metadados": metadados,
        "sha256": sha256_arquivo(destino_zip),
    }


def copiar_para_volume(local, destino):
    """Copia um arquivo local para o volume, criando a pasta de destino."""
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    shutil.copyfile(local, destino)


def manifestos(volume_path):
    """Manifestos (`*.manifest.json`) das coletas no volume, da mais antiga para a mais recente.

    Cada manifesto ganha a chave `data_path` (caminho do arquivo de dados ao lado dele). Manifestos sem o
    arquivo de dados são ignorados com aviso.
    """
    saida = []
    for caminho in glob.glob(f"{volume_path}/**/*.manifest.json", recursive=True):
        with open(caminho, encoding="utf-8") as fh:
            manifesto = json.load(fh)
        dados = os.path.join(os.path.dirname(caminho), manifesto["file_name"])
        if not os.path.exists(dados):
            print(f"Aviso: manifesto sem arquivo de dados, ignorado: {caminho}")
            continue
        manifesto["data_path"] = dados
        saida.append(manifesto)
    return sorted(saida, key=lambda m: m["collected_at_utc"])


def ultimo_manifesto(volume_path):
    """Manifesto da coleta mais recente no volume, ou None."""
    todos = manifestos(volume_path)
    return todos[-1] if todos else None
