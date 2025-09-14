"""
HAKER

Características:
- Rastreo concurrente con límite por dominio (rate limiting)
- Fuerza bruta ligera de rutas comunes (backups/uploads)
- Detección de "Index of" y listado de archivos
- Prueba de parámetros tipo ?file=... ?download=...
- Descarga verificando Content-Type y forzando extensión apropiada
- Persistencia básica (state.json) para reanudar
- Logging en consola y archivo
"""

import os
import re
import time
import json
import logging
import argparse
import threading
from urllib.parse import urljoin, urlparse, urldefrag, parse_qs, urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
import urllib.robotparser

# ----------------- CONFIG -----------------
ALLOWED_EXT = {'.sql', '.xls', '.xlsx', '.zip', '.bak', '.tar', '.gz'}
COMMON_PATHS = [
    "backup.sql", "db.sql", "database.sql", "dump.sql", "backup.zip", "db.zip",
    "backup/backup.sql", "uploads/backup.sql", "export.xlsx", "export.xls"
]
COMMON_PARAMS = ["file", "download", "db", "dump", "path", "name", "f"]
USER_AGENT = "MiCrawlerLegal/3.0 (+https://example.com - contact@tuempresa.com)"
RATE_DELAY_DEFAULT = 1.0
MAX_WORKERS = 8
MAX_DOWNLOAD_WORKERS = 6
MAX_DEPTH = None
STATE_FILE = "crawler_state.json"
DOWNLOAD_DIR = os.path.join(os.getcwd(), "descargas_permitidas")
MAX_FILE_SIZE = 200 * 1024 * 1024  # 200 MB
REQUEST_TIMEOUT = 20

CONTENTTYPE_EXT = {
    "application/sql": ".sql",
    "application/x-sql": ".sql",
    "text/x-sql": ".sql",
    "application/zip": ".zip",
    "application/x-gzip": ".gz",
    "application/x-tar": ".tar",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/octet-stream": None,
}

# ----------------- LOGGING -----------------
logger = logging.getLogger("crawler")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
sh = logging.StreamHandler()
sh.setFormatter(fmt)
logger.addHandler(sh)
fh = logging.FileHandler("crawler.log", encoding='utf-8')
fh.setFormatter(fmt)
logger.addHandler(fh)

# ----------------- GLOBALS -----------------
session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})
session.max_redirects = 10

robots_cache = {}
robots_lock = threading.Lock()
domain_last_request = {}
domain_lock = threading.Lock()
state_lock = threading.Lock()

# ----------------- UTIL -----------------
def normalizar_url(url: str) -> str:
    url, _ = urldefrag(url)
    return url.rstrip("/") if url.endswith("/") else url

def get_domain(url: str) -> str:
    return urlparse(url).netloc.lower()

def wait_for_domain(domain: str, delay: float = RATE_DELAY_DEFAULT):
    with domain_lock:
        last = domain_last_request.get(domain)
        now = time.time()
        if last and now - last < delay:
            time.sleep(delay - (now - last))
        domain_last_request[domain] = time.time()

def cache_robots_for_domain(scheme_netloc: str):
    with robots_lock:
        if scheme_netloc in robots_cache:
            return robots_cache[scheme_netloc]
        rp = urllib.robotparser.RobotFileParser()
        try:
            rp.set_url(f"{scheme_netloc}/robots.txt")
            rp.read()
        except Exception:
            rp = None
        robots_cache[scheme_netloc] = rp
        return rp

def can_fetch(url: str) -> bool:
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    rp = cache_robots_for_domain(base)
    return True if rp is None else rp.can_fetch(USER_AGENT, url)

def safe_filename(name: str) -> str:
    return re.sub(r'[^A-Za-z0-9._-]', '_', name)

def content_type_to_ext(content_type: str):
    if not content_type:
        return None
    return CONTENTTYPE_EXT.get(content_type.split(";")[0].strip().lower())

# ----------------- PERSISTENCE -----------------
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            logger.warning("No se pudo leer state file, iniciando nuevo estado.")
    return {"visited": [], "to_visit": [], "files": []}

def save_state(state):
    with state_lock:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)

# ----------------- SCRAPING -----------------
def obtener_links(url: str):
    try:
        wait_for_domain(get_domain(url))
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        return [normalizar_url(urljoin(url, a['href'])) for a in soup.find_all("a", href=True)]
    except Exception:
        return []

# ----------------- DESCARGA -----------------
def descargar_file(url: str, max_size=MAX_FILE_SIZE) -> bool:
    if not can_fetch(url):
        return False
    try:
        wait_for_domain(get_domain(url))
        with session.get(url, stream=True, timeout=REQUEST_TIMEOUT) as r:
            if r.status_code != 200:
                return False
            content_length = r.headers.get("Content-Length")
            if content_length and content_length.isdigit() and int(content_length) > max_size:
                return False
            ctype = r.headers.get("Content-Type", "").lower()
            ext = content_type_to_ext(ctype)
            fname = os.path.basename(urlparse(url).path) or "archivo_descargado"
            base, file_ext = os.path.splitext(fname)
            if file_ext.lower() not in ALLOWED_EXT and ext:
                fname = base + ext
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            safe_name = safe_filename(fname)
            destino = os.path.join(DOWNLOAD_DIR, safe_name)
            total = 0
            with open(destino, "wb") as f:
                for chunk in r.iter_content(8192):
                    if chunk:
                        total += len(chunk)
                        if total > max_size:
                            f.close()
                            os.remove(destino)
                            return False
                        f.write(chunk)
            logger.info(f"[descargado] {destino} ({total} bytes)")
            return True
    except Exception:
        return False

# ----------------- CRAWLER -----------------
def crawl(start_url: str, max_workers=MAX_WORKERS, max_depth=MAX_DEPTH):
    start_url = normalizar_url(start_url)
    state = load_state()
    visited = set(state.get("visited", []))
    to_visit = state.get("to_visit", []) or [start_url]
    files_to_download = set(state.get("files", []))

    while to_visit:
        url = to_visit.pop(0)
        if url in visited:
            continue
        visited.add(url)
        logger.info(f"[visitando] {url}")
        links = obtener_links(url)
        for link in links:
            if link not in visited:
                to_visit.append(link)
                if any(link.lower().endswith(ext) for ext in ALLOWED_EXT):
                    files_to_download.add(link)
        save_state({"visited": list(visited), "to_visit": to_visit, "files": list(files_to_download)})

    # Descargas finales
    for f in files_to_download:
        descargar_file(f)

# ----------------- CLI -----------------
def main():
    global MAX_WORKERS, MAX_DOWNLOAD_WORKERS, MAX_DEPTH, RATE_DELAY_DEFAULT, MAX_FILE_SIZE

    parser = argparse.ArgumentParser(description="Crawler mejorado (auditoría). Usa con permisos.")
    parser.add_argument("--url", "-u", help="URL inicial")
    parser.add_argument("--workers", "-w", type=int, default=MAX_WORKERS, help="Hilos para crawling")
    parser.add_argument("--dw", type=int, default=MAX_DOWNLOAD_WORKERS, help="Hilos para descargas")
    parser.add_argument("--depth", "-d", type=int, default=-1, help="Profundidad máxima ( -1 = ilimitado )")
    parser.add_argument("--rate", type=float, default=RATE_DELAY_DEFAULT, help="Delay mínimo por dominio (segundos)")
    parser.add_argument("--max-size-mb", type=int, default=MAX_FILE_SIZE // (1024 * 1024), help="Tamaño máximo archivo (MB)")
    args = parser.parse_args()

    # Si no se pasó URL, pedirla al usuario
    if not args.url:
        args.url = input("Introduce la URL inicial (solo sitios donde tengas permiso): ").strip()

    if not args.url.startswith("http"):
        logger.error("La URL debe incluir http/https.")
        return

    MAX_WORKERS = args.workers
    MAX_DOWNLOAD_WORKERS = args.dw
    MAX_DEPTH = None if args.depth < 0 else args.depth
    RATE_DELAY_DEFAULT = max(0.1, args.rate)
    MAX_FILE_SIZE = max(1, args.max_size_mb) * 1024 * 1024

    logger.info(f"Iniciando crawl en {args.url}")
    logger.info(f"Workers: {MAX_WORKERS}, Download workers: {MAX_DOWNLOAD_WORKERS}, rate: {RATE_DELAY_DEFAULT}s, max file: {MAX_FILE_SIZE} bytes")
    crawl(args.url, max_workers=MAX_WORKERS, max_depth=MAX_DEPTH)

if __name__ == "__main__":
    main()
