"""
scrape_dol.py — Monthly DOL performance data scraper (LCA, PERM, Prevailing Wage)

Source:
  https://www.dol.gov/agencies/eta/foreign-labor/performance

Designed to run as a monthly Databricks job. Uses a three-tier deduplication
strategy so that unchanged files are never re-downloaded:

  Tier 1  ETag header match      → skip immediately (no download)
  Tier 2  Last-Modified match    → skip immediately (no download)
  Tier 3  SHA-256 content hash   → file bytes unchanged despite new headers;
                                   refresh cached headers, do NOT flag as new data

When a program has genuinely new content the scraper:
  • Saves the file to disk under data/{Program}/{year}/{filename}
  • Sets Databricks task values (lca_new / perm_new / pw_new) so downstream
    Workflow tasks can decide whether to re-run their ETL pipeline
  • Writes data/new_data.json with the same flags for local / non-Databricks use

Outputs
  data/LCA Program/{year}/{file}
  data/PERM Program/{year}/{file}
  data/Prevailing Wage Program/{year}/{file}
  data/logs/scrape_{timestamp}.log
  data/manifest.json          (SHA-256 hashes, ETags, download timestamps)
  data/new_data.json          ({"lca": bool, "perm": bool, "pw": bool, ...})

Environment variables
  GALE_VISA_DATA_DIR   Override the output root (default: ./data).
                       On Databricks set to /dbfs/FileStore/gale_visa

Exit codes
  0   Completed normally (check new_data.json for pipeline triggers)
  1   Fatal error (DOL page unreachable, unrecoverable I/O failure, etc.)
"""

import hashlib
import json
import logging
import os
import re
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup

# ── Configuration ─────────────────────────────────────────────────────────────

DOL_URL  = "https://www.dol.gov/agencies/eta/foreign-labor/performance"
DATA_DIR = Path(os.environ.get("GALE_VISA_DATA_DIR", "data"))

# Only scrape spreadsheet formats consumed by the ETL pipelines; skip PDFs/docs/zips
VALID_EXTS = (".xlsx", ".xls", ".csv")

# Keyword → canonical program folder name.
# Folder names match what lca/, perm/, prevailing_wage/ compile scripts expect.
PROGRAM_KEYWORDS: Dict[str, str] = {
    "lca":        "LCA Program",
    "h-1b":       "LCA Program",
    "h1b":        "LCA Program",
    "perm":       "PERM Program",
    "pw":         "Prevailing Wage Program",
    "prevailing": "Prevailing Wage Program",
}

# Short keys for new_data.json and Databricks task values
PROGRAM_KEYS: Dict[str, str] = {
    "LCA Program":             "lca",
    "PERM Program":            "perm",
    "Prevailing Wage Program": "pw",
}

SKIP_PATTERNS = [
    "annual performance report",
    "fy 2016 report", "fy 2015 report", "fy 2014 report", "fy 2013 report",
    "fy 2012 report", "fy 2011 report", "fy 2010 report", "fy 2009 report",
    "fy 2007 report", "fy 2006 report",
]

POLITE_DELAY    = 0.5       # seconds between requests (be a polite scraper)
REQUEST_TIMEOUT = (10, 60)  # (connect timeout, read timeout) in seconds

MANIFEST_PATH = DATA_DIR / "manifest.json"
LOG_DIR       = DATA_DIR / "logs"
NEW_DATA_PATH = DATA_DIR / "new_data.json"


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file  = LOG_DIR / f"scrape_{timestamp}.log"

    logger = logging.getLogger("scrape_dol")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(f"Log: {log_file}")
    return logger


logger = setup_logging()


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "GaleVisa-Scraper/2.0 (monthly ETL job)"})
    return s


def retrying_get(session: requests.Session, url: str, **kwargs) -> requests.Response:
    """GET with exponential backoff on transient server errors."""
    backoff = 1.0
    for attempt in range(4):
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"retryable HTTP {r.status_code}")
            r.raise_for_status()
            return r
        except Exception as exc:
            if attempt == 3:
                raise
            logger.warning(f"  GET retry {attempt + 1}/3 → {url}: {exc}")
            time.sleep(backoff)
            backoff *= 2


def retrying_head(session: requests.Session, url: str) -> Optional[requests.Response]:
    """HEAD request for cheap deduplication; returns None if server doesn't support it."""
    try:
        r = session.head(url, timeout=(10, 30), allow_redirects=True)
        if r.status_code in (405, 501):   # HEAD not supported
            return None
        r.raise_for_status()
        return r
    except Exception as exc:
        logger.debug(f"  HEAD failed for {url}: {exc}")
        return None


# ── URL / filename helpers ────────────────────────────────────────────────────

def normalize_url(url: str) -> str:
    """Canonicalize a URL so that equivalent URLs always compare equal."""
    p = urlsplit(url.strip())
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path.rstrip("/"), p.query, ""))


def extract_year(filename: str) -> str:
    """Pull a 4-digit year from a filename; falls back to 'unknown_year'."""
    m = re.search(r"fy\s*(\d{2,4})", filename, re.IGNORECASE)
    if m:
        t = m.group(1)
        return f"20{t}" if len(t) == 2 else t
    m = re.search(r"(19|20)\d{2}", filename)
    return m.group(0) if m else "unknown_year"


# ── Discovery helpers ─────────────────────────────────────────────────────────

def detect_program(filename: str, context: str = "") -> Optional[str]:
    """Return the canonical program folder name if LCA/PERM/PW; else None."""
    combined = (filename + " " + context).lower()
    for keyword, program in PROGRAM_KEYWORDS.items():
        if keyword in combined:
            return program
    return None


def should_skip(filename: str, context: str = "") -> bool:
    """True for deprecated annual-report files (but never for record-layout PDFs)."""
    combined = (filename + " " + context).lower()
    if "record layout" in combined or "record_layout" in combined:
        return False
    for pat in SKIP_PATTERNS:
        if pat in combined:
            return True
    if "annual" in combined and "report" in combined and filename.lower().endswith(".pdf"):
        return True
    return False


def _parse_table_links(soup: BeautifulSoup) -> List[Dict]:
    """Extract file links from the 'Latest Quarterly Updates' table section."""
    results: List[Dict] = []
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            program_cell = cells[0].get_text(strip=True)
            for cell in cells[1:]:
                for a in cell.find_all("a", href=True):
                    href = a["href"]
                    if not any(href.lower().endswith(ext) for ext in VALID_EXTS):
                        continue
                    filename = href.split("/")[-1]
                    if should_skip(filename, program_cell):
                        continue
                    program = detect_program(filename, program_cell)
                    if not program:
                        continue
                    results.append({
                        "program":  program,
                        "url":      normalize_url(urljoin(DOL_URL, href)),
                        "filename": filename,
                        "year":     extract_year(filename),
                    })
    return results


def discover_files(session: requests.Session) -> List[Dict]:
    """
    Fetch the DOL performance page and return all LCA/PERM/PW spreadsheet links.
    Deduplicates by URL; table links (quarterly section) take priority.
    """
    r = retrying_get(session, DOL_URL)
    soup = BeautifulSoup(r.text, "lxml")

    seen:       set       = set()
    candidates: List[Dict] = []

    def _add(item: Dict) -> None:
        if item["url"] not in seen:
            seen.add(item["url"])
            candidates.append(item)

    for item in _parse_table_links(soup):
        _add(item)

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not any(href.lower().endswith(ext) for ext in VALID_EXTS):
            continue
        filename = href.split("/")[-1]
        if should_skip(filename):
            continue

        # Resolve program: filename → parent element → nearest heading
        program = detect_program(filename)
        if not program:
            parent = a.find_parent(["td", "li", "p", "div"])
            if parent:
                program = detect_program("", parent.get_text(strip=True))
        if not program:
            for heading in a.find_all_previous(["h2", "h3", "h4", "strong", "b"]):
                program = detect_program("", heading.get_text(strip=True))
                if program:
                    break
        if not program:
            continue   # H-2A, H-2B, CW-1, or unrecognised — skip

        _add({
            "program":  program,
            "url":      normalize_url(urljoin(DOL_URL, href)),
            "filename": filename,
            "year":     extract_year(filename),
        })

    return candidates


# ── Manifest (JSON, atomic write) ─────────────────────────────────────────────

def load_manifest() -> Dict:
    if MANIFEST_PATH.exists():
        try:
            with MANIFEST_PATH.open(encoding="utf-8") as f:
                m = json.load(f)
            logger.info(f"Loaded manifest: {len(m)} entries")
            return m
        except json.JSONDecodeError as exc:
            logger.error(f"Manifest corrupted ({exc}); trying backup")
            bak = Path(str(MANIFEST_PATH) + ".bak")
            if bak.exists():
                try:
                    with bak.open(encoding="utf-8") as f:
                        m = json.load(f)
                    logger.info(f"Restored from backup: {len(m)} entries")
                    return m
                except json.JSONDecodeError:
                    logger.error("Backup also corrupted — starting fresh")
    return {}


def save_manifest(manifest: Dict) -> None:
    """Write manifest atomically: temp file → rename, keeping one backup."""
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    if MANIFEST_PATH.exists():
        shutil.copy2(MANIFEST_PATH, str(MANIFEST_PATH) + ".bak")
    fd, tmp = tempfile.mkstemp(dir=DATA_DIR, prefix=".manifest_", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, default=str)
        shutil.move(tmp, MANIFEST_PATH)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ── Download (atomic) ─────────────────────────────────────────────────────────

def download_atomic(session: requests.Session, url: str, dest: Path) -> bytes:
    """Download url to a temp file then atomically rename to dest. Returns file bytes."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=dest.parent, prefix=".dl_", suffix=dest.suffix)
    try:
        r = retrying_get(session, url, stream=True)
        with os.fdopen(fd, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 16):   # 64 KB chunks
                f.write(chunk)
        shutil.move(tmp, dest)
        return dest.read_bytes()
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ── Per-file logic ────────────────────────────────────────────────────────────

def process_file(
    session:  requests.Session,
    item:     Dict,
    manifest: Dict,
) -> Tuple[str, bool]:
    """
    Decide whether to (re)download one file and update the manifest.

    Returns (status, is_new_content):
      "skipped_etag"    ETag matched cached value
      "skipped_lastmod" Last-Modified matched cached value
      "skipped_hash"    Downloaded but SHA-256 identical to cached — headers refreshed
      "downloaded"      New or updated file written to disk
      "failed"          Download or I/O error
    """
    url      = item["url"]
    filename = item["filename"]
    program  = item["program"]
    year     = item["year"]
    dest     = DATA_DIR / program / year / filename
    cached: Optional[Dict] = manifest.get(url)

    # ── Tier 1 & 2: cheap header check (no download) ─────────────────────────
    if cached:
        head = retrying_head(session, url)
        time.sleep(POLITE_DELAY)
        if head is not None:
            srv_etag = head.headers.get("ETag")
            srv_lmod = head.headers.get("Last-Modified")
            if srv_etag and srv_etag == cached.get("etag"):
                logger.debug(f"SKIP(etag)     {program}/{year}/{filename}")
                return "skipped_etag", False
            if srv_lmod and srv_lmod == cached.get("last_modified"):
                logger.debug(f"SKIP(last-mod) {program}/{year}/{filename}")
                return "skipped_lastmod", False

    # ── Download ──────────────────────────────────────────────────────────────
    logger.info(f"↓  {program}/{year}/{filename}")
    try:
        content = download_atomic(session, url, dest)
        time.sleep(POLITE_DELAY)
    except Exception as exc:
        logger.error(f"   FAILED {url}: {exc}")
        return "failed", False

    new_hash = hashlib.sha256(content).hexdigest()

    # ── Tier 3: content hash check ────────────────────────────────────────────
    if cached and cached.get("sha256") == new_hash:
        # Bytes are identical — update cached headers for faster skips next run
        head = retrying_head(session, url)
        cached.update({
            "etag":          head.headers.get("ETag")          if head else None,
            "last_modified": head.headers.get("Last-Modified") if head else None,
            "checked_at":    datetime.now(timezone.utc).isoformat(),
        })
        manifest[url] = cached
        save_manifest(manifest)
        logger.debug(f"SKIP(hash)     {program}/{year}/{filename}  (bytes unchanged)")
        return "skipped_hash", False

    # ── Genuinely new/changed content ─────────────────────────────────────────
    head = retrying_head(session, url)
    manifest[url] = {
        "program":       program,
        "year":          year,
        "filename":      filename,
        "saved_path":    str(dest),
        "sha256":        new_hash,
        "etag":          head.headers.get("ETag")          if head else None,
        "last_modified": head.headers.get("Last-Modified") if head else None,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
    save_manifest(manifest)
    logger.info(f"   ✓ saved → {dest}")
    return "downloaded", True


# ── Main orchestration ────────────────────────────────────────────────────────

def scrape() -> Dict[str, bool]:
    """
    Run the full scrape and return {"lca": bool, "perm": bool, "pw": bool}.
    True means at least one genuinely new file was found for that program.
    """
    logger.info("=" * 60)
    logger.info(f"DOL scrape started   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Data directory       {DATA_DIR.resolve()}")
    logger.info("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    session  = get_session()
    manifest = load_manifest()

    logger.info(f"\nFetching {DOL_URL} ...")
    candidates = discover_files(session)
    logger.info(f"Discovered {len(candidates)} LCA / PERM / PW file(s)\n")

    new_data: Dict[str, bool] = {"lca": False, "perm": False, "pw": False}
    counts = {k: 0 for k in ("downloaded", "skipped_etag", "skipped_lastmod", "skipped_hash", "failed")}

    for item in candidates:
        status, is_new = process_file(session, item, manifest)
        counts[status] += 1
        if is_new:
            key = PROGRAM_KEYS.get(item["program"])
            if key:
                new_data[key] = True

    # ── Summary log ───────────────────────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info("Scrape complete")
    logger.info(f"  Downloaded       : {counts['downloaded']}")
    logger.info(f"  Skipped (ETag)   : {counts['skipped_etag']}")
    logger.info(f"  Skipped (LMod)   : {counts['skipped_lastmod']}")
    logger.info(f"  Skipped (hash)   : {counts['skipped_hash']}")
    logger.info(f"  Failed           : {counts['failed']}")
    logger.info(f"\nNew data flagged:")
    logger.info(f"  LCA  : {new_data['lca']}")
    logger.info(f"  PERM : {new_data['perm']}")
    logger.info(f"  PW   : {new_data['pw']}")
    logger.info("=" * 60)

    # ── Write new_data.json ───────────────────────────────────────────────────
    payload = {
        **new_data,
        "scrape_time":      datetime.now(timezone.utc).isoformat(),
        "files_downloaded": counts["downloaded"],
        "files_failed":     counts["failed"],
    }
    with NEW_DATA_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    logger.info(f"\nWrote {NEW_DATA_PATH}")

    # ── Notify Databricks downstream tasks (no-op outside Workflows) ──────────
    try:
        for key, val in new_data.items():
            dbutils.jobs.taskValues.set(key=f"{key}_new", value=val)  # noqa: F821
        logger.info("Set Databricks task values: lca_new, perm_new, pw_new")
    except NameError:
        pass   # dbutils not available — running locally

    return new_data


def main() -> int:
    try:
        scrape()
        return 0
    except Exception as exc:
        logger.exception(f"Fatal: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


# Configure logging to stdout for Railway
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Configs
ROOT = "https://www.dol.gov/agencies/eta/foreign-labor/performance"
FILE_EXTS = (".pdf", ".xlsx", ".xls", ".csv", ".docx", ".doc", ".zip")

MODE = "safe"
POLITE_DELAY = 0.5

# Skip deprecated annual reports
SKIP_PATTERNS = [
    "annual performance report",
    "fy 2016 report", "fy 2015 report", "fy 2014 report", "fy 2013 report",
    "fy 2012 report", "fy 2011 report", "fy 2010 report", "fy 2009 report",
    "fy 2007 report", "fy 2006 report",
]

# Program detection mapping
PROGRAM_MAP = {
    "perm": "PERM Program",
    "lca": "LCA Program",
    "h-1b": "LCA Program",
    "h1b": "LCA Program",
    "pw": "Prevailing Wage Program",
    "prevailing": "Prevailing Wage Program",
    "h-2a": "H-2A Program",
    "h2a": "H-2A Program",
    "h-2b": "H-2B Program",
    "h2b": "H-2B Program",
    "cw-1": "CW-1 Program",
    "cw1": "CW-1 Program",
}

# HTTP helpers
def get_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "DataResScraper/1.0"})
    return s

def retrying_get(session: requests.Session, url: str, *, timeout=(10, 30), stream=False):
    backoff = 1.0
    for i in range(4):
        try:
            r = session.get(url, timeout=timeout, stream=stream)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"retryable {r.status_code}")
            r.raise_for_status()
            return r
        except Exception as e:
            if i == 3:
                logger.error(f"GET failed {url}: {e}")
                raise
            logger.warning(f"GET retry {i+1} for {url}: {e}")
            time.sleep(backoff)
            backoff *= 2

# Utilities
def normalize_url(url: str) -> str:
    """Normalize URL so duplicates always match."""
    parsed = urlsplit(url.strip())
    return urlunsplit((
        parsed.scheme.lower(),
        parsed.netloc.lower(),
        parsed.path.rstrip('/'),
        parsed.query,
        parsed.fragment,
    ))

def clean_program_name(name: str) -> str:
    """Sanitize folder names."""
    name = re.sub(r'[\\/*?:"<>|]', "_", name.strip())
    if len(name) > 80:
        name = name[:80]
    return name

def extract_year(filename: str) -> str:
    """Extract fiscal or calendar year from filename."""
    match = re.search(r"fy\s*(\d{2,4})", filename, re.IGNORECASE)
    if match:
        token = match.group(1)
        return f"20{token}" if len(token) == 2 else token
    match = re.search(r"(19|20)\d{2}", filename)
    return match.group(0) if match else "unknown_year"

def detect_program_from_filename(filename: str) -> str:
    """Detect program from filename as fallback."""
    filename_lower = filename.lower()
    for key, val in PROGRAM_MAP.items():
        if key in filename_lower:
            return val
    return None

def should_skip_file(filename: str, text_context: str = "") -> bool:
    """Check if file should be skipped (deprecated annual reports)."""
    combined = (filename + " " + text_context).lower()
    
    # Skip if matches deprecated patterns (but not "record layout" PDFs)
    if "record layout" not in combined and "record_layout" not in combined:
        for pattern in SKIP_PATTERNS:
            if pattern in combined:
                return True
        
        # Skip PDF annual reports specifically (but not layouts)
        if "annual" in combined and "report" in combined and filename.lower().endswith(".pdf"):
            return True
    
    return False

def parse_table_links(soup: BeautifulSoup) -> list:
    """Parse download links from table format (Latest Quarterly Updates)."""
    table_links = []
    
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            
            # First cell typically has program name
            program_cell = cells[0].get_text(strip=True)
            
            # Remaining cells have file links
            for cell in cells[1:]:
                for link in cell.find_all("a", href=True):
                    href = link["href"]
                    if not any(href.lower().endswith(ext) for ext in FILE_EXTS):
                        continue
                    
                    filename = href.split("/")[-1]
                    
                    # Skip deprecated files
                    if should_skip_file(filename, program_cell):
                        logger.info(f"Skipping deprecated: {filename}")
                        continue
                    
                    full_url = urljoin(ROOT, href)
                    year = extract_year(filename)
                    
                    # Detect program from table cell or filename
                    current_program = None
                    for key, val in PROGRAM_MAP.items():
                        if key in program_cell.lower() or key in filename.lower():
                            current_program = val
                            break
                    
                    if not current_program:
                        current_program = detect_program_from_filename(filename)
                    
                    if not current_program:
                        current_program = "Uncategorized"
                    
                    table_links.append({
                        "program": current_program,
                        "url": normalize_url(full_url),
                        "filename": filename,
                        "year": year
                    })
    
    return table_links

def discover_files(session: requests.Session) -> list:
    """
    Returns list of dicts: {"program": str, "year": str, "url": str, "filename": str}
    """
    r = retrying_get(session, ROOT)
    soup = BeautifulSoup(r.text, "html.parser")
    
    download_links = []
    
    # Parse table-based links
    logger.info("Parsing table-based links...")
    table_links = parse_table_links(soup)
    logger.info(f"Found {len(table_links)} file(s) from tables")
    download_links.extend(table_links)
    
    # Parse all links on page
    logger.info("Scanning all links on page...")
    all_links = soup.find_all("a", href=True)
    
    for link in all_links:
        href = link["href"]
        href_lower = href.lower()
        
        if not any(href_lower.endswith(ext) for ext in FILE_EXTS):
            continue
        
        filename = href.split("/")[-1]
        
        if should_skip_file(filename):
            continue
        
        program = detect_program_from_filename(filename)
        
        # Try parent context
        if not program:
            parent = link.find_parent(["td", "p", "li", "div"])
            if parent:
                context = parent.get_text(strip=True)
                for key, val in PROGRAM_MAP.items():
                    if key in context.lower():
                        program = val
                        break
        
        # Try preceding headings
        if not program:
            for prev in link.find_all_previous(["h2", "h3", "h4", "strong", "b"]):
                text = prev.get_text(strip=True).lower()
                for key, val in PROGRAM_MAP.items():
                    if key in text and "annual" not in text:
                        program = val
                        break
                if program:
                    break
        
        if not program:
            program = "Uncategorized"
        
        full_url = urljoin(ROOT, href)
        normalized_url = normalize_url(full_url)
        year = extract_year(filename)
        
        # Deduplicate by URL
        if any(item["url"] == normalized_url for item in download_links):
            continue
        
        download_links.append({
            "program": program,
            "url": normalized_url,
            "filename": filename,
            "year": year
        })
    
    logger.info(f"Found {len(download_links)} total file(s) discovered")
    return download_links

# Main
def main():
    session = get_session()
    
    counts = {"downloaded": 0, "versioned": 0, "skipped": 0, "unchanged": 0, "errors": 0}

    try:
        candidates = discover_files(session)
    except Exception as e:
        logger.error(f"[error] discovery failed ({e})")
        return

    # Group by program for separate manifests
    programs = {}
    for item in candidates:
        prog = item["program"]
        if prog not in programs:
            programs[prog] = []
        programs[prog].append(item)
    
    # Process each program
    for program, items in programs.items():
        logger.info(f"Processing {program}: {len(items)} file(s)")
        
        # Create manifest for this program
        manifest = DBManifest(source_id="dolstats", file_type="dol", mode=MODE, program=program)
        
        for item in items:
            year = item["year"]
            file_url = item["url"]
            filename = item["filename"]
            
            # Period format: "PERM Program/2024"
            period = f"{program}/{year}"
            
            try:
                decision = manifest.plan(period, file_url)

                # Decision: skip
                if decision["decision"] == "skip":
                    counts["skipped"] += 1
                    logger.info(f"[skipped] {period} {file_url} ({decision['reason']})")
                    continue

                # Check if file already exists
                pdir = get_dol_outdir(program, year)
                expected_path = pdir / filename
                
                if expected_path.exists():
                    existing = manifest.get_existing(period, file_url)
                    if not existing:
                        # File exists but not in manifest - register it
                        if manifest.register_existing_file(period, file_url, str(expected_path)):
                            counts["downloaded"] += 1
                            logger.info(f"[registered] {period} {file_url} -> {expected_path}")
                        continue

                versioned = (decision["decision"] == "version")

                saved = manifest.download_and_record(
                    session, file_url, outdir=str(pdir), period=period, versioned=versioned
                )

                if saved:
                    if versioned:
                        counts["versioned"] += 1
                        logger.info(f"[new-version] {period} {file_url} -> {saved}")
                    else:
                        counts["downloaded"] += 1
                        logger.info(f"[downloaded] {period} {file_url} -> {saved}")
                else:
                    counts["unchanged"] += 1
                    logger.info(f"[unchanged] {period} {file_url} (no write)")

            except Exception as e:
                counts["errors"] += 1
                logger.error(f"[error] {period} {file_url} ({e})")

            time.sleep(POLITE_DELAY)

    logger.info(
        "DOL summary: "
        f"downloaded={counts['downloaded']}, new_versions={counts['versioned']}, "
        f"skipped={counts['skipped']}, unchanged={counts['unchanged']}, errors={counts['errors']}"
    )

if __name__ == "__main__":
    main()
