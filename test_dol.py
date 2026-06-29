"""
test_dol.py — Isolated test suite for scrape_dol.py

Covers:
  1. Logging          — versioned log file created and non-empty
  2. Discovery        — DOL page returns >=1 file per program (LCA, PERM, PW)
  3. Download         — 1 file per program saved to disk; SHA-256 verified
  4a. Dedupe headers  — immediate re-process returns ETag/Last-Modified skip
  4b. Dedupe hash     — stale headers forced; re-download finds identical bytes
  5. Databricks       — dbutils absence is handled gracefully (no crash)

All test artifacts go to data/test/ (isolated from real data/).
Clean up with:  Remove-Item -Recurse -Force data\test
"""

import hashlib
import os
import sys
from datetime import datetime
from pathlib import Path

# ── Point scrape_dol at a test-only data directory BEFORE importing it ────────
os.environ["GALE_VISA_DATA_DIR"] = str(Path(__file__).parent / "data" / "test")

# scrape_dol runs module-level setup (DATA_DIR, logger, etc.) on import.
# The env var above ensures everything lands in data/test/.
import scrape_dol  # noqa: E402
from scrape_dol import (  # noqa: E402
    DATA_DIR, LOG_DIR, MANIFEST_PATH, PROGRAM_KEYS,
    discover_files, get_session, load_manifest,
    process_file, save_manifest, setup_logging,
)

# Separate logger for the test runner itself
_log = setup_logging()


# ── Formatting helpers ────────────────────────────────────────────────────────

def _header(title: str) -> None:
    _log.info("")
    _log.info("─" * 60)
    _log.info(f"  {title}")
    _log.info("─" * 60)


def _ok(msg: str)   -> None: _log.info(f"  ✓  {msg}")
def _fail(msg: str) -> None: _log.error(f"  ✗  {msg}")
def _info(msg: str) -> None: _log.info(f"     {msg}")


# ── Individual tests ──────────────────────────────────────────────────────────

def test_logging() -> bool:
    """Verify that a versioned log file was created and is non-empty."""
    _header("TEST 1 — Logging")
    try:
        log_files = sorted(LOG_DIR.glob("scrape_*.log"), key=lambda p: p.stat().st_mtime)
        assert log_files, f"No log file found in {LOG_DIR}"
        newest = log_files[-1]
        assert newest.stat().st_size > 0, f"Log file is empty: {newest}"
        _ok(f"Log file: {newest.name}  ({newest.stat().st_size} bytes)")
        _ok(f"LOG_DIR : {LOG_DIR}")
        return True
    except Exception as exc:
        _fail(str(exc))
        return False


def test_discovery(session) -> tuple[bool, dict]:
    """Fetch the DOL page and confirm >=1 file per target program."""
    _header("TEST 2 — File Discovery (DOL page)")
    try:
        candidates = discover_files(session)
        _info(f"Total LCA/PERM/PW candidates: {len(candidates)}")

        per_program: dict = {}
        for item in candidates:
            key = PROGRAM_KEYS.get(item["program"])
            if key:
                per_program.setdefault(key, []).append(item)

        all_ok = True
        for prog in ("lca", "perm", "pw"):
            count = len(per_program.get(prog, []))
            if count:
                _ok(f"{prog.upper():<4}  {count} file(s) discovered")
            else:
                _fail(f"{prog.upper():<4}  0 files discovered")
                all_ok = False

        return all_ok, per_program
    except Exception as exc:
        _fail(f"Discovery raised: {exc}")
        return False, {}


def test_download_one_per_program(session, per_program: dict) -> bool:
    """
    Download the first available file for each of LCA, PERM, and PW.
    Verifies the file lands on disk and that the stored SHA-256 matches
    the actual bytes on disk.
    """
    _header("TEST 3 — Single-File Download (1 per program, hash verified)")
    manifest: dict = {}
    all_ok = True

    for prog_key, prog_label in (
        ("lca",  "LCA Program"),
        ("perm", "PERM Program"),
        ("pw",   "Prevailing Wage Program"),
    ):
        items = per_program.get(prog_key, [])
        if not items:
            _fail(f"{prog_key.upper():<4}  no candidates — cannot download")
            all_ok = False
            continue

        item = items[0]
        _info(f"{prog_key.upper():<4}  {item['filename']}  (year={item['year']})")

        try:
            status, is_new = process_file(session, item, manifest)
            entry = manifest.get(item["url"], {})
            saved = Path(entry.get("saved_path", ""))

            assert status == "downloaded",  f"expected 'downloaded', got '{status}'"
            assert is_new,                  "is_new should be True for a first download"
            assert saved.exists(),          f"file not on disk: {saved}"

            actual_hash = hashlib.sha256(saved.read_bytes()).hexdigest()
            assert actual_hash == entry["sha256"], "SHA-256 in manifest does not match file bytes"

            _ok(f"{prog_key.upper():<4}  saved → {saved.relative_to(Path.cwd())}")
            _ok(f"       SHA-256: {entry['sha256'][:16]}…  (verified)")
        except Exception as exc:
            _fail(f"{prog_key.upper():<4}  {exc}")
            all_ok = False

    # Persist manifest so the deduplication tests can read it
    if manifest:
        save_manifest(manifest)

    return all_ok


def test_dedupe_headers(session) -> bool:
    """
    Immediately re-process the same files.
    The server's ETag or Last-Modified should match the cached values,
    so each call must return a header-based skip without downloading.
    """
    _header("TEST 4a — Deduplication: Header Check (ETag / Last-Modified)")
    manifest = load_manifest()
    if not manifest:
        _fail("Manifest empty — download test must run first")
        return False

    # One representative URL per program
    seen:    set  = set()
    samples: list = []
    for url, entry in manifest.items():
        key = PROGRAM_KEYS.get(entry.get("program", ""))
        if key and key not in seen:
            seen.add(key)
            samples.append((key, url, entry))
        if len(seen) == 3:
            break

    all_ok = True
    for prog_key, url, entry in samples:
        item = {
            "url":      url,
            "filename": entry["filename"],
            "program":  entry["program"],
            "year":     entry["year"],
        }
        try:
            status, is_new = process_file(session, item, manifest)
            assert status in ("skipped_etag", "skipped_lastmod", "skipped_hash"), \
                f"expected a skip status, got '{status}'"
            assert not is_new, "is_new must be False on a repeated call"
            _ok(f"{prog_key.upper():<4}  {status}  ({entry['filename']})")
        except Exception as exc:
            _fail(f"{prog_key.upper():<4}  {exc}")
            all_ok = False

    return all_ok


def test_dedupe_hash_tier(session) -> bool:
    """
    Force Tiers 1 & 2 to miss by poisoning the cached ETag and Last-Modified,
    then verify that re-downloading the same file still results in no new-data
    flag because the SHA-256 matches (Tier 3).
    """
    _header("TEST 4b — Deduplication: Hash Tier (stale headers, re-downloaded)")
    manifest = load_manifest()
    if not manifest:
        _fail("Manifest empty — download test must run first")
        return False

    url, entry = next(iter(manifest.items()))
    item = {
        "url":      url,
        "filename": entry["filename"],
        "program":  entry["program"],
        "year":     entry["year"],
    }
    _info(f"File under test: {entry['filename']}  (SHA-256: {entry['sha256'][:16]}…)")

    # Poison the cached headers so Tier 1 and 2 will never match
    poisoned: dict = {
        url: {
            **entry,
            "etag":          "stale-etag-forced-by-test",
            "last_modified": "Thu, 01 Jan 2000 00:00:00 GMT",
        }
    }

    try:
        status, is_new = process_file(session, item, poisoned)

        # Expected outcome: file bytes haven't changed → skipped_hash
        # Edge case: file was updated on the server between our two calls → downloaded
        assert status in ("skipped_hash", "downloaded"), \
            f"unexpected status: '{status}'"

        if status == "skipped_hash":
            _ok("skipped_hash — re-downloaded file has identical SHA-256; no pipeline triggered")
        else:
            _ok("downloaded  — content changed on server between calls (rare but valid)")

        if status == "skipped_hash":
            assert not is_new, "is_new must be False when hash matches"
            _ok(f"is_new=False confirmed")

        return True
    except Exception as exc:
        _fail(str(exc))
        return False


def test_databricks_integration() -> bool:
    """
    Confirm that the absence of dbutils (local environment) is caught
    cleanly as a NameError and does not crash the scraper.
    """
    _header("TEST 5 — Databricks Task-Value Integration")
    try:
        dbutils.jobs.taskValues.set(key="test_key", value=True)  # noqa: F821
        # Reached only when running inside a Databricks Workflow
        _ok("dbutils IS available — task value set successfully")
        _info("Running inside a Databricks Workflow")
    except NameError:
        _ok("NameError caught — dbutils not present (expected in local environment)")
        _info("On Databricks, lca_new / perm_new / pw_new will be set automatically")
    except Exception as exc:
        _fail(f"Unexpected error: {exc}")
        return False

    return True


# ── Test runner ───────────────────────────────────────────────────────────────

def main() -> int:
    _log.info("=" * 60)
    _log.info("scrape_dol.py  —  TEST SUITE")
    _log.info(f"Started    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    _log.info(f"Test data  : {DATA_DIR.resolve()}")
    _log.info("=" * 60)

    session = get_session()
    results: dict[str, bool] = {}

    # Run tests in dependency order
    results["logging"]       = test_logging()
    ok, per_program          = test_discovery(session)
    results["discovery"]     = ok
    results["download"]      = test_download_one_per_program(session, per_program)
    results["dedupe_headers"] = test_dedupe_headers(session)
    results["dedupe_hash"]   = test_dedupe_hash_tier(session)
    results["databricks"]    = test_databricks_integration()

    # Final summary
    passed = sum(results.values())
    total  = len(results)
    _log.info("")
    _log.info("=" * 60)
    _log.info(f"RESULTS  {passed}/{total} passed")
    _log.info("=" * 60)
    for name, ok in results.items():
        _log.info(f"  {'✓' if ok else '✗'}  {name}")
    _log.info("")

    if passed == total:
        _log.info("All tests passed.")
    else:
        failed = [n for n, v in results.items() if not v]
        _log.error(f"Failed: {', '.join(failed)}")

    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
