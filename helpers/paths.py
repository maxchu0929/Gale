# paths.py — Databricks DBFS path configuration for the gale_visa pipeline
import os
import re

# DBFS root for all gale_visa data.
# Override with GALE_VISA_DBFS_ROOT env var to use a custom mount point.
DBFS_ROOT = os.environ.get("GALE_VISA_DBFS_ROOT", "dbfs:/FileStore/gale_visa")

# Top-level data directories (DBFS URIs)
PERFORMANCE_DATA_DIR = f"{DBFS_ROOT}/performance_data"  # DOL PERM/LCA/PW/H-2A/H-2B files
VISA_STATS_DIR       = f"{DBFS_ROOT}/visa_stats"         # State Dept monthly/annual visa files
USCIS_DIR            = f"{DBFS_ROOT}/uscis_data"         # USCIS H-1B/H-2A/H-2B employer files

# Delta table location (2-level namespace for Community Edition; override for Unity Catalog)
CATALOG        = os.environ.get("GALE_VISA_CATALOG", "hive_metastore")
SCHEMA         = os.environ.get("GALE_VISA_SCHEMA",  "gale_visa")
MANIFEST_TABLE = f"{CATALOG}.{SCHEMA}.file_manifest"


def dbfs_to_local(path: str) -> str:
    """
    Convert a dbfs:/ URI to the equivalent /dbfs/ local path readable on cluster nodes.
    e.g. "dbfs:/FileStore/gale_visa/foo.xlsx" -> "/dbfs/FileStore/gale_visa/foo.xlsx"
    """
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path[len("dbfs:/"):]
    return path


def _sanitize(name: str) -> str:
    """Strip filesystem-unsafe characters and cap length."""
    return re.sub(r'[\\/*?:"<>|]', "_", name.strip())[:80]


# ============================================================================
# VISA STATISTICS PATHS
# ============================================================================

def get_monthly_outdir(program: str, period: str) -> str:
    """
    DBFS path for monthly visa stats files.
    e.g. get_monthly_outdir("IV", "FY2024-10") -> "dbfs:/FileStore/gale_visa/visa_stats/monthly/IV/FY2024/FY2024-10"
    """
    fy_match = re.match(r"(FY\d{4})", period)
    if fy_match:
        fiscal_year = fy_match.group(1)
        return f"{VISA_STATS_DIR}/monthly/{program}/{fiscal_year}/{period}"
    return f"{VISA_STATS_DIR}/monthly/{program}/{period}"


def get_annual_outdir(year: str) -> str:
    """
    DBFS path for annual visa stats files.
    e.g. get_annual_outdir("2024") -> "dbfs:/FileStore/gale_visa/visa_stats/annual/2024"
    """
    return f"{VISA_STATS_DIR}/annual/{year}"


# ============================================================================
# DOL PERFORMANCE DATA PATHS
# ============================================================================

def get_dol_outdir(program: str, year: str) -> str:
    """
    DBFS path for DOL performance data files.
    e.g. get_dol_outdir("PERM Program", "2024") -> "dbfs:/FileStore/gale_visa/performance_data/PERM_Program/2024"
    """
    return f"{PERFORMANCE_DATA_DIR}/{_sanitize(program)}/{year}"


# ============================================================================
# USCIS EMPLOYER DATA PATHS
# ============================================================================

def get_uscis_outdir(visa_type: str, year_or_misc: str) -> str:
    """
    DBFS path for USCIS employer data files.
    e.g. get_uscis_outdir("h1b", "2024") -> "dbfs:/FileStore/gale_visa/uscis_data/h1b/2024"
    """
    return f"{USCIS_DIR}/{visa_type}/{year_or_misc}"
