# manifest.py — Delta Lake manifest for tracking ETL file downloads
"""
Replaces the PostgreSQL DBManifest with a Delta Lake implementation
that runs natively on Databricks (Community Edition and above).

For local development, install databricks-connect (version must match
your cluster's Databricks Runtime) so that PySpark calls proxy to the
remote cluster:
    pip install databricks-connect==<runtime-version>.*

Usage:
    from helpers.manifest import DeltaManifest

    manifest = DeltaManifest(source_id="dolstats", file_type="dol", program="PERM Program")
    decision = manifest.plan(period="PERM Program/2024", url="https://...")
    if decision["decision"] == "download":
        content = ...  # bytes
        manifest.record(period=..., url=..., filename=..., saved_path=..., content=content)
"""

import hashlib
from datetime import datetime, timezone
from typing import Optional, Dict

from helpers.paths import MANIFEST_TABLE


def _get_spark():
    """Return the active SparkSession, or create one if running locally."""
    from pyspark.sql import SparkSession
    return SparkSession.getActiveSession() or SparkSession.builder.appName("gale_visa").getOrCreate()


class DeltaManifest:
    """
    Delta Lake manifest for tracking downloaded files across all data sources.

    Modes:
        fast — skip if (period, url) already exists in the table (no DBFS check)
        safe — also verify the file is physically present on DBFS before skipping
    """

    def __init__(
        self,
        source_id: str,
        file_type: str,
        mode: str = "safe",
        program: Optional[str] = None,
    ):
        self.source_id = source_id
        self.file_type = file_type
        self.program = program
        self.mode = mode.lower().strip()
        self._spark = None  # lazy-initialised

    @property
    def spark(self):
        if self._spark is None:
            self._spark = _get_spark()
        return self._spark

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def get_existing(self, period: str, url: str) -> Optional[Dict]:
        """Return the most recent active record for (period, url), or None."""
        safe_period = period.replace("'", "''")
        safe_url    = url.replace("'", "''")
        df = self.spark.sql(f"""
            SELECT * FROM {MANIFEST_TABLE}
            WHERE period = '{safe_period}'
              AND url    = '{safe_url}'
              AND status = 'active'
            ORDER BY version DESC
            LIMIT 1
        """)
        rows = df.collect()
        return rows[0].asDict() if rows else None

    # ------------------------------------------------------------------
    # Decision logic
    # ------------------------------------------------------------------

    def plan(self, period: str, url: str) -> Dict:
        """
        Decide whether to download or skip this file.
        Returns {"decision": "download"|"skip", "reason": str}.
        """
        existing = self.get_existing(period, url)
        if not existing:
            return {"decision": "download", "reason": "not in manifest"}

        if self.mode == "fast":
            return {"decision": "skip", "reason": "exists (fast mode)"}

        # safe mode: confirm the file is physically present on DBFS
        from helpers.paths import dbfs_to_local
        import os
        if not os.path.exists(dbfs_to_local(existing["saved_path"])):
            return {"decision": "download", "reason": "file missing from DBFS"}

        return {"decision": "skip", "reason": "exists and verified on DBFS"}

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    def record(
        self,
        period: str,
        url: str,
        filename: str,
        saved_path: str,
        content: bytes,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
    ):
        """
        Record a successful file download.
        Marks any previous active version as 'replaced', then appends
        the new row with an incremented version number.
        """
        sha256   = hashlib.sha256(content).hexdigest()
        existing = self.get_existing(period, url)
        version  = (existing["version"] + 1) if existing else 1

        if existing:
            safe_period = period.replace("'", "''")
            safe_url    = url.replace("'", "''")
            self.spark.sql(f"""
                UPDATE {MANIFEST_TABLE}
                SET status = 'replaced', updated_at = current_timestamp()
                WHERE period = '{safe_period}'
                  AND url    = '{safe_url}'
                  AND status = 'active'
            """)

        now = datetime.now(timezone.utc)
        row = {
            "source_id":     self.source_id,
            "file_type":     self.file_type,
            "program":       self.program,
            "period":        period,
            "url":           url,
            "filename":      filename,
            "saved_path":    saved_path,
            "bytes":         len(content),
            "sha256":        sha256,
            "etag":          etag,
            "last_modified": last_modified,
            "version":       version,
            "status":        "active",
            "error_message": None,
            "downloaded_at": now,
            "created_at":    now,
            "updated_at":    now,
        }
        from pyspark.sql import Row
        self.spark.createDataFrame([Row(**row)]).write \
            .format("delta").mode("append").saveAsTable(MANIFEST_TABLE)

    def mark_failed(self, period: str, url: str, filename: str, error: str):
        """Record a failed download attempt."""
        now = datetime.now(timezone.utc)
        row = {
            "source_id":     self.source_id,
            "file_type":     self.file_type,
            "program":       self.program,
            "period":        period,
            "url":           url,
            "filename":      filename,
            "saved_path":    "",
            "bytes":         None,
            "sha256":        None,
            "etag":          None,
            "last_modified": None,
            "version":       1,
            "status":        "failed",
            "error_message": error,
            "downloaded_at": None,
            "created_at":    now,
            "updated_at":    now,
        }
        from pyspark.sql import Row
        self.spark.createDataFrame([Row(**row)]).write \
            .format("delta").mode("append").saveAsTable(MANIFEST_TABLE)
