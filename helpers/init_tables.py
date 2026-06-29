# init_tables.py — Create Delta tables for the gale_visa pipeline on Databricks
"""
Run once to set up the schema and file_manifest Delta table.

Usage from a Databricks notebook:
    from helpers.init_tables import init_tables
    init_tables()

Usage from terminal (with databricks-connect configured):
    python -m helpers.init_tables
"""

from helpers.paths import CATALOG, SCHEMA, MANIFEST_TABLE


def init_tables(spark=None):
    """Create the gale_visa schema and file_manifest Delta table if they don't exist."""
    if spark is None:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("gale_visa_init").getOrCreate()

    print("Initializing Databricks tables...")
    print(f"  Catalog : {CATALOG}")
    print(f"  Schema  : {SCHEMA}")
    print(f"  Table   : {MANIFEST_TABLE}")
    print()

    # Create schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    print(f"✓ Schema '{CATALOG}.{SCHEMA}' ready")

    # Create file_manifest Delta table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE} (
            source_id      STRING    NOT NULL  COMMENT 'Data source: dolstats, visastats, uscis',
            file_type      STRING    NOT NULL  COMMENT 'monthly | annual | dol | uscis',
            program        STRING              COMMENT 'LCA Program, PERM Program, h1b, etc.',
            period         STRING    NOT NULL  COMMENT 'e.g. PERM Program/2024, FY2024-10',
            url            STRING    NOT NULL  COMMENT 'Original download URL',
            filename       STRING    NOT NULL,
            saved_path     STRING    NOT NULL  COMMENT 'Full DBFS path',
            bytes          BIGINT              COMMENT 'File size in bytes',
            sha256         STRING              COMMENT 'SHA-256 content hash',
            etag           STRING,
            last_modified  STRING,
            version        INT       DEFAULT 1,
            status         STRING    DEFAULT 'active'  COMMENT 'active | missing | replaced | failed',
            error_message  STRING,
            downloaded_at  TIMESTAMP,
            created_at     TIMESTAMP DEFAULT current_timestamp(),
            updated_at     TIMESTAMP DEFAULT current_timestamp()
        )
        USING DELTA
        COMMENT 'Tracks all downloaded files across ETL data sources'
    """)
    print(f"✓ Table '{MANIFEST_TABLE}' ready")

    # Enable auto-optimize for efficient small-file writes
    spark.sql(f"""
        ALTER TABLE {MANIFEST_TABLE}
        SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)
    print("✓ Delta auto-optimize enabled")

    print()
    print("✅ Databricks tables initialized successfully")
    print()
    print("Supported file_types: monthly, annual, dol, uscis")
    print(f"Default DBFS root  : set GALE_VISA_DBFS_ROOT to override")
    print(f"Default catalog    : set GALE_VISA_CATALOG to override (use 'main' for Unity Catalog)")


if __name__ == "__main__":
    init_tables()
