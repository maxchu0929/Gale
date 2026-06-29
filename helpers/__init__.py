"""
Helper modules for the gale_visa ETL pipeline (Databricks).

- manifest    : DeltaManifest — tracks downloaded files in a Delta Lake table
- init_tables : creates the gale_visa schema and file_manifest table on Databricks
- paths       : DBFS path helpers and Delta table name configuration
"""

__version__ = "1.0.0"