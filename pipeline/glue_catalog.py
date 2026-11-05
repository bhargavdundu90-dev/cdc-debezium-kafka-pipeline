"""Glue Data Catalog version of the `_CURRENT` pointer used by the local daily run.

On AWS the dimension is a Glue table whose StorageDescriptor.Location points at one
version folder, e.g. s3://lake/silver/dim_customer_scd2/v=2026-10-02/. Promoting a new
version is a single UpdateTable call, so Athena queries switch from yesterday's folder to
today's in one step and never see a half-written table.

UpdateTable takes a TableInput, not the Table that GetTable returns: the read-only fields
(CreateTime, DatabaseName, VersionId, ...) have to be dropped or the call is rejected.
Everything else (columns, SerDe, table parameters) is carried over unchanged, so the
schema defined in Terraform is not lost on the first promote.
"""
from __future__ import annotations

from datetime import datetime, timezone

# fields GetTable returns that TableInput does not accept
_READ_ONLY = {
    "DatabaseName", "CreateTime", "UpdateTime", "CreatedBy", "IsRegisteredWithLakeFormation",
    "CatalogId", "VersionId", "FederatedTable", "IsMultiDialectView", "Status",
}


class GlueCatalog:
    def __init__(self, database: str, table: str, client=None):
        if client is None:
            import boto3
            client = boto3.client("glue")
        self.glue = client
        self.database = database
        self.table = table

    def _get(self) -> dict:
        return self.glue.get_table(DatabaseName=self.database, Name=self.table)["Table"]

    def current_location(self) -> str | None:
        """The live version's location, or None while the table still has its placeholder."""
        table = self._get()
        if table.get("Parameters", {}).get("cdc_live_version") is None:
            return None
        return table["StorageDescriptor"]["Location"]

    def live_version(self) -> str | None:
        return self._get().get("Parameters", {}).get("cdc_live_version")

    def promote(self, location: str, version: str, run_id: str) -> dict:
        """Point the table at `location`; returns the TableInput that was sent."""
        table = self._get()
        table_input = {k: v for k, v in table.items() if k not in _READ_ONLY}
        table_input["StorageDescriptor"] = {**table["StorageDescriptor"], "Location": location}
        table_input["Parameters"] = {
            **table.get("Parameters", {}),
            "cdc_live_version": version,
            "cdc_previous_version": table.get("Parameters", {}).get("cdc_live_version", ""),
            "cdc_last_run_id": run_id,
            "cdc_promoted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        self.glue.update_table(DatabaseName=self.database, TableInput=table_input)
        return table_input

    def rollback(self, table_root: str, run_id: str) -> str:
        """Point the table back at the previous version (one step, for a bad promote found later)."""
        prev = self._get().get("Parameters", {}).get("cdc_previous_version")
        if not prev:
            raise RuntimeError("no previous version recorded on the table")
        location = f"{table_root.rstrip('/')}/{prev}/"
        self.promote(location, prev, run_id)
        return location
