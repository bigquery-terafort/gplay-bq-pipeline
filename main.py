#!/usr/bin/env python3
"""
main.py — Google Play Console -> BigQuery, free of charge. (Single-file pipeline)

Pulls monthly report CSVs straight from your private pubsite_prod_rev_* bucket
and loads them into BigQuery. No BigQuery Data Transfer Service, no $25/app,
no stored credentials. Scales flat across any number of apps.

Design (bleed-proof by construction):
  * Bucket is listed ONCE per report (cheap metadata), then months are
    processed ONE AT A TIME: download -> load -> free memory. A 120-month
    backfill across 120+ apps runs in bounded memory on a standard runner.
  * Idempotent: each month's BigQuery partition is atomically replaced via
    WRITE_TRUNCATE on table$YYYYMM. Re-runs never duplicate; skipped runs
    self-heal on the next run.
  * Schema-drift proof: every report column loads as STRING with
    ALLOW_FIELD_ADDITION, so Google adding/renaming columns (fee columns
    change in July 2026) can never break ingestion. Cast in SQL views.
  * Per-table AND per-month error isolation: one bad file/month can never
    kill the rest of the run; any failure still exits non-zero so GitHub
    emails you.
  * PUBLIC-REPO SAFE: never logs the bucket name, project id, service
    account, or package names at INFO level — public Actions logs stay clean.
    (Config also lives in GitHub Secrets, which GitHub auto-masks.)

Run locally  : `gcloud auth application-default login` then `python main.py`
Run in CI    : auth handled by Workload Identity Federation (see workflow YAML)

Environment:
  GCS_PLAY_BUCKET            required  gs://pubsite_prod_rev_... URI or bucket id
  GCP_PROJECT_ID             required  your GCP project id
  BQ_DATASET                 optional  default: play_reports
  BQ_LOCATION                optional  default: US (decide before first run)
  MONTHS_TO_LOAD             optional  default: 2 (set 120 once to backfill)
  APPS_ALLOWLIST             optional  comma-separated packages; empty = ALL apps
  PARTITION_EXPIRATION_DAYS  optional  e.g. 730 to auto-drop old months
  LOG_LEVEL                  optional  default: INFO
"""
from __future__ import annotations

import io
import os
import re
import sys
import csv
import zipfile
import logging
from typing import NamedTuple
from datetime import datetime, timezone

from google.cloud import storage, bigquery
from google.api_core.retry import Retry

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gplay")

# =========================================================================== #
# Report configuration. Edit freely.
#   scope "per_app"  -> one file per app:  {report}_{pkg}_{YYYYMM}_{dim}.csv
#   scope "account"  -> one file for the whole account (financial reports)
#   BigQuery table   -> per_app: "{report}_{dim}"   account: "{report}"
# =========================================================================== #
REPORTS = [
    # Total installs / uninstalls / active devices:
    {"report": "installs", "prefix": "stats/installs/", "scope": "per_app",
     "dimensions": ["overview", "country"]},

    # ORGANIC vs other acquisition channels (Play Store organic search/explore):
    {"report": "store_performance", "prefix": "stats/store_performance/",
     "scope": "per_app", "dimensions": ["traffic_source"]},

    # Ratings & crashes (optional, cheap):
    {"report": "ratings", "prefix": "stats/ratings/", "scope": "per_app",
     "dimensions": ["overview"]},
    {"report": "crashes", "prefix": "stats/crashes/", "scope": "per_app",
     "dimensions": ["overview"]},

    # In-app purchases / revenue (account-wide -> pulled ONCE, not per app):
    {"report": "sales", "prefix": "sales/", "scope": "account",
     "dimensions": [None]},
    {"report": "earnings", "prefix": "earnings/", "scope": "account",
     "dimensions": [None]},
]

# =========================================================================== #
# SECTION 1 — Reading the Play Console bucket
# =========================================================================== #

# Retry transient GCS errors (5xx, connection resets) automatically.
_RETRY = Retry(initial=1.0, maximum=30.0, multiplier=2.0, timeout=300.0)

# Column names reserved for our own metadata. Any incoming CSV column that
# cleans to one of these is dropped (our version is authoritative).
RESERVED_COLUMNS = {"report_month_date", "package_name", "_loaded_at", "_source_file"}


class FileRef(NamedTuple):
    """A matched report file: which blob, which app, which month."""
    blob: "storage.Blob"
    pkg: str
    ym: str


def clean_column(name: str) -> str:
    """'Daily Device Installs' -> 'daily_device_installs' (BigQuery-safe)."""
    c = name.strip().lower()
    c = re.sub(r"[^0-9a-z]+", "_", c)
    c = re.sub(r"_+", "_", c).strip("_")
    if not c:
        c = "col"
    if c[0].isdigit():
        c = "_" + c
    return c


def _dedupe(cols: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for c in cols:
        if c in seen:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            out.append(c)
    return out


def _decode(raw: bytes) -> str:
    """Play stats CSVs are UTF-16LE with a BOM; financial CSVs (sales/earnings)
    are UTF-8 with no BOM. Sniff the BOM first, then use NUL-byte density to
    tell BOM-less UTF-16 apart from UTF-8 — never guess UTF-16 on UTF-8 bytes
    (that silently turns revenue data into mojibake). Never crash."""
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return raw.decode("utf-16", errors="replace")
    if raw[:3] == b"\xef\xbb\xbf":
        return raw.decode("utf-8-sig", errors="replace")
    sample = raw[:4096]
    if sample.count(b"\x00") > len(sample) // 4:
        return raw.decode("utf-16", errors="replace")
    for enc in ("utf-8", "latin-1"):
        try:
            return raw.decode(enc)
        except (UnicodeDecodeError, UnicodeError):
            continue
    return raw.decode("utf-8", errors="replace")


def _csv_bytes_to_records(raw: bytes) -> list[dict]:
    text = _decode(raw)
    reader = csv.reader(io.StringIO(text))
    rows = [r for r in reader if any(cell.strip() for cell in r)]
    if not rows:
        return []
    header = _dedupe([clean_column(h) for h in rows[0]])
    records: list[dict] = []
    for r in rows[1:]:
        # Pad/truncate defensively — never rely on a fixed column count.
        r = (r + [""] * len(header))[: len(header)]
        rec = {header[i]: (r[i] if r[i] != "" else None) for i in range(len(header))}
        for k in list(rec.keys()):        # drop columns colliding with metadata
            if k in RESERVED_COLUMNS:
                del rec[k]
        records.append(rec)
    return records


def _blob_to_records(blob) -> list[dict]:
    raw = blob.download_as_bytes(retry=_RETRY)
    if blob.name.lower().endswith(".zip"):
        out: list[dict] = []
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            for inner in zf.namelist():
                if inner.lower().endswith(".csv"):
                    out.extend(_csv_bytes_to_records(zf.read(inner)))
        return out
    return _csv_bytes_to_records(raw)


def _annotate(rows: list[dict], pkg: str, ym: str, source: str) -> None:
    month_date = f"{ym[:4]}-{ym[4:6]}-01"
    now = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r["report_month_date"] = month_date
        r["package_name"] = pkg
        r["_loaded_at"] = now
        r["_source_file"] = source


class PlayReportsReader:
    """Two-phase, bounded-memory reader.

    Phase 1 (index_*): ONE bucket listing per report -> {YYYYMM: [FileRef]}.
             Metadata only — costs nothing, holds nothing heavy.
    Phase 2 (read_month): download + parse ONLY one month's files at a time.
    """

    def __init__(self, bucket_id: str, project: str):
        # Accept 'pubsite_prod_rev_123' or 'gs://pubsite_prod_rev_123/stats/...'
        bucket_id = bucket_id.replace("gs://", "").split("/")[0].strip()
        self._client = storage.Client(project=project)
        self._bucket = self._client.bucket(bucket_id)
        self.bucket_id = bucket_id

    def _list(self, prefix: str):
        return self._client.list_blobs(self._bucket, prefix=prefix, retry=_RETRY)

    def index_per_app(self, prefix: str, report: str, dimension: str,
                      months: set[str],
                      allowlist: set[str] | None) -> dict[str, list[FileRef]]:
        """
        One listing pass. Match files like:
            {report}_{package}_{YYYYMM}_{dimension}.csv
        e.g. installs_com.example.app_202607_country.csv
        Returns {YYYYMM: [FileRef, ...]} for the requested months only.
        """
        pat = re.compile(
            rf"^{re.escape(report)}_(?P<pkg>.+)_(?P<ym>\d{{6}})_"
            rf"{re.escape(dimension)}\.csv$"
        )
        index: dict[str, list[FileRef]] = {}
        for blob in self._list(prefix):
            base = blob.name.split("/")[-1]
            m = pat.match(base)
            if not m:
                continue
            ym = m.group("ym")
            if ym not in months:
                continue
            pkg = m.group("pkg")
            if allowlist and pkg not in allowlist:
                continue
            index.setdefault(ym, []).append(FileRef(blob, pkg, ym))
        return index

    def index_account(self, prefix: str,
                      months: set[str]) -> dict[str, list[FileRef]]:
        """
        Account-wide financial reports (sales, earnings): one file covers ALL
        apps, so match on month only. Some months ship as multiple zip parts —
        all parts are indexed and unioned.
        """
        index: dict[str, list[FileRef]] = {}
        month_re = re.compile(r"(\d{6})")
        for blob in self._list(prefix):
            base = blob.name.split("/")[-1]
            if not (base.endswith(".csv") or base.endswith(".zip")):
                continue
            ym = next((x for x in month_re.findall(base) if x in months), None)
            if ym is None:
                continue
            index.setdefault(ym, []).append(FileRef(blob, "__account__", ym))
        return index

    def read_month(self, refs: list[FileRef]) -> list[dict]:
        """Download + parse + annotate all files of ONE month."""
        records: list[dict] = []
        for ref in refs:
            rows = _blob_to_records(ref.blob)
            _annotate(rows, ref.pkg, ref.ym, ref.blob.name)
            records.extend(rows)
            log.debug("  + %s (%d rows)", ref.blob.name, len(rows))
        return records

# =========================================================================== #
# SECTION 2 — Writing to BigQuery (idempotent, per-month partitions)
# =========================================================================== #

_META = [
    bigquery.SchemaField("report_month_date", "DATE"),
    bigquery.SchemaField("package_name", "STRING"),
    bigquery.SchemaField("_loaded_at", "TIMESTAMP"),
    bigquery.SchemaField("_source_file", "STRING"),
]
_META_NAMES = {f.name for f in _META}

_MS_PER_DAY = 24 * 60 * 60 * 1000


class BigQueryWriter:
    def __init__(self, project: str, dataset: str, location: str,
                 partition_expiration_days: int | None = None):
        self._client = bigquery.Client(project=project, location=location)
        self._project = project
        self._dataset = dataset
        self._location = location
        self._exp_ms = (partition_expiration_days * _MS_PER_DAY
                        if partition_expiration_days else None)
        self._ensure_dataset()

    def _ds_ref(self) -> str:
        return f"{self._project}.{self._dataset}"

    def _partitioning(self) -> "bigquery.TimePartitioning":
        return bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="report_month_date",
            expiration_ms=self._exp_ms,   # None => partitions never expire
        )

    def _ensure_dataset(self) -> None:
        ds = bigquery.Dataset(self._ds_ref())
        ds.location = self._location
        self._client.create_dataset(ds, exists_ok=True)
        log.info("Dataset ready: %s (%s)", self._ds_ref(), self._location)

    def _ensure_table(self, table: str, data_columns: list[str]) -> str:
        table_id = f"{self._ds_ref()}.{table}"
        schema = list(_META) + [
            bigquery.SchemaField(c, "STRING")
            for c in data_columns if c not in _META_NAMES
        ]
        tbl = bigquery.Table(table_id, schema=schema)
        tbl.time_partitioning = self._partitioning()
        tbl.clustering_fields = ["package_name"]
        tbl = self._client.create_table(tbl, exists_ok=True)

        # If an expiration is configured, keep pre-existing tables in sync so
        # turning the knob on later still applies everywhere. (When no
        # expiration is configured we never touch existing settings.)
        if self._exp_ms is not None:
            current = (tbl.time_partitioning.expiration_ms
                       if tbl.time_partitioning else None)
            if current != self._exp_ms:
                tbl.time_partitioning = self._partitioning()
                self._client.update_table(tbl, ["time_partitioning"])
                log.info("Partition expiration on %s set to %d day(s)",
                         table, self._exp_ms // _MS_PER_DAY)
        return table_id

    def load_month(self, table: str, ym: str, records: list[dict]) -> int:
        """Replace one month's partition with `records` (all apps for that month)."""
        if not records:
            return 0

        # Union of all columns seen this batch (schemas can drift between apps).
        cols: list[str] = []
        seen: set[str] = set()
        for r in records:
            for k in r.keys():
                if k not in seen:
                    seen.add(k)
                    cols.append(k)
        data_columns = [c for c in cols if c not in _META_NAMES]

        table_id = self._ensure_table(table, data_columns)
        schema = list(_META) + [bigquery.SchemaField(c, "STRING") for c in data_columns]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
            ],
            time_partitioning=self._partitioning(),
            clustering_fields=["package_name"],
        )

        # Target ONLY this month's partition: table$YYYYMM
        destination = f"{table_id}${ym}"
        job = self._client.load_table_from_json(records, destination, job_config=job_config)
        job.result(timeout=900)  # wait; raises on failure or after 15 min (no silent hangs)
        log.info("Loaded %d rows -> %s (partition %s)", len(records), table, ym)
        return len(records)

# =========================================================================== #
# SECTION 3 — Orchestration
# =========================================================================== #

def _env(name: str, default=None, required: bool = False) -> str:
    v = os.environ.get(name, default)
    if required and not v:
        log.error("Missing required environment variable: %s", name)
        sys.exit(2)
    return v


def _recent_months(n: int) -> list[str]:
    """Return the last n months as 'YYYYMM', most recent first."""
    today = datetime.now(timezone.utc).date()
    y, m = today.year, today.month
    out = []
    for _ in range(max(1, n)):
        out.append(f"{y:04d}{m:02d}")
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    return out


def _allowlist_from_env() -> set[str] | None:
    """
    Optional allowlist via the APPS_ALLOWLIST env var (comma-separated package
    names). Set it as a GitHub *Secret* so your app portfolio never appears in
    a public repo. Unset/empty => process ALL apps found in the bucket.
    """
    raw = os.environ.get("APPS_ALLOWLIST", "").strip()
    if not raw:
        return None
    return {p.strip() for p in raw.split(",") if p.strip()} or None


def main() -> int:
    bucket = _env("GCS_PLAY_BUCKET", required=True)
    project = _env("GCP_PROJECT_ID", required=True)
    dataset = _env("BQ_DATASET", default="play_reports")
    location = _env("BQ_LOCATION", default="US")
    months_to_load = int(_env("MONTHS_TO_LOAD", default="2"))

    exp_raw = _env("PARTITION_EXPIRATION_DAYS", default="").strip()
    partition_expiration_days = int(exp_raw) if exp_raw else None

    months = set(_recent_months(months_to_load))
    allowlist = _allowlist_from_env()

    # NOTE: deliberately NOT logging bucket / project / service identifiers —
    # in a public repo these logs are world-readable.
    log.info("Config: dataset=%s location=%s months=%d (%s..%s) apps=%s expiry=%s",
             dataset, location, len(months), min(months), max(months),
             f"{len(allowlist)} allowlisted" if allowlist else "ALL",
             f"{partition_expiration_days}d" if partition_expiration_days
             else "keep forever")

    reader = PlayReportsReader(bucket, project)
    writer = BigQueryWriter(project, dataset, location,
                            partition_expiration_days=partition_expiration_days)

    total_rows = 0
    errors: list[str] = []

    for spec in REPORTS:
        for dim in spec["dimensions"]:
            table = (f'{spec["report"]}_{dim}'
                     if spec["scope"] == "per_app" else spec["report"])
            log.info("=== %s ===", table)
            try:
                # Phase 1: ONE cheap metadata listing -> {month: [files]}
                if spec["scope"] == "per_app":
                    index = reader.index_per_app(
                        spec["prefix"], spec["report"], dim, months, allowlist
                    )
                else:
                    index = reader.index_account(spec["prefix"], months)
            except Exception as exc:
                log.exception("FAILED listing: %s", table)
                errors.append(f"{table} (listing): {exc}")
                continue

            if not index:
                log.info("  (no files found for the target months)")
                continue

            # Phase 2: process ONE month at a time -> bounded memory, and a
            # bad month can never take down the other months.
            for ym in sorted(index):
                try:
                    rows = reader.read_month(index[ym])
                    loaded = writer.load_month(table, ym, rows)
                    total_rows += loaded
                    log.info("  %s: %d file(s) -> %d rows loaded",
                             ym, len(index[ym]), loaded)
                except Exception as exc:
                    log.exception("FAILED: %s month %s", table, ym)
                    errors.append(f"{table} {ym}: {exc}")
                finally:
                    rows = None  # free before the next month

    log.info("──────────────────────────────────────────────")
    log.info("Done. %d rows loaded across all tables.", total_rows)
    if errors:
        log.error("%d table/month load(s) failed:", len(errors))
        for e in errors:
            log.error("  - %s", e)
        return 1  # non-zero => GitHub marks the run failed and notifies you
    return 0


if __name__ == "__main__":
    sys.exit(main())
