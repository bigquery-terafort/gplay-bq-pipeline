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
  GCS_PLAY_BUCKET            required  ALL bucket URIs/ids, comma-separated.
                                       Prefix any entry with a friendly console
                                       label (':' or '=') to fill source_console:
                                         "apex: gs://pubsite_prod_1, acme = gs://pubsite_prod_2"
                                       (no label => defaults to the bucket id)
  GCP_SA_EMAIL_2 (.._9)      optional  extra service-account emails. Play caps
                                       ONE identity at 10 developer accounts, so
                                       consoles past 10 live on more SAs. You do
                                       NOT sort buckets by SA — the pipeline
                                       probes each bucket and reads it with
                                       whichever identity has access.
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
from google.api_core.exceptions import NotFound
import google.auth
from google.auth import impersonated_credentials

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
RESERVED_COLUMNS = {"report_month_date", "package_name", "source_console",
                    "_loaded_at", "_source_file"}

# Account-wide financial reports (sales/earnings) identify the app in Google's
# own column. We promote it into `package_name` per row, so EVERY table shares
# one canonical join key. Ordered by preference; extend if Google renames again.
_APP_ID_COLUMNS = ("package_id", "product_id")


class FileRef(NamedTuple):
    """A matched report file: which blob, which app, which month, which console."""
    blob: "storage.Blob"
    pkg: str
    ym: str
    console: str = ""


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
    try:
        raw = blob.download_as_bytes(retry=_RETRY)
    except NotFound:
        # The file was present when we LISTED the bucket but gone by the time we
        # DOWNLOAD it. Google continuously regenerates current-month reports, so
        # a long backfill can race with that rewrite. This is benign: skip the
        # vanished file (the next run loads the fresh version). Do NOT fail the
        # run over it. (No filename logged -> public-repo-log safe.)
        log.warning("A report file vanished between listing and download "
                    "(Google was regenerating it) — skipped; the next run "
                    "will pick up the new version.")
        return []
    if blob.name.lower().endswith(".zip"):
        out: list[dict] = []
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            for inner in zf.namelist():
                if inner.lower().endswith(".csv"):
                    out.extend(_csv_bytes_to_records(zf.read(inner)))
        return out
    return _csv_bytes_to_records(raw)


def _annotate(rows: list[dict], pkg: str, ym: str, source: str,
              console: str) -> None:
    month_date = f"{ym[:4]}-{ym[4:6]}-01"
    now = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r["report_month_date"] = month_date
        if pkg == "__account__":
            # Financial rows: promote Google's own app column (package_id /
            # product_id) into the canonical join key. Rows Google doesn't
            # attribute to an app (e.g. balance adjustments) stay __account__.
            r["package_name"] = next(
                (r[c] for c in _APP_ID_COLUMNS if r.get(c)), "__account__"
            )
        else:
            r["package_name"] = pkg
        r["source_console"] = console      # friendly label for THIS console
        r["_loaded_at"] = now
        r["_source_file"] = source


def _parse_buckets(raw: str) -> list[tuple[str, str]]:
    """
    Parse a comma-separated bucket list. Each entry may carry a friendly
    console label, separated from the bucket by ':' OR '=' :

        'apex: gs://pubsite_prod_1, app variety digital = gs://pubsite_prod_2'
        -> [('apex', 'gs://pubsite_prod_1'),
            ('app variety digital', 'gs://pubsite_prod_2')]

    The bucket is anchored on 'gs://' (or a bare 'pubsite_prod_' id), so labels
    may freely contain spaces, colons, equals, or alignment padding without any
    ambiguity. An entry with no label gets '' — the reader then defaults it to
    the bucket id, so the source_console column is never blank.
    """
    out: list[tuple[str, str]] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        pos = entry.lower().find("gs://")
        if pos > 0:                       # 'label<sep> gs://bucket'  (sep = : or =)
            label = entry[:pos].strip().rstrip(":=").strip()
            bucket = entry[pos:].strip()
        elif pos == 0:                    # 'gs://bucket'  (no label)
            label, bucket = "", entry
        elif "=" in entry:                # 'label=pubsite_prod_id'  (bare id)
            lab, buc = entry.split("=", 1)
            label, bucket = lab.strip(), buc.strip()
        else:                             # bare 'pubsite_prod_id'  (no label)
            label, bucket = "", entry
        out.append((label, bucket))
    return out


def _merge_indexes(
    indexes: list[dict[str, list["FileRef"]]]
) -> dict[str, list["FileRef"]]:
    """
    Union {month: [files]} maps from SEVERAL buckets so each month is loaded
    ONCE with every account's files combined. Loading bucket-by-bucket would
    be a data-loss bug: the second bucket's WRITE_TRUNCATE of a month's
    partition would wipe the first bucket's freshly loaded rows.
    """
    merged: dict[str, list[FileRef]] = {}
    for idx in indexes:
        for ym, refs in idx.items():
            merged.setdefault(ym, []).extend(refs)
    return merged


def _read_month(refs: list["FileRef"]) -> list[dict]:
    """Download + parse + annotate all files of ONE month (any bucket mix)."""
    records: list[dict] = []
    for ref in refs:
        rows = _blob_to_records(ref.blob)
        # Bucket-qualified source => full traceability across accounts.
        # (Goes into BigQuery data, never into public logs.)
        _annotate(rows, ref.pkg, ref.ym,
                  f"{ref.blob.bucket.name}/{ref.blob.name}", ref.console)
        records.extend(rows)
        log.debug("  + %s (%d rows)", ref.blob.name, len(rows))
    return records


class PlayReportsReader:
    """Two-phase, bounded-memory reader.

    Phase 1 (index_*): ONE bucket listing per report -> {YYYYMM: [FileRef]}.
             Metadata only — costs nothing, holds nothing heavy.
    Phase 2 (read_month): download + parse ONLY one month's files at a time.
    """

    def __init__(self, bucket_id: str, project: str, client=None,
                 console: str = ""):
        # Accept 'pubsite_prod_rev_123' or 'gs://pubsite_prod_rev_123/stats/...'
        bucket_id = bucket_id.replace("gs://", "").split("/")[0].strip()
        # An injected client lets a bucket be read under a DIFFERENT identity
        # (impersonated service account) while BigQuery writes stay on the
        # workflow's primary identity.
        self._client = client or storage.Client(project=project)
        self._bucket = self._client.bucket(bucket_id)
        self.bucket_id = bucket_id
        # Friendly label for this console; falls back to the bucket id so the
        # source_console column is never empty.
        self.console = console.strip() or bucket_id

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
            index.setdefault(ym, []).append(FileRef(blob, pkg, ym, self.console))
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
            index.setdefault(ym, []).append(
                FileRef(blob, "__account__", ym, self.console))
        return index

# =========================================================================== #
# SECTION 2 — Writing to BigQuery (idempotent, per-month partitions)
# =========================================================================== #

_META = [
    bigquery.SchemaField("report_month_date", "DATE"),
    bigquery.SchemaField("package_name", "STRING"),
    bigquery.SchemaField("source_console", "STRING"),
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


_CLOUD_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


def _impersonation_emails() -> list[str]:
    """
    Extra service-account identities to try, from GCP_SA_EMAIL_2..9.

    Play Console caps ONE identity at 10 developer accounts, so a big empire
    spreads its consoles across several service accounts. You do NOT tell the
    pipeline which bucket belongs to which SA — it probes each bucket against
    the primary identity first, then each of these, and reads it with whichever
    one actually has access. Add a console on a new SA later? Just append its
    email here; routing keeps working with zero re-shuffling.
    """
    out: list[str] = []
    for n in range(2, 10):
        e = os.environ.get(f"GCP_SA_EMAIL_{n}", "").strip()
        if e:
            out.append(e)
    return out


def _storage_client(project: str, impersonate: str | None):
    """Primary identity when impersonate is None; otherwise a client whose
    every call runs as the target service account (keyless, via the IAM
    Credentials API — free)."""
    if not impersonate:
        return storage.Client(project=project)
    source, _ = google.auth.default()
    creds = impersonated_credentials.Credentials(
        source_credentials=source,
        target_principal=impersonate,
        target_scopes=_CLOUD_SCOPES,
    )
    return storage.Client(project=project, credentials=creds)


def _can_read(client, bucket_id: str) -> bool:
    """True if this identity can list the bucket (cheap 1-object probe)."""
    try:
        next(iter(client.list_blobs(bucket_id, max_results=1, retry=_RETRY)), None)
        return True
    except Exception:
        return False


def _route_bucket(bucket_id: str, clients: list[tuple[str, object]]):
    """Return (client, identity_tag) for the first identity that can read the
    bucket, or (None, None). identity_tag is 'primary'/'sa#2'/... — never an
    email or bucket id, so it's safe to log in a public repo."""
    for tag, client in clients:
        if _can_read(client, bucket_id):
            return client, tag
    return None, None


def main() -> int:
    buckets = _parse_buckets(_env("GCS_PLAY_BUCKET", required=True))
    project = _env("GCP_PROJECT_ID", required=True)
    dataset = _env("BQ_DATASET", default="play_reports")
    location = _env("BQ_LOCATION", default="US")
    months_to_load = int(_env("MONTHS_TO_LOAD", default="2"))

    exp_raw = _env("PARTITION_EXPIRATION_DAYS", default="").strip()
    partition_expiration_days = int(exp_raw) if exp_raw else None

    months = set(_recent_months(months_to_load))
    allowlist = _allowlist_from_env()

    # Build one client per identity: primary (the workflow's own SA via WIF)
    # plus one impersonated client for each GCP_SA_EMAIL_2..9.
    clients: list[tuple[str, object]] = [("primary", _storage_client(project, None))]
    for i, email in enumerate(_impersonation_emails(), start=2):
        clients.append((f"sa#{i}", _storage_client(project, email)))

    # NOTE: deliberately NOT logging bucket / project / service identifiers —
    # in a public repo these logs are world-readable. Counts + tags only.
    log.info("Config: dataset=%s location=%s buckets=%d identities=%d "
             "months=%d (%s..%s) apps=%s expiry=%s",
             dataset, location, len(buckets), len(clients), len(months),
             min(months), max(months),
             f"{len(allowlist)} allowlisted" if allowlist else "ALL",
             f"{partition_expiration_days}d" if partition_expiration_days
             else "keep forever")

    # Auto-route every bucket to the identity that can actually read it.
    readers = []
    routing: dict[str, int] = {}
    unreadable: list[str] = []
    for label, bucket in buckets:
        bid = bucket.replace("gs://", "").split("/")[0].strip()
        client, tag = _route_bucket(bid, clients)
        if client is None:
            unreadable.append(label or "(unlabeled)")
            continue
        readers.append(
            PlayReportsReader(bucket, project, client=client, console=label))
        routing[tag] = routing.get(tag, 0) + 1

    log.info("Routing: %s%s",
             ", ".join(f"{k}={v}" for k, v in sorted(routing.items())) or "none",
             f" | UNREADABLE={len(unreadable)}" if unreadable else "")
    for u in unreadable:
        # label only (user-chosen) — never the bucket id — to stay log-safe.
        log.error("Unreadable bucket (label=%s): still propagating, or its "
                  "console was invited to a service account not listed in "
                  "GCP_SA_EMAIL_2..9.", u)
    if not readers:
        log.error("No readable buckets — nothing to do. Aborting.")
        return 2

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
                # Phase 1: ONE cheap metadata listing PER BUCKET, merged into
                # a single {month: [files]} map — so every month is loaded
                # ONCE with all accounts' files (never truncating each other).
                if spec["scope"] == "per_app":
                    index = _merge_indexes([
                        rd.index_per_app(spec["prefix"], spec["report"],
                                         dim, months, allowlist)
                        for rd in readers
                    ])
                else:
                    index = _merge_indexes([
                        rd.index_account(spec["prefix"], months)
                        for rd in readers
                    ])
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
                    rows = _read_month(index[ym])
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
