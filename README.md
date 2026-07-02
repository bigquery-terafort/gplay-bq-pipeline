# Google Play Console → BigQuery (free) — 3-file pipeline

Loads Google Play Console reports (installs, organic acquisition, ratings,
crashes, sales, earnings) into BigQuery daily — **without the BigQuery Data
Transfer Service** and its **$25 per app per month** charge. At 120+ apps DTS
costs ~$3,000/month; this does the same job for **$0** by reading the exact
same report CSVs from the private `pubsite_prod_rev_*` bucket Google already
fills for your account. DTS was just a paid, automated copy of those files.

```
Play Console bucket  ──►  GitHub Actions (daily)  ──►  BigQuery
gs://pubsite_prod_rev_*    reads + decodes CSVs         partitioned + clustered
(Google-owned, free)       (keyless, via WIF)            tables, one per report
```

**The whole repo is 3 files:**

```
gplay-bq-pipeline/
├── .github/workflows/play-reports.yml   # schedule + auth + keepalive
├── main.py                              # the entire pipeline
└── README.md                            # this file (setup script + SQL below)
```

Guarantees built in: keyless auth (no stored credentials), idempotent
per-month partition replacement (duplicates impossible, skipped runs
self-heal), all-STRING schema so Google's column changes can't break loads,
bounded memory even on a 120-month backfill, per-table + per-month error
isolation, zero third-party actions, and public-repo-safe logging.

## Safe in a PUBLIC repository

- All config lives in GitHub **Secrets** → auto-masked (`***`) in the
  world-readable Actions logs. There are no credentials at all (WIF is keyless).
- `main.py` never logs your bucket, project, service account, or package
  names at INFO level. (`LOG_LEVEL=WARNING` for near-silence.)
- Need to restrict to specific apps? Use the `APPS_ALLOWLIST` **Secret**
  (comma-separated packages) — never commit your app list to a public repo.
  Default (unset) = all apps auto-discovered, zero maintenance.
- Triggers are `schedule` + `workflow_dispatch` only — fork PRs cannot run the
  workflow or mint credentials. Never add `pull_request` triggers.
- WIF trust is pinned to your owner + repo + default branch.
- Recommended: Settings → Actions → General → *Require approval for all
  external contributors*.
- Bonus: public repos get **unlimited** free Actions minutes.

## Cost: why it stays $0

Reading the bucket: $0 (Google-owned/billed, not requester-pays). IAM/WIF: $0
(IAM API is free). Actions minutes: $0 (public = unlimited). BigQuery batch
loads: $0 (free by design, any volume). Storage: first 10 GiB free — your data
is well under 1 GiB (optional `PARTITION_EXPIRATION_DAYS` keeps it tiny
forever). Queries: first 1 TiB/month free. Set a $1 budget alert as a tripwire
(it *notifies*; it doesn't auto-stop billing — but there's nothing to bill).

## Setup (one time)

### 1. Run the GCP setup script

Copy the block below into a file (or straight into Cloud Shell), **edit the
top variables**, and run it. Safe to re-run — it converges. It creates the
service account, BigQuery roles, and a Workload Identity pool/provider whose
trust is pinned to your GitHub owner + repo + default branch, then prints your
Secret values.

```bash
#!/usr/bin/env bash
set -euo pipefail

# ============================ EDIT THESE ============================
PROJECT_ID="your-gcp-project"
GITHUB_ORG="your-github-username-or-org"            # the OWNER only
GITHUB_REPO="your-github-username-or-org/your-repo" # full owner/name
DEFAULT_BRANCH="main"                               # branch the schedule runs on
SA_NAME="gh-play-reports"
POOL_ID="github-pool"
PROVIDER_ID="github-provider"
BQ_DATASET="play_reports"
BQ_LOCATION="US"
# ===================================================================

PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

ATTR_MAPPING="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner,attribute.ref=assertion.ref"
ATTR_CONDITION="assertion.repository_owner == '${GITHUB_ORG}' && assertion.ref == 'refs/heads/${DEFAULT_BRANCH}'"

echo ">> Enabling required APIs..."
gcloud services enable \
  iam.googleapis.com sts.googleapis.com iamcredentials.googleapis.com \
  bigquery.googleapis.com storage.googleapis.com \
  --project="$PROJECT_ID"

echo ">> Creating service account (idempotent)..."
gcloud iam service-accounts create "$SA_NAME" \
  --project="$PROJECT_ID" \
  --display-name="GitHub Actions - Play Console reports loader" \
  || echo "   (already exists, continuing)"

echo ">> Granting BigQuery roles..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.jobUser" --condition=None >/dev/null
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.dataEditor" --condition=None >/dev/null

echo ">> Creating Workload Identity Pool (idempotent)..."
gcloud iam workload-identity-pools create "$POOL_ID" \
  --project="$PROJECT_ID" --location="global" \
  --display-name="GitHub Actions pool" \
  || echo "   (already exists, continuing)"

echo ">> Creating or updating the OIDC provider (converges on re-run)..."
gcloud iam workload-identity-pools providers create-oidc "$PROVIDER_ID" \
  --project="$PROJECT_ID" --location="global" \
  --workload-identity-pool="$POOL_ID" \
  --display-name="GitHub provider" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="$ATTR_MAPPING" \
  --attribute-condition="$ATTR_CONDITION" \
  || gcloud iam workload-identity-pools providers update-oidc "$PROVIDER_ID" \
       --project="$PROJECT_ID" --location="global" \
       --workload-identity-pool="$POOL_ID" \
       --attribute-mapping="$ATTR_MAPPING" \
       --attribute-condition="$ATTR_CONDITION"

echo ">> Allowing ONLY ${GITHUB_REPO} to impersonate the service account..."
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --project="$PROJECT_ID" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_ID}/attribute.repository/${GITHUB_REPO}" >/dev/null

cat <<EOF

==================== PUT THESE INTO GITHUB REPOSITORY *SECRETS* ====================
(Settings -> Secrets and variables -> Actions -> Secrets -> New repository secret)

WIF_PROVIDER    = projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_ID}/providers/${PROVIDER_ID}
GCP_SA_EMAIL    = ${SA_EMAIL}
GCP_PROJECT_ID  = ${PROJECT_ID}
BQ_DATASET      = ${BQ_DATASET}
BQ_LOCATION     = ${BQ_LOCATION}
GCS_PLAY_BUCKET = <Play Console -> Download reports -> Statistics -> Copy Cloud Storage URI>

Optional Secrets:
APPS_ALLOWLIST            = comma-separated package names (keeps your app list private)
PARTITION_EXPIRATION_DAYS = e.g. 730 to auto-drop data older than ~2 years

NEXT: In Play Console -> Users and permissions, invite ${SA_EMAIL}
Grant at the Account (Global) level:
  - "View app information and download bulk reports"
  - "View financial data, orders, and cancellation survey responses"
Then wait ~24 hours for access to propagate before the first run.
EOF
```

### 2. Invite the service account in Play Console

`Users and permissions` → invite the SA email printed above → grant, at the
**Account (Global)** level: *View app information and download bulk reports* +
*View financial data, orders, and cancellation survey responses*. Wait ~24h
for propagation (skipping the wait is the #1 cause of a false 403).

### 3. Copy your bucket URI

Play Console → `Download reports` → `Statistics` → top-right **Copy Cloud
Storage URI** (looks like `gs://pubsite_prod_rev_0123456789/…`).

### 4. Add the GitHub Secrets

The six values printed by the script (plus the two optional ones if wanted),
under `Settings → Secrets and variables → Actions → Secrets`. Decide
`BQ_LOCATION` now — a dataset's location can't change later.

### 5. Push these 3 files, then run

Actions tab → *Play Console → BigQuery (daily)* → **Run workflow** → set
`months` to `120` once to backfill full history (takes ~1–3 hours; daily runs
take ~5–15 minutes and refresh the last 2 months automatically).

### 6. Verify, THEN kill DTS

Confirm the run is green and tables in the `play_reports` dataset have rows.
Only then: BigQuery → **Data transfers** → disable/delete every Google Play
transfer config to stop the ~$100/day bleed. Watch for duplicate configs.

## Querying (example casting views)

The loader stores every column as STRING on purpose (schema-drift armor); cast
in views. Column names come from Google's headers, lowercased with `_`. They
can vary by account — **confirm first**:

```sql
SELECT column_name
FROM `YOUR_PROJECT.play_reports.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'installs_overview'
ORDER BY ordinal_position;
```

Then adapt and run:

```sql
CREATE OR REPLACE VIEW `YOUR_PROJECT.play_reports.v_daily_installs` AS
SELECT
  package_name,
  SAFE_CAST(date AS DATE)                    AS day,
  SAFE_CAST(daily_user_installs   AS INT64)  AS daily_user_installs,
  SAFE_CAST(daily_device_installs AS INT64)  AS daily_device_installs,
  SAFE_CAST(active_device_installs AS INT64) AS active_device_installs,
  SAFE_CAST(total_user_installs   AS INT64)  AS total_user_installs
FROM `YOUR_PROJECT.play_reports.installs_overview`
WHERE date IS NOT NULL;

CREATE OR REPLACE VIEW `YOUR_PROJECT.play_reports.v_organic_acquisition` AS
SELECT
  package_name,
  SAFE_CAST(date AS DATE)                        AS day,
  traffic_source,
  SAFE_CAST(store_listing_visitors     AS INT64) AS visitors,
  SAFE_CAST(store_listing_acquisitions AS INT64) AS acquisitions
FROM `YOUR_PROJECT.play_reports.store_performance_traffic_source`
WHERE LOWER(traffic_source) LIKE 'play store (organic)%';

CREATE OR REPLACE VIEW `YOUR_PROJECT.play_reports.v_revenue_monthly` AS
SELECT
  report_month_date AS month,
  product_id,
  currency_of_sale,
  ROUND(SUM(SAFE_CAST(amount_buyer_currency AS FLOAT64)), 2) AS gross_amount
FROM `YOUR_PROJECT.play_reports.sales`
GROUP BY month, product_id, currency_of_sale;
```

*(If you ever run `main.py` locally, add a `.gitignore` with `__pycache__/`
before committing — CI never generates artifacts, so the repo doesn't need one.)*
