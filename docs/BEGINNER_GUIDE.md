# MediCore Healthcare ERP Lakehouse — Beginner's Step-by-Step Guide

You don't need any Databricks experience. Follow the steps in order; each one says
**where to click**, **what to type**, and **what you should see**.
Total time: about 2–3 hours the first time.

---

## Part 0 — What you are building (read once, 5 minutes)

```
 ERP exports (fake data)        Lakeflow Pipeline (SDP / DLT)                 Consumers
 ─────────────────────   ┌───────────────────────────────────────────┐   ─────────────────────
 patients  (CDC)         │ BRONZE        SILVER              GOLD     │   Genie space (chat)
 encounters         ───► │ raw JSON ──►  cleaned, typed ──►  KPIs &   │──► Databricks App
 claims                  │ Auto Loader   quality rules       marts    │   (MediCore website)
 inventory               │               SCD2 history        (no PHI) │   SQL / dashboards
 staff                   └───────────────────────────────────────────┘
        ▲                         ▲ masks + row filters (Unity Catalog)
        └──── one Lakeflow JOB runs everything, every night, in order ────┘
```

### Words you will meet

| Term | Plain-English meaning |
|---|---|
| **Workspace** | Your Databricks website (e.g. `https://dbc-1234.cloud.databricks.com`). |
| **Unity Catalog (UC)** | The governance layer. Every table has a 3-part name: `catalog.schema.table` (like `hc_erp_dev.gold.facility_daily_kpis`). Permissions, masks, lineage and audit live here. |
| **Volume** | A folder inside Unity Catalog for files (our raw JSON lands here). |
| **Lakehouse** | Data lake files + database features (Delta tables) in one place. |
| **Medallion architecture** | Bronze = raw, Silver = clean, Gold = business-ready. |
| **SDP / DLT** | *Lakeflow Spark Declarative Pipelines* — the new name of *Delta Live Tables*. You declare tables; Databricks figures out order, retries, and incremental processing. This project uses **both** APIs: `import dlt` (bronze) and `from pyspark import pipelines as dp` (silver). |
| **Auto Loader** | Reads only *new* files each run. |
| **Expectations** | Data-quality rules inside the pipeline (warn, drop bad rows, or fail). |
| **AUTO CDC / SCD Type 2** | Applies insert/update/delete change feeds and keeps full history of patient records. |
| **Streaming table vs Materialized view** | Streaming table = processes new rows incrementally. Materialized view = a stored query result that refreshes. |
| **Job / Task** | A Job is a workflow; Tasks are its steps (SQL, Python, pipeline, for-each loop, if/else). |
| **Asset Bundle (DAB)** | The `databricks.yml` project format. One command deploys pipeline + job + app. |
| **Column mask / Row filter** | UC functions that hide values (e.g. phone → `XXXXXX1234`) or hide rows (e.g. only your hospital branch) depending on *who is asking*. |
| **Genie space** | Ask questions in English, get SQL + answers. |
| **Databricks App** | A web app hosted inside Databricks, signed in with your Databricks login. |

### Project files

```
hc-erp-lakehouse/
├── databricks.yml                     ← main config (edit 3 values here)
├── resources/
│   ├── pipeline.yml                   ← the SDP/DLT pipeline
│   ├── jobs.yml                       ← the 10-task job
│   └── app.yml                        ← the web app
├── src/
│   ├── sql/00_setup_catalog.sql       ← catalog, schemas, volume, ops tables
│   ├── sql/01_security_functions.sql  ← masking + row-filter functions
│   ├── sql/02_governance_security.sql ← tags, grants, audit view
│   ├── sql/06_genie_metadata.sql      ← comments that help Genie
│   ├── notebooks/01_generate_erp_data.py   ← fake hospital data
│   ├── notebooks/03_dq_report.py           ← data-quality report
│   ├── notebooks/04_facility_snapshot.py   ← runs once per hospital (for-each)
│   ├── notebooks/05_quality_alert.py       ← runs only if quality is bad (if/else)
│   └── pipelines/{bronze,silver,gold}/     ← medallion code
├── genie/                             ← Genie instructions + example SQL
└── app/                               ← the website (FastAPI + HTML)
```

---

## Part 1 — Get a Databricks workspace (15 min)

**Recommended: a 14-day free trial** on AWS, Azure or GCP (go to databricks.com → *Try Databricks*).
It gives you every feature this project uses (Unity Catalog, serverless, groups, Genie, Apps).

*Alternative: Databricks Free Edition* (free forever, serverless only). Most of the project
works there, but some admin features — especially account-level groups used by the masking
functions — may be limited. If something in Part 3 isn't available, use a trial instead.

1. Sign up and open your workspace.
2. Copy the URL from the browser address bar up to `.com` (or `.net` on Azure).
   Example: `https://dbc-a1b2c3d4-e5f6.cloud.databricks.com` → this is your **HOST**.
3. Make sure **Unity Catalog** is on: left sidebar → **Catalog**. If you see catalogs listed
   (e.g. `main`, `system`, `samples`), you're good.

### Quick UI tour (left sidebar)
- **Workspace** – your files and notebooks
- **Catalog** – browse catalogs → schemas → tables, see lineage and permissions
- **Jobs & Pipelines** – your job and pipeline will appear here
- **SQL Editor / SQL Warehouses** – run SQL; warehouses are SQL compute
- **Genie** – natural-language spaces
- **Compute → Apps** – your web app

---

## Part 2 — Find your SQL Warehouse ID (5 min)

1. Sidebar → **SQL Warehouses**.
2. A trial normally has one already (e.g. *Serverless Starter Warehouse*). If not: **Create SQL warehouse** → type *Serverless* → size *2X-Small* → Create.
3. Click the warehouse → look at the URL: `.../sql/warehouses/`**`1a2b3c4d5e6f7g8h`** — that last part is the **WAREHOUSE_ID**. (Also visible under *Connection details* → HTTP path.)
4. Make sure it's **Running** (click *Start* if stopped).

---

## Part 3 — Create the security groups (10 min) — IMPORTANT

The masking and row-filter functions decide what each person can see based on group membership.

1. Click your profile icon (top-right) → **Settings** → **Identity and access** → **Groups** → **Manage** → **Add group**.
2. Create these 8 groups (exact names):

   `hc_admins` · `hc_compliance` · `hc_finance` · `hc_analysts` ·
   `hc_clinicians_chn` · `hc_clinicians_blr` · `hc_clinicians_hyd` · `hc_clinicians_mum`

3. Open `hc_admins` → **Add members** → add **yourself**.

> ⚠️ **Why this matters:** the pipeline runs *as you*. The gold tables read the silver tables
> through the row filter — if you're not in `hc_admins`, the pipeline only "sees" zero rows and
> your gold tables will be empty.

Optional (to *see* masking working): invite a second user (a second email address) and put them in
`hc_analysts` only. Later they'll see `R*****`, `XXXXXX1234`, `[REDACTED]` where you see clear values.

---

## Part 4 — Install the Databricks CLI on your laptop (10 min)

**Windows** (PowerShell):
```powershell
winget install Databricks.DatabricksCLI
```
**macOS**:
```bash
brew tap databricks/tap
brew install databricks
```
**Linux**:
```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

Close and reopen the terminal, then check:
```bash
databricks -v
```
You should see a version number. Use a recent version (if `bundle` commands later complain about
unknown fields such as `apps` or `user_api_scopes`, upgrade the CLI).

### Log in
```bash
databricks auth login --host https://<YOUR-HOST>
```
A browser opens → sign in → back in the terminal it asks for a profile name → press Enter (`DEFAULT`).

Test:
```bash
databricks current-user me
```
You should see your email in the JSON.

---

## Part 5 — Configure the project (10 min)

1. Unzip `hc-erp-lakehouse.zip` and open the folder in VS Code (or any editor).
2. Open **`databricks.yml`** and replace:

| Find | Replace with |
|---|---|
| `https://<your-workspace>.cloud.databricks.com` (both places) | your HOST from Part 1 |
| `<YOUR_SQL_WAREHOUSE_ID>` | your WAREHOUSE_ID from Part 2 |
| `data-team@example.com` | your email |

3. Leave the `prod` target alone for now (you'll only use `dev`).
4. Open **`app/app.yaml`** — `HC_CATALOG` is `hc_erp_dev`. Keep it.

---

## Part 6 — Validate and deploy (5 min)

In the terminal, inside the project folder:

```bash
cd hc-erp-lakehouse
databricks bundle validate -t dev
```
✅ Expect: `Validation OK!`
❌ If you see an error, it names the file and line — usually a typo in the host or warehouse ID.

```bash
databricks bundle deploy -t dev
```
This uploads the code and creates:
- a **pipeline** named `[dev yourname] [dev] HC-ERP Medallion (SDP/DLT)`
- a **job** named `[dev yourname] [dev] HC-ERP End-to-End Lakehouse`
- an **app** named `hc-erp-portal-dev`

Check in the UI: sidebar → **Jobs & Pipelines** — you should see the job and the pipeline.

> In `dev` mode, Databricks prefixes names with `[dev yourname]` and pauses schedules. That's normal.

---

## Part 7 — Run the end-to-end job (15–25 min)

```bash
databricks bundle run hc_erp_end_to_end -t dev
```
(or in the UI: **Jobs & Pipelines** → click the job → **Run now**).

Open the job in the UI → **Runs** tab → click the running run. You'll see the task graph:

| # | Task | What it does | Type |
|---|---|---|---|
| 1 | `setup_catalog` | Creates catalog `hc_erp_dev`, schemas `landing/bronze/silver/gold/governance/ops`, a volume, ops tables, and the facility-access map | SQL task |
| 2 | `security_functions` | Creates masking functions (`mask_phone`, `mask_aadhaar`…) and the `facility_filter` row filter | SQL task |
| 3 | `generate_data` | Writes fake ERP JSON files into the volume | Python task (serverless) |
| 4 | `medallion_pipeline` | Runs the SDP/DLT pipeline: bronze → silver → gold | Pipeline task |
| 5 | `apply_governance` | Adds classification tags, grants per group, creates a PHI audit view | SQL task |
| 6 | `dq_report` | Reads pipeline quality metrics, saves them, sets a *task value* | Python task |
| 7 | `per_facility_kpis` | **For-each** loop: runs 4 times in parallel, once per hospital | For-each task |
| 8 | `breach_check` | **If/else**: is failed-record % > 2? | Condition task |
| 9 | `notify_on_breach` | Only runs if the condition was true — writes an alert | Python task |
| 10 | `refresh_genie_metadata` | Adds rich table/column comments for Genie | SQL task |

Green = success. Click any task to see its output/logs.

> 💡 `notify_on_breach` will often show as **Excluded/Skipped** — that means quality was fine. That's the if/else working.

### If `setup_catalog` fails on `CREATE CATALOG`
Some workspaces require a storage location for new catalogs. Fix:
Sidebar → **Catalog** → **+** → **Create a catalog** → name `hc_erp_dev` → use default storage → Create.
Then **Repair run** (button on the failed run) — the `IF NOT EXISTS` makes it safe to re-run.

---

## Part 8 — Explore the pipeline (10 min)

Sidebar → **Jobs & Pipelines** → click the **pipeline**.

1. **Graph** — you'll see every table and the arrows between them (bronze → silver → gold).
2. Click `silver.encounters` → **Data quality** tab: how many rows passed/failed each expectation
   (`has_facility`, `non_negative_charge`…). About 2% of fake rows are deliberately bad.
3. Click `ops.encounters_quarantine` — this is where the bad rows went, for a data steward to review.
4. Click `silver.patients` — this is the **SCD Type 2** table; it has `__START_AT` / `__END_AT` columns.

---

## Part 9 — Explore data, masking and lineage (15 min)

Sidebar → **SQL Editor** → choose your warehouse (top right) → paste and run:

```sql
-- Business KPIs (gold)
SELECT * FROM hc_erp_dev.gold.facility_daily_kpis ORDER BY kpi_date DESC LIMIT 20;

-- Patient data (silver) — you are hc_admins, so you see clear values
SELECT patient_id, first_name, phone, email, aadhaar_no, address
FROM hc_erp_dev.silver.patients_current LIMIT 10;

-- Patient history (SCD Type 2)
SELECT patient_id, city, __START_AT, __END_AT
FROM hc_erp_dev.silver.patients ORDER BY patient_id, __START_AT LIMIT 20;

-- Which groups am I in?
SELECT is_account_group_member('hc_admins') AS admin, is_account_group_member('hc_analysts') AS analyst;
```

### See masking in action
**Option A (two users):** log in as your second user (in `hc_analysts`) and run the patient query —
names become `R*****`, phones `XXXXXX1234`, Aadhaar `XXXX XXXX 1234`, address `[REDACTED]`,
DOB becomes Jan 1 of the birth year, and MRN becomes a hash `H-3fa9…`.

**Option B (one user):** temporarily remove yourself from `hc_admins` and add yourself to
`hc_clinicians_chn`. Re-run the query: you'll see **only Chennai patients** (row filter) in clear text.
Switch back to `hc_admins` afterwards (before the next pipeline run!).

### Lineage & tags
Sidebar → **Catalog** → `hc_erp_dev` → `gold` → `facility_daily_kpis` → **Lineage** tab → *See lineage graph*:
shows bronze → silver → gold automatically. Open `silver.patients_current` → **Overview**:
see the tags (`pii = phone`, etc.) and the mask icon on protected columns.

---

## Part 10 — Create the Genie space (10 min)

Follow `genie/README.md` (summary):

1. Sidebar → **Genie** → **+ New**.
2. Add the 6 tables from `hc_erp_dev.gold`.
3. Pick your warehouse, title it `MediCore Hospital Analyst`.
4. **Instructions** → paste `genie/instructions.md`.
5. **SQL queries / examples** → add the queries from `genie/sample_queries.sql`.
6. Ask: *“Which payer has the highest denial rate?”* — Genie writes SQL and answers.
7. Copy the **space ID** from the URL (`/genie/rooms/<SPACE_ID>`).

Connect it to the app:
- `app/app.yaml` → set `GENIE_SPACE_ID: "<SPACE_ID>"` (keep the quotes)
- `resources/app.yml` → uncomment the 4 `genie-space` lines and paste the ID
- Redeploy: `databricks bundle deploy -t dev`

---

## Part 11 — Launch the website (10 min)

```bash
databricks bundle run hc_erp_portal -t dev
```
This uploads `app/`, installs `requirements.txt`, and starts it. Wait until it says the app is running.

Open it: sidebar → **Compute** → **Apps** → `hc-erp-portal-dev` → click the URL.
The first time, Databricks asks you to **authorize** the app to use SQL and Genie on your behalf → Allow.

What you'll see:
- **Overview** – KPI cards, encounter trend per hospital, department revenue, readmissions
- **Revenue cycle** – billed vs approved per insurer, denial and collection rates
- **Inventory** – low-stock and near-expiry items
- **Patient lookup** – search patients; masked values are highlighted orange. Click a row → encounter history.
- **Ask Genie** – chat with the Genie space inside your own website

Because the app runs every query **as the logged-in user**, a user in `hc_analysts` sees masked
data in the website too — governance is enforced by Unity Catalog, not by app code.

Give colleagues access: App page → **Permissions** → add users/groups with *Can Use*.

---

## Part 12 — Run it again (see incremental + CDC) (5 min)

```bash
databricks bundle run hc_erp_end_to_end -t dev
```
A new batch of fake data lands. Auto Loader picks up **only the new files**, and patient
updates/deletes create new SCD2 versions. Compare row counts before/after in the SQL editor.

To schedule nightly: `resources/jobs.yml` → `pause_status: UNPAUSED` → deploy.

---

## Part 13 — Costs, cleanup, and going to production

- **Save money:** stop the SQL warehouse when idle (it auto-stops after a few minutes anyway);
  stop the app from Compute → Apps → **Stop**.
- **Remove everything the bundle created:**
  ```bash
  databricks bundle destroy -t dev
  ```
  (Tables in the catalog are kept; drop with `DROP CATALOG hc_erp_dev CASCADE;` if you want.)
- **Production:** create a service principal, put its application ID in `databricks.yml` under
  `prod.run_as`, add it to `hc_admins`, then `databricks bundle deploy -t prod`.
  Typically you'd do this from CI/CD (GitHub Actions) instead of your laptop.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `bundle validate` → unknown field (`apps`, `user_api_scopes`, `for_each_task`) | Upgrade the Databricks CLI. |
| Pipeline error: function `...governance.mask_name` not found | `security_functions` task didn't run first. Run the whole job, not just the pipeline. |
| Gold tables exist but are **empty** | You (the pipeline owner) aren't in `hc_admins`. Add yourself, then in the pipeline click **Start → Full refresh all**. |
| `GRANT` fails: principal does not exist | One of the 8 groups is missing or misspelled (Part 3). |
| `CREATE CATALOG` fails | Create the catalog in the UI (Part 7 note) and repair the run. |
| App shows *PERMISSION_DENIED* | The user lacks a group/grant, or didn't authorize the app. That's governance working. Add them to a `hc_*` group. |
| App shows *DATABRICKS_WAREHOUSE_ID is not configured* | Warehouse ID in `databricks.yml` is wrong, or the app wasn't redeployed. |
| Genie tab: *Genie space not configured* | Do Part 10 and redeploy. |
| `generate_data` fails on `faker` | Serverless environment couldn't install packages — check the workspace allows PyPI access. |

---

## What to show in a demo / interview

1. Job graph with SQL, Python, Pipeline, **For-each** and **If/else** tasks
2. Pipeline graph + **Data quality** tab + quarantine table
3. SCD2 history in `silver.patients`
4. Same query, two users → clear vs masked data, all vs one facility
5. Lineage graph and PII tags in Catalog Explorer
6. Genie answering a business question
7. The website — same masking enforced end to end
