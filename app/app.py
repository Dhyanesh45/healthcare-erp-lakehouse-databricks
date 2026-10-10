"""
MediCore Command Center — Databricks App (FastAPI backend + static HTML frontend)

Security model
  Databricks Apps forward the signed-in user's OAuth token in the
  `x-forwarded-access-token` header (on-behalf-of-user auth, enabled via
  `user_api_scopes` in resources/app.yml). Every SQL query and every Genie call is
  executed AS THAT USER, so Unity Catalog column masks and facility row filters
  are enforced automatically — the app never sees data the user isn't allowed to.
"""
import os
import re
from typing import Any

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

CFG = Config()
CATALOG = os.getenv("HC_CATALOG", "hc_erp_dev")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "")
FACILITY_RE = re.compile(r"^FAC-[A-Z]{3}$")

app = FastAPI(title="MediCore Command Center")
app.mount("/static", StaticFiles(directory="static"), name="static")


# ---------------------------------------------------------------- helpers
def run_query(token: str | None, query: str, params: dict | None = None) -> list[dict[str, Any]]:
    if not WAREHOUSE_ID:
        raise HTTPException(500, "DATABRICKS_WAREHOUSE_ID is not configured")
    conn_kwargs = dict(server_hostname=CFG.host.replace("https://", ""),
                       http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}")
    if token:                                   # on-behalf-of-user
        conn_kwargs["access_token"] = token
    else:                                       # local dev / app service principal
        conn_kwargs["credentials_provider"] = lambda: CFG.authenticate
    try:
        with sql.connect(**conn_kwargs) as conn, conn.cursor() as cur:
            cur.execute(query, params or {})
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, [_jsonable(v) for v in row])) for row in cur.fetchall()]
    except Exception as exc:  # surface permission errors nicely
        raise HTTPException(403 if "PERMISSION" in str(exc).upper() else 500, str(exc)[:400])


def _jsonable(v):
    if v is None or isinstance(v, (int, float, str, bool)):
        return v
    try:
        return float(v)                         # Decimal
    except (TypeError, ValueError):
        return str(v)                           # dates/timestamps


def ws_client(token: str | None) -> WorkspaceClient:
    return WorkspaceClient(host=CFG.host, token=token, auth_type="pat") if token else WorkspaceClient()


def T(name: str) -> str:
    return f"{CATALOG}.{name}"


# ---------------------------------------------------------------- pages
@app.get("/")
def index():
    return FileResponse("static/index.html")


@app.get("/api/me")
def me(x_forwarded_email: str | None = Header(None), x_forwarded_access_token: str | None = Header(None)):
    groups = run_query(x_forwarded_access_token, """
        SELECT g FROM VALUES ('hc_admins'),('hc_compliance'),('hc_finance'),('hc_analysts'),
          ('hc_clinicians_chn'),('hc_clinicians_blr'),('hc_clinicians_hyd'),('hc_clinicians_mum') AS t(g)
        WHERE is_account_group_member(g)""")
    return {"email": x_forwarded_email or "local-dev", "groups": [g["g"] for g in groups],
            "catalog": CATALOG, "genie_enabled": bool(GENIE_SPACE_ID)}


# ---------------------------------------------------------------- dashboard APIs
@app.get("/api/kpis")
def kpis(days: int = Query(30, ge=1, le=365), x_forwarded_access_token: str | None = Header(None)):
    rows = run_query(x_forwarded_access_token, f"""
        SELECT sum(total_encounters)   AS encounters,
               sum(admissions)         AS admissions,
               sum(er_visits)          AS er_visits,
               round(avg(avg_los_days),2) AS avg_los_days,
               sum(gross_revenue_inr)  AS revenue_inr,
               round(avg(readmission_rate_pct),2) AS readmission_rate_pct
        FROM {T('gold.facility_daily_kpis')}
        WHERE kpi_date >= date_sub(current_date(), :days)""", {"days": days})
    active = run_query(x_forwarded_access_token,
                       f"SELECT count_if(is_active_inpatient) AS n FROM {T('gold.fact_encounters')}")
    out = rows[0] if rows else {}
    out["active_inpatients"] = active[0]["n"] if active else 0
    return out


@app.get("/api/trend")
def trend(days: int = Query(60, ge=7, le=365), x_forwarded_access_token: str | None = Header(None)):
    return run_query(x_forwarded_access_token, f"""
        SELECT kpi_date, facility_id, total_encounters, admissions, er_visits,
               avg_los_days, gross_revenue_inr, readmission_rate_pct
        FROM {T('gold.facility_daily_kpis')}
        WHERE kpi_date >= date_sub(current_date(), :days)
        ORDER BY kpi_date""", {"days": days})


@app.get("/api/payers")
def payers(x_forwarded_access_token: str | None = Header(None)):
    return run_query(x_forwarded_access_token, f"""
        SELECT payer, sum(claims) AS claims, sum(billed_inr) AS billed_inr, sum(approved_inr) AS approved_inr,
               round(100.0*sum(approved_inr)/nullif(sum(billed_inr),0),1) AS collection_rate_pct,
               round(sum(claims*denial_rate_pct)/nullif(sum(claims),0),1)   AS denial_rate_pct
        FROM {T('gold.revenue_cycle_by_payer')} GROUP BY payer ORDER BY billed_inr DESC""")


@app.get("/api/departments")
def departments(x_forwarded_access_token: str | None = Header(None)):
    return run_query(x_forwarded_access_token, f"""
        SELECT department, sum(encounters) AS encounters, round(avg(avg_los_days),2) AS avg_los_days,
               sum(revenue_inr) AS revenue_inr, sum(doctors) AS doctors
        FROM {T('gold.department_performance')} GROUP BY department ORDER BY revenue_inr DESC""")


@app.get("/api/inventory/low-stock")
def low_stock(x_forwarded_access_token: str | None = Header(None)):
    return run_query(x_forwarded_access_token, f"""
        SELECT facility_id, item_code, item_name, category, stock_on_hand, reorder_level, batches_expiring_30d
        FROM {T('gold.inventory_stock_position')}
        WHERE is_low_stock OR batches_expiring_30d > 0
        ORDER BY facility_id, stock_on_hand LIMIT 200""")


@app.get("/api/readmissions")
def readmissions(x_forwarded_access_token: str | None = Header(None)):
    return run_query(x_forwarded_access_token, f"""
        SELECT icd10_code, diagnosis, encounters, readmission_rate_pct, avg_los_days
        FROM {T('gold.readmission_by_diagnosis')} ORDER BY readmission_rate_pct DESC""")


# ---------------------------------------------------------------- PHI (masked per user)
@app.get("/api/patients")
def patients(q: str = Query("", max_length=40),
             facility: str | None = None,
             x_forwarded_access_token: str | None = Header(None)):
    params: dict[str, Any] = {"q": f"%{q.strip()}%"}
    where = "(patient_id ILIKE :q OR first_name ILIKE :q OR last_name ILIKE :q OR city ILIKE :q)"
    if facility:
        if not FACILITY_RE.match(facility):
            raise HTTPException(400, "bad facility id")
        where += " AND home_facility_id = :fac"
        params["fac"] = facility
    return run_query(x_forwarded_access_token, f"""
        SELECT patient_id, mrn, first_name, last_name, dob, age_band, gender, phone, email,
               aadhaar_no, city, blood_group, home_facility_id
        FROM {T('silver.patients_current')}
        WHERE {where} ORDER BY patient_id LIMIT 50""", params)


@app.get("/api/patients/{patient_id}/encounters")
def patient_encounters(patient_id: str, x_forwarded_access_token: str | None = Header(None)):
    return run_query(x_forwarded_access_token, f"""
        SELECT encounter_id, facility_id, department, encounter_type, admit_ts, discharge_ts,
               los_days, icd10_code, diagnosis_desc, total_charges_inr
        FROM {T('gold.fact_encounters')} WHERE patient_id = :pid ORDER BY admit_ts DESC LIMIT 100""",
                     {"pid": patient_id})


@app.get("/api/patients/{patient_id}/claims")
def patient_claims(patient_id: str, x_forwarded_access_token: str | None = Header(None)):
    return run_query(x_forwarded_access_token, f"""
        SELECT claim_id, payer, claim_status, billed_amount_inr, approved_amount_inr,
               denial_reason, submitted_date
        FROM {T('silver.claims')} WHERE patient_id = :pid ORDER BY submitted_date DESC LIMIT 50""",
                     {"pid": patient_id})


# ---------------------------------------------------------------- Genie chat
@app.post("/api/genie/ask")
def genie_ask(body: dict, x_forwarded_access_token: str | None = Header(None)):
    if not GENIE_SPACE_ID:
        raise HTTPException(400, "Genie space not configured — set GENIE_SPACE_ID in app/app.yaml")
    question = (body.get("question") or "").strip()[:500]
    if not question:
        raise HTTPException(400, "empty question")
    w = ws_client(x_forwarded_access_token)
    conv_id = body.get("conversation_id")
    msg = (w.genie.create_message_and_wait(GENIE_SPACE_ID, conv_id, question) if conv_id
           else w.genie.start_conversation_and_wait(GENIE_SPACE_ID, question))

    answer = {"conversation_id": msg.conversation_id, "text": None, "sql": None, "columns": [], "rows": []}
    for att in msg.attachments or []:
        if att.text and att.text.content:
            answer["text"] = att.text.content
        if att.query:
            answer["sql"] = att.query.query
            answer["text"] = answer["text"] or att.query.description
            res = w.genie.get_message_attachment_query_result(
                GENIE_SPACE_ID, msg.conversation_id, msg.id, att.attachment_id)
            sr = res.statement_response
            if sr and sr.manifest and sr.result:
                answer["columns"] = [c.name for c in sr.manifest.schema.columns]
                answer["rows"] = (sr.result.data_array or [])[:200]
    return answer
