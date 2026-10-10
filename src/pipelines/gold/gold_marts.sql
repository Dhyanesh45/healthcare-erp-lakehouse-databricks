-- =====================================================================
-- GOLD layer — SQL materialized views inside the same SDP/DLT pipeline.
-- Gold is DE-IDENTIFIED (no names/phones/IDs) so it is safe for Genie,
-- dashboards and analysts. Facility row filter still applies.
-- ${hc.catalog} is substituted from the pipeline configuration.
-- =====================================================================

-- Encounter fact with patient demographics (non-identifying only)
CREATE OR REFRESH MATERIALIZED VIEW gold.fact_encounters
  COMMENT '[GOLD] One row per encounter with LOS, charges, diagnosis and de-identified demographics'
  TBLPROPERTIES ('quality' = 'gold')
  WITH ROW FILTER ${hc.catalog}.governance.facility_filter ON (facility_id)
AS SELECT
  e.encounter_id,
  e.patient_id,                       -- surrogate key only, no PHI
  e.facility_id,
  e.department,
  e.attending_doctor_id,
  e.encounter_type,
  e.admit_date,
  e.admit_ts,
  e.discharge_ts,
  e.los_days,
  e.is_active_inpatient,
  e.icd10_code,
  e.diagnosis_desc,
  e.total_charges_inr,
  e.readmission_flag,
  p.age_band,
  p.gender,
  p.city
FROM silver.encounters e
LEFT JOIN silver.patients_current p USING (patient_id);

-- Daily KPIs per facility
CREATE OR REFRESH MATERIALIZED VIEW gold.facility_daily_kpis
  COMMENT '[GOLD] Daily admissions, discharges, average LOS, revenue and readmissions per facility'
  TBLPROPERTIES ('quality' = 'gold')
  WITH ROW FILTER ${hc.catalog}.governance.facility_filter ON (facility_id)
AS SELECT
  facility_id,
  admit_date                                                        AS kpi_date,
  count(*)                                                          AS total_encounters,
  count_if(encounter_type = 'INPATIENT')                            AS admissions,
  count_if(encounter_type = 'EMERGENCY')                            AS er_visits,
  count_if(encounter_type = 'OUTPATIENT')                           AS opd_visits,
  count_if(is_active_inpatient)                                     AS active_inpatients,
  round(avg(CASE WHEN encounter_type = 'INPATIENT' THEN los_days END), 2) AS avg_los_days,
  sum(total_charges_inr)                                            AS gross_revenue_inr,
  round(100.0 * count_if(readmission_flag) / count(*), 2)           AS readmission_rate_pct
FROM silver.encounters
GROUP BY facility_id, admit_date;

-- Revenue cycle by payer
CREATE OR REFRESH MATERIALIZED VIEW gold.revenue_cycle_by_payer
  COMMENT '[GOLD] Claims billed vs approved, denial rate and top denial reason per payer and month'
  TBLPROPERTIES ('quality' = 'gold')
AS SELECT
  payer,
  date_trunc('MONTH', submitted_date)                               AS claim_month,
  count(*)                                                          AS claims,
  sum(billed_amount_inr)                                            AS billed_inr,
  sum(coalesce(approved_amount_inr, 0))                             AS approved_inr,
  round(100.0 * count_if(claim_status = 'DENIED') / count(*), 2)    AS denial_rate_pct,
  round(100.0 * sum(coalesce(approved_amount_inr,0)) / nullif(sum(billed_amount_inr),0), 2) AS collection_rate_pct,
  mode(denial_reason)                                               AS top_denial_reason
FROM silver.claims
GROUP BY payer, date_trunc('MONTH', submitted_date);

-- Stock on hand per item & facility
CREATE OR REFRESH MATERIALIZED VIEW gold.inventory_stock_position
  COMMENT '[GOLD] Current stock on hand per item and facility, with low-stock and near-expiry flags'
  TBLPROPERTIES ('quality' = 'gold')
  WITH ROW FILTER ${hc.catalog}.governance.facility_filter ON (facility_id)
AS SELECT
  facility_id,
  item_code,
  max(item_name)                                                    AS item_name,
  max(category)                                                     AS category,
  sum(signed_qty)                                                   AS stock_on_hand,
  max(reorder_level)                                                AS reorder_level,
  sum(signed_qty) < max(reorder_level)                              AS is_low_stock,
  count_if(expiry_date < date_add(current_date(), 30) AND txn_type = 'RECEIPT') AS batches_expiring_30d,
  sum(CASE WHEN txn_type = 'ISSUE' THEN txn_value_inr END)          AS consumption_value_inr
FROM silver.inventory_txn
GROUP BY facility_id, item_code;

-- Department performance
CREATE OR REFRESH MATERIALIZED VIEW gold.department_performance
  COMMENT '[GOLD] Encounters, LOS, revenue and doctor headcount per department and facility'
  TBLPROPERTIES ('quality' = 'gold')
  WITH ROW FILTER ${hc.catalog}.governance.facility_filter ON (facility_id)
AS WITH docs AS (
  SELECT facility_id, department, count(*) AS doctors
  FROM silver.staff WHERE role = 'Doctor' GROUP BY ALL
)
SELECT
  e.facility_id,
  e.department,
  count(*)                              AS encounters,
  round(avg(e.los_days), 2)             AS avg_los_days,
  sum(e.total_charges_inr)              AS revenue_inr,
  coalesce(max(d.doctors), 0)           AS doctors,
  round(sum(e.total_charges_inr) / nullif(max(d.doctors), 0), 2) AS revenue_per_doctor_inr
FROM silver.encounters e
LEFT JOIN docs d ON e.facility_id = d.facility_id AND e.department = d.department
GROUP BY e.facility_id, e.department;

-- Readmissions by diagnosis
CREATE OR REFRESH MATERIALIZED VIEW gold.readmission_by_diagnosis
  COMMENT '[GOLD] 30-day readmission rate by ICD-10 diagnosis'
  TBLPROPERTIES ('quality' = 'gold')
AS SELECT
  icd10_code,
  max(diagnosis_desc)                                      AS diagnosis,
  count(*)                                                 AS encounters,
  count_if(readmission_flag)                               AS readmissions,
  round(100.0 * count_if(readmission_flag) / count(*), 2)  AS readmission_rate_pct,
  round(avg(los_days), 2)                                  AS avg_los_days
FROM silver.encounters
GROUP BY icd10_code;
