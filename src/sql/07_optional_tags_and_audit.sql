-- =====================================================================
-- 07_optional_tags_and_audit.sql — OPTIONAL, run by hand in the SQL editor
-- (not part of the job). Run each block separately; if one fails on your
-- workspace edition, skip it — the masks and row filters still protect data.
-- =====================================================================
USE CATALOG hc_erp_dev;

-- ---------- 1. Column-level PII tags on pipeline-managed materialized views ----------
ALTER MATERIALIZED VIEW silver.patients_current SET TAGS ('contains_phi' = 'true', 'regulation' = 'DPDP-Act-2023,HIPAA');
ALTER MATERIALIZED VIEW silver.patients_current ALTER COLUMN first_name SET TAGS ('pii' = 'name');
ALTER MATERIALIZED VIEW silver.patients_current ALTER COLUMN last_name  SET TAGS ('pii' = 'name');
ALTER MATERIALIZED VIEW silver.patients_current ALTER COLUMN phone      SET TAGS ('pii' = 'phone');
ALTER MATERIALIZED VIEW silver.patients_current ALTER COLUMN email      SET TAGS ('pii' = 'email');
ALTER MATERIALIZED VIEW silver.patients_current ALTER COLUMN aadhaar_no SET TAGS ('pii' = 'national_id');
ALTER MATERIALIZED VIEW silver.patients_current ALTER COLUMN address    SET TAGS ('pii' = 'address');
ALTER MATERIALIZED VIEW silver.patients_current ALTER COLUMN dob        SET TAGS ('pii' = 'dob');
ALTER MATERIALIZED VIEW silver.staff ALTER COLUMN monthly_salary_inr    SET TAGS ('sensitivity' = 'confidential');

-- ---------- 2. PHI access audit (needs access to system tables) ----------
CREATE OR REPLACE VIEW ops.phi_access_audit
COMMENT 'Reads of silver (PHI) tables from the Unity Catalog audit log'
AS SELECT
  event_time,
  user_identity.email               AS user_email,
  action_name,
  request_params['full_name_arg']   AS table_name,
  source_ip_address
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name IN ('getTable', 'generateTemporaryTableCredential')
  AND request_params['full_name_arg'] LIKE 'hc_erp_dev.silver.%'
  AND event_date >= current_date() - INTERVAL 7 DAYS;
