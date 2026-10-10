-- =====================================================================
-- 01_security_functions.sql — Data masking & row-level security UDFs
-- Must run BEFORE the pipeline, because silver/gold tables declare
-- `MASK <fn>` and `ROW FILTER <fn>` that point at these functions.
--
-- Groups used (create them in Settings → Identity and access → Groups):
--   hc_admins        full clear-text access, all facilities
--   hc_compliance    full clear-text access (audits), all facilities
--   hc_clinicians_*  clear clinical identity for THEIR facility only
--   hc_finance       financial columns (salary) + all facilities, PHI masked
--   hc_analysts      de-identified only (everything masked)
-- =====================================================================
USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA governance;

-- Helper: is the caller allowed to see clear PHI?
CREATE OR REPLACE FUNCTION can_see_phi()
RETURNS BOOLEAN
COMMENT 'TRUE when the current user belongs to a PHI-privileged group'
RETURN is_account_group_member('hc_admins')
    OR is_account_group_member('hc_compliance')
    OR is_account_group_member('hc_clinicians_chn')
    OR is_account_group_member('hc_clinicians_blr')
    OR is_account_group_member('hc_clinicians_hyd')
    OR is_account_group_member('hc_clinicians_mum');

-- ---------------- Column masks ----------------
CREATE OR REPLACE FUNCTION mask_name(v STRING) RETURNS STRING
COMMENT 'Shows first letter only, e.g. R*****'
RETURN CASE WHEN can_see_phi() THEN v
            WHEN v IS NULL THEN NULL
            ELSE concat(left(v, 1), repeat('*', greatest(length(v) - 1, 1))) END;

CREATE OR REPLACE FUNCTION mask_phone(v STRING) RETURNS STRING
COMMENT 'Keeps last 4 digits'
RETURN CASE WHEN can_see_phi() THEN v
            WHEN v IS NULL THEN NULL
            ELSE concat('XXXXXX', right(regexp_replace(v, '[^0-9]', ''), 4)) END;

CREATE OR REPLACE FUNCTION mask_email(v STRING) RETURNS STRING
COMMENT 'j***@domain.com'
RETURN CASE WHEN can_see_phi() THEN v
            WHEN v IS NULL OR instr(v, '@') = 0 THEN '***'
            ELSE concat(left(v, 1), '***', substr(v, instr(v, '@'))) END;

CREATE OR REPLACE FUNCTION mask_aadhaar(v STRING) RETURNS STRING
COMMENT 'National ID: only admins/compliance see it; others get XXXX XXXX 1234'
RETURN CASE WHEN is_account_group_member('hc_admins') OR is_account_group_member('hc_compliance') THEN v
            WHEN v IS NULL THEN NULL
            ELSE concat('XXXX XXXX ', right(v, 4)) END;

CREATE OR REPLACE FUNCTION mask_mrn(v STRING) RETURNS STRING
COMMENT 'Deterministic hash so analysts can still join/count without seeing the MRN'
RETURN CASE WHEN can_see_phi() THEN v ELSE concat('H-', left(sha2(v, 256), 12)) END;

CREATE OR REPLACE FUNCTION mask_dob(v DATE) RETURNS DATE
COMMENT 'Non-privileged users see Jan 1 of birth year (HIPAA Safe-Harbor style)'
RETURN CASE WHEN can_see_phi() THEN v ELSE make_date(year(v), 1, 1) END;

CREATE OR REPLACE FUNCTION mask_redact(v STRING) RETURNS STRING
COMMENT 'Full redaction for addresses, policy numbers, PAN'
RETURN CASE WHEN is_account_group_member('hc_admins') OR is_account_group_member('hc_compliance') THEN v
            ELSE '[REDACTED]' END;

CREATE OR REPLACE FUNCTION mask_salary(v INT) RETURNS INT
COMMENT 'Salary visible only to finance/admins'
RETURN CASE WHEN is_account_group_member('hc_admins') OR is_account_group_member('hc_finance') THEN v
            ELSE NULL END;

-- ---------------- Row filter ----------------
CREATE OR REPLACE FUNCTION facility_filter(facility_id STRING)
RETURNS BOOLEAN
COMMENT 'Row-level security: user sees rows only for facilities mapped to their groups'
RETURN is_account_group_member('hc_admins')
    OR EXISTS (
         SELECT 1 FROM governance.facility_access a
         WHERE (a.facility_id = '*' OR a.facility_id = facility_id)
           AND is_account_group_member(a.group_name)
       );
