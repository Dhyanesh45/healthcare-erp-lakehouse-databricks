-- =====================================================================
-- 02_governance_security.sql — runs AFTER the pipeline
--   * Classification tags on schemas
--   * Least-privilege GRANTs per persona group
--     (Databricks allows ONE principal per GRANT statement)
-- Column-level PII tags + the PHI audit view are in
-- 07_optional_tags_and_audit.sql (run manually in the SQL editor).
-- =====================================================================
USE CATALOG IDENTIFIER(:catalog);

-- ---------- Classification tags ----------
ALTER SCHEMA bronze SET TAGS ('layer' = 'bronze', 'data_classification' = 'phi');
ALTER SCHEMA silver SET TAGS ('layer' = 'silver', 'data_classification' = 'phi');
ALTER SCHEMA gold   SET TAGS ('layer' = 'gold',   'data_classification' = 'de-identified');

-- ---------- Catalog access ----------
GRANT ALL PRIVILEGES ON CATALOG IDENTIFIER(:catalog) TO `hc_admins`;
GRANT USE CATALOG ON CATALOG IDENTIFIER(:catalog) TO `hc_compliance`;
GRANT USE CATALOG ON CATALOG IDENTIFIER(:catalog) TO `hc_finance`;
GRANT USE CATALOG ON CATALOG IDENTIFIER(:catalog) TO `hc_analysts`;
GRANT USE CATALOG ON CATALOG IDENTIFIER(:catalog) TO `hc_clinicians_chn`;
GRANT USE CATALOG ON CATALOG IDENTIFIER(:catalog) TO `hc_clinicians_blr`;
GRANT USE CATALOG ON CATALOG IDENTIFIER(:catalog) TO `hc_clinicians_hyd`;
GRANT USE CATALOG ON CATALOG IDENTIFIER(:catalog) TO `hc_clinicians_mum`;

-- Bronze: admins only (nobody else gets USE SCHEMA)

-- ---------- Silver: clinicians + compliance (masks / row filters decide what they see) ----------
GRANT USE SCHEMA, SELECT ON SCHEMA silver TO `hc_compliance`;
GRANT USE SCHEMA, SELECT ON SCHEMA silver TO `hc_clinicians_chn`;
GRANT USE SCHEMA, SELECT ON SCHEMA silver TO `hc_clinicians_blr`;
GRANT USE SCHEMA, SELECT ON SCHEMA silver TO `hc_clinicians_hyd`;
GRANT USE SCHEMA, SELECT ON SCHEMA silver TO `hc_clinicians_mum`;
GRANT USE SCHEMA ON SCHEMA silver TO `hc_finance`;
GRANT SELECT ON TABLE silver.staff  TO `hc_finance`;
GRANT SELECT ON TABLE silver.claims TO `hc_finance`;

-- ---------- Gold: everyone (de-identified) ----------
GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `hc_compliance`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `hc_finance`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `hc_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `hc_clinicians_chn`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `hc_clinicians_blr`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `hc_clinicians_hyd`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `hc_clinicians_mum`;

-- ---------- Ops: compliance reads quality results ----------
GRANT USE SCHEMA, SELECT ON SCHEMA ops TO `hc_compliance`;

-- ---------- Governance functions: everyone reading masked tables must execute them ----------
GRANT USE SCHEMA, EXECUTE ON SCHEMA governance TO `hc_compliance`;
GRANT USE SCHEMA, EXECUTE ON SCHEMA governance TO `hc_finance`;
GRANT USE SCHEMA, EXECUTE ON SCHEMA governance TO `hc_analysts`;
GRANT USE SCHEMA, EXECUTE ON SCHEMA governance TO `hc_clinicians_chn`;
GRANT USE SCHEMA, EXECUTE ON SCHEMA governance TO `hc_clinicians_blr`;
GRANT USE SCHEMA, EXECUTE ON SCHEMA governance TO `hc_clinicians_hyd`;
GRANT USE SCHEMA, EXECUTE ON SCHEMA governance TO `hc_clinicians_mum`;
