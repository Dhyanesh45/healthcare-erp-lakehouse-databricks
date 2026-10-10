-- =====================================================================
-- 06_genie_metadata.sql — schema-level descriptions Genie reads.
-- Gold tables are pipeline-managed materialized views, so their table
-- comments live in src/pipelines/gold/gold_marts.sql, and business
-- vocabulary (facility codes, LOS, denial rate) lives in
-- genie/instructions.md.
-- =====================================================================
USE CATALOG IDENTIFIER(:catalog);

COMMENT ON SCHEMA gold IS
  'De-identified hospital KPIs for MediCore (4 branches: FAC-CHN Chennai, FAC-BLR Bengaluru, FAC-HYD Hyderabad, FAC-MUM Mumbai). Admissions, ER/OPD visits, length of stay, revenue in INR, insurance claims by payer, pharmacy stock, department performance, readmissions by ICD-10 diagnosis.';

COMMENT ON SCHEMA silver IS
  'Cleansed patient, encounter, claim, inventory and staff records. Contains PHI protected by column masks and a facility row filter.';

COMMENT ON SCHEMA ops IS
  'Data-quality results, daily per-facility KPI snapshots and alerts written by the job.';

-- Sanity check: every gold table has rows (fails loudly if the pipeline produced nothing)
SELECT 'facility_daily_kpis' AS gold_table, count(*) AS row_count FROM gold.facility_daily_kpis
UNION ALL SELECT 'revenue_cycle_by_payer',   count(*) FROM gold.revenue_cycle_by_payer
UNION ALL SELECT 'inventory_stock_position', count(*) FROM gold.inventory_stock_position
UNION ALL SELECT 'department_performance',   count(*) FROM gold.department_performance
UNION ALL SELECT 'readmission_by_diagnosis', count(*) FROM gold.readmission_by_diagnosis
UNION ALL SELECT 'fact_encounters',          count(*) FROM gold.fact_encounters;
