-- =====================================================================
-- 00_setup_catalog.sql  — Unity Catalog foundation (idempotent)
-- Run by job task `setup_catalog` with parameter :catalog
-- =====================================================================
CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog)
  COMMENT 'MediCore Healthcare ERP Lakehouse';

USE CATALOG IDENTIFIER(:catalog);

-- Medallion + support schemas
CREATE SCHEMA IF NOT EXISTS landing    COMMENT 'Raw file drop zone (UC Volumes) for ERP extracts';
CREATE SCHEMA IF NOT EXISTS bronze     COMMENT 'Raw, append-only, schema-on-read ingest (Auto Loader)';
CREATE SCHEMA IF NOT EXISTS silver     COMMENT 'Cleansed, conformed, de-duplicated, CDC-applied entities';
CREATE SCHEMA IF NOT EXISTS gold       COMMENT 'Business-ready marts consumed by Genie, dashboards and the portal';
CREATE SCHEMA IF NOT EXISTS governance COMMENT 'Masking functions, row filters, mapping and audit tables';
CREATE SCHEMA IF NOT EXISTS ops        COMMENT 'Data quality reports, KPI snapshots, alerts';

CREATE VOLUME IF NOT EXISTS landing.raw_files
  COMMENT 'Landing volume: one folder per ERP entity (patients/, encounters/, claims/, inventory/, staff/)';

-- Ops tables written by job tasks
CREATE TABLE IF NOT EXISTS ops.dq_runs (
  run_ts             TIMESTAMP,
  dataset            STRING,
  expectation        STRING,
  passed_records     BIGINT,
  failed_records     BIGINT
) COMMENT 'Expectation results harvested from the SDP event log';

CREATE TABLE IF NOT EXISTS ops.facility_kpi_snapshots (
  snapshot_date      DATE,
  facility_id        STRING,
  active_inpatients  BIGINT,
  avg_los_days       DOUBLE,
  revenue_inr        DECIMAL(18,2),
  denial_rate_pct    DOUBLE,
  low_stock_items    BIGINT
) COMMENT 'Daily per-facility KPI snapshot (written by for_each job task)';

CREATE TABLE IF NOT EXISTS ops.alerts (
  alert_ts  TIMESTAMP,
  severity  STRING,
  message   STRING
) COMMENT 'Quality / operational alerts raised by the job';

-- Mapping table that drives the facility row filter
CREATE TABLE IF NOT EXISTS governance.facility_access (
  group_name   STRING COMMENT 'Workspace/account group',
  facility_id  STRING COMMENT 'Facility the group may see; * = all'
) COMMENT 'Row-level security map: which group can see which facility';

MERGE INTO governance.facility_access t
USING (SELECT * FROM VALUES
         ('hc_admins',          '*'),
         ('hc_compliance',      '*'),
         ('hc_finance',         '*'),
         ('hc_clinicians_chn',  'FAC-CHN'),
         ('hc_clinicians_blr',  'FAC-BLR'),
         ('hc_clinicians_hyd',  'FAC-HYD'),
         ('hc_clinicians_mum',  'FAC-MUM')
       AS s(group_name, facility_id)) s
ON  t.group_name = s.group_name AND t.facility_id = s.facility_id
WHEN NOT MATCHED THEN INSERT *;
