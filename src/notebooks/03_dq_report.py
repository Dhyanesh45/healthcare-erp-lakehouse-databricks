"""
03_dq_report.py — reads the SDP pipeline event log, stores expectation results in
ops.dq_runs, and publishes a task value (failed_record_pct) that the job's
condition task uses to decide whether to raise an alert.
"""
import argparse

from databricks.sdk.runtime import dbutils, spark

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--los-threshold", type=float, default=6.0)
args = parser.parse_args()
C = args.catalog

# event_log() table-valued function works on any streaming table / MV of the pipeline
spark.sql(f"""
  INSERT INTO {C}.ops.dq_runs
  SELECT current_timestamp(),
         e.dataset,
         e.name,
         e.passed_records,
         e.failed_records
  FROM (
    SELECT explode(from_json(
             details:flow_progress:data_quality:expectations,
             'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
    FROM event_log(TABLE({C}.silver.encounters))
    WHERE event_type = 'flow_progress'
      AND details:flow_progress:data_quality:expectations IS NOT NULL
      AND timestamp >= current_timestamp() - INTERVAL 1 DAY
  )
""")

stats = spark.sql(f"""
  SELECT coalesce(sum(failed_records), 0) AS failed,
         coalesce(sum(passed_records + failed_records), 0) AS total
  FROM {C}.ops.dq_runs
  WHERE run_ts >= current_timestamp() - INTERVAL 1 HOUR
""").first()

pct = round(100.0 * stats.failed / stats.total, 2) if stats.total else 0.0
avg_los = spark.sql(f"SELECT round(avg(avg_los_days),2) FROM {C}.gold.facility_daily_kpis").first()[0] or 0.0

print(f"failed_record_pct={pct}  avg_los={avg_los}")
dbutils.jobs.taskValues.set(key="failed_record_pct", value=pct)
dbutils.jobs.taskValues.set(key="avg_los_days", value=float(avg_los))
dbutils.jobs.taskValues.set(key="los_breach", value=float(avg_los) > args.los_threshold)
