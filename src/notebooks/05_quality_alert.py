"""
05_quality_alert.py — only runs when the condition task finds the failed-record %
above threshold. Logs an alert row (hook Slack/Teams/email webhooks in here).
"""
import argparse

from databricks.sdk.runtime import spark

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
args = parser.parse_args()

spark.sql(f"""
  INSERT INTO {args.catalog}.ops.alerts
  SELECT current_timestamp(), 'HIGH',
         concat('Data quality breach: ', sum(failed_records), ' rows failed expectations in the last hour')
  FROM {args.catalog}.ops.dq_runs
  WHERE run_ts >= current_timestamp() - INTERVAL 1 HOUR
""")
print("🚨 alert logged to ops.alerts")
