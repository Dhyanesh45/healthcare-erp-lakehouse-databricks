"""
04_facility_snapshot.py — runs once PER FACILITY inside a for_each job task.
Writes a daily KPI snapshot row for the facility it was given.
"""
import argparse

from databricks.sdk.runtime import spark

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--facility", required=True)
args = parser.parse_args()
C, F = args.catalog, args.facility

spark.sql(f"DELETE FROM {C}.ops.facility_kpi_snapshots WHERE snapshot_date = current_date() AND facility_id = '{F}'")
spark.sql(f"""
  INSERT INTO {C}.ops.facility_kpi_snapshots
  SELECT current_date(),
         '{F}',
         (SELECT count_if(is_active_inpatient) FROM {C}.gold.fact_encounters WHERE facility_id = '{F}'),
         (SELECT round(avg(los_days),2) FROM {C}.gold.fact_encounters
            WHERE facility_id = '{F}' AND encounter_type = 'INPATIENT'),
         (SELECT sum(total_charges_inr) FROM {C}.gold.fact_encounters WHERE facility_id = '{F}'),
         (SELECT round(100.0 * count_if(c.claim_status = 'DENIED') / count(*), 2)
            FROM {C}.silver.claims c JOIN {C}.silver.encounters e USING (encounter_id)
            WHERE e.facility_id = '{F}'),
         (SELECT count_if(is_low_stock) FROM {C}.gold.inventory_stock_position WHERE facility_id = '{F}')
""")
print(f"✅ snapshot written for {F}")
