# Genie Space — "MediCore Hospital Analyst"

Genie lets business users ask questions in plain English ("Which payer denied the most
claims last month?") and get SQL + charts back. It runs on the **gold** layer, which is
de-identified, and it always queries **as the person asking** — so facility row filters
and masks still apply.

## Create it (UI — about 5 minutes)
1. Left sidebar → **Genie** → **+ New**.
2. **Data**: pick these tables from `<catalog>.gold`:
   - `facility_daily_kpis`
   - `revenue_cycle_by_payer`
   - `inventory_stock_position`
   - `department_performance`
   - `readmission_by_diagnosis`
   - `fact_encounters`
3. **SQL warehouse**: the same warehouse you put in `databricks.yml`.
4. **Title**: `MediCore Hospital Analyst`
5. **Instructions** → paste the contents of `instructions.md`.
6. **Example SQL queries** → add each block from `sample_queries.sql`
   (give each one the title written in its comment).
7. Save. Copy the **space ID** from the URL:
   `https://<workspace>/genie/rooms/<THIS_PART_IS_THE_SPACE_ID>`
8. Share it with groups `hc_analysts`, `hc_finance`, `hc_compliance` (Can Run).
9. Paste the space ID into `resources/app.yml` (uncomment the genie block) and into
   `app/app.yaml` (`GENIE_SPACE_ID`), then redeploy.

## Questions to try
- What was the average length of stay per facility last month?
- Which payer has the highest denial rate this quarter, and what is the top denial reason?
- Show daily ER visits in Chennai for the last 30 days as a line chart.
- Which items are low on stock in Bengaluru?
- Which departments earn the most revenue per doctor?
- Which diagnoses have a readmission rate above 10%?
