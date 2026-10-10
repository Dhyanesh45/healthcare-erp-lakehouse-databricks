-- Title: Average length of stay by facility (last 30 days)
SELECT facility_id, round(avg(avg_los_days), 1) AS avg_los_days
FROM gold.facility_daily_kpis
WHERE kpi_date >= current_date() - INTERVAL 30 DAYS
GROUP BY facility_id
ORDER BY avg_los_days DESC;

-- Title: Payer denial rate and top denial reason this quarter
SELECT payer,
       round(100.0 * sum(claims * denial_rate_pct / 100) / sum(claims), 1) AS denial_rate_pct,
       max_by(top_denial_reason, claims)                                    AS top_denial_reason
FROM gold.revenue_cycle_by_payer
WHERE claim_month >= date_trunc('QUARTER', current_date())
GROUP BY payer
ORDER BY denial_rate_pct DESC;

-- Title: Daily ER visits for a facility
SELECT kpi_date, er_visits
FROM gold.facility_daily_kpis
WHERE facility_id = 'FAC-CHN' AND kpi_date >= current_date() - INTERVAL 30 DAYS
ORDER BY kpi_date;

-- Title: Low-stock items by facility
SELECT facility_id, item_name, stock_on_hand, reorder_level
FROM gold.inventory_stock_position
WHERE is_low_stock
ORDER BY facility_id, stock_on_hand;

-- Title: Revenue per doctor by department
SELECT department, sum(revenue_inr) / nullif(sum(doctors), 0) AS revenue_per_doctor_inr
FROM gold.department_performance
GROUP BY department
ORDER BY revenue_per_doctor_inr DESC;
