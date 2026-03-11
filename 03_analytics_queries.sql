-- =============================================================
-- SmartMFG Machine Scheduling Optimization - Analytics Queries
-- File   : 03_analytics_queries.sql
-- Catalog: satsen_catalog
-- Schema : smartmfg_machine_optimization_1
-- =============================================================
-- A curated set of Gold-layer SQL queries for:
--   1. Machine Utilization
--   2. Scheduling Performance (OTD, Farm-Out, Lateness)
--   3. Predictive Maintenance & Machine Health
--   4. Capacity Planning
--   5. Farm-Out vs Make Analysis
-- =============================================================

USE CATALOG satsen_catalog;
USE SCHEMA   smartmfg_machine_optimization_1;


-- ─────────────────────────────────────────────────────────────
-- SECTION 1 — Machine Utilization
-- ─────────────────────────────────────────────────────────────

-- Q1.1: Top 5 most utilized machines (monthly average)
SELECT
  machine_id,
  ROUND(AVG(utilization_pct), 1)  AS avg_monthly_utilization_pct,
  ROUND(AVG(efficiency_pct),  1)  AS avg_efficiency_pct,
  ROUND(SUM(revenue_usd),     2)  AS total_revenue_usd,
  SUM(work_order_count)           AS total_work_orders
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_machine_utilization
GROUP BY machine_id
ORDER BY avg_monthly_utilization_pct DESC
LIMIT 5;

-- ─────────────────────────────────────────────────────────────

-- Q1.2: Monthly utilization trend per machine
SELECT
  DATE_FORMAT(period_month, 'yyyy-MM')  AS month,
  machine_id,
  ROUND(utilization_pct, 1)             AS utilization_pct,
  ROUND(efficiency_pct,  1)             AS efficiency_pct,
  work_order_count,
  ROUND(revenue_usd, 2)                 AS revenue_usd
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_machine_utilization
ORDER BY period_month, machine_id;

-- ─────────────────────────────────────────────────────────────

-- Q1.3: Yearly utilization summary (compare year-over-year)
SELECT
  period_year,
  machine_id,
  ROUND(AVG(utilization_pct), 1) AS avg_utilization_pct,
  ROUND(SUM(revenue_usd),     0) AS total_revenue_usd,
  SUM(work_order_count)          AS total_work_orders
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_machine_utilization
GROUP BY period_year, machine_id
ORDER BY period_year DESC, avg_utilization_pct DESC;


-- ─────────────────────────────────────────────────────────────
-- SECTION 2 — Scheduling Performance
-- ─────────────────────────────────────────────────────────────

-- Q2.1: Monthly on-time delivery (OTD) by work center
SELECT
  DATE_FORMAT(period_month, 'yyyy-MM') AS month,
  work_center,
  total_orders,
  completed_count,
  late_count,
  ROUND(on_time_delivery_pct, 1)       AS otd_pct,
  ROUND(total_farm_out_cost, 2)        AS farm_out_cost_usd
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_scheduling_performance
ORDER BY period_month DESC, work_center;

-- ─────────────────────────────────────────────────────────────

-- Q2.2: Weekly scheduling pressure (high priority late orders)
SELECT
  DATE_FORMAT(period_week, 'yyyy-[W]ww') AS week,
  work_center,
  priority,
  total_orders,
  late_count,
  farm_out_count,
  ROUND(total_farm_out_cost, 2)           AS farm_out_cost_usd,
  ROUND(on_time_delivery_pct, 1)          AS otd_pct
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_scheduling_performance
WHERE priority = 'High'
ORDER BY period_week DESC;

-- ─────────────────────────────────────────────────────────────

-- Q2.3: Yearly scheduling summary (executive view)
SELECT
  period_year,
  SUM(total_orders)                         AS total_orders,
  SUM(completed_count)                      AS total_completed,
  SUM(late_count)                           AS total_late,
  SUM(farm_out_count)                       AS total_farm_out,
  ROUND(SUM(total_farm_out_cost),    0)     AS total_farm_out_cost_usd,
  ROUND(AVG(on_time_delivery_pct),   1)     AS avg_otd_pct,
  ROUND(SUM(total_standard_hrs),     0)     AS total_standard_hrs
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_scheduling_performance
GROUP BY period_year
ORDER BY period_year DESC;


-- ─────────────────────────────────────────────────────────────
-- SECTION 3 — Predictive Maintenance & Machine Health
-- ─────────────────────────────────────────────────────────────

-- Q3.1: Current machine health snapshot (latest date per machine)
WITH latest AS (
  SELECT machine_id, MAX(reading_date) AS latest_date
  FROM satsen_catalog.smartmfg_machine_optimization_1.gold_predictive_maintenance
  GROUP BY machine_id
)
SELECT
  p.machine_id,
  p.machine_name,
  p.machine_type,
  p.work_center,
  p.reading_date                  AS last_reading_date,
  p.avg_health_score,
  p.min_health_score,
  p.max_tool_wear_pct,
  p.anomaly_count,
  p.maintenance_urgency,
  p.days_since_last_pm
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_predictive_maintenance p
JOIN latest l ON p.machine_id = l.machine_id AND p.reading_date = l.latest_date
ORDER BY
  CASE p.maintenance_urgency WHEN 'Critical' THEN 1 WHEN 'Warning' THEN 2 ELSE 3 END,
  p.avg_health_score;

-- ─────────────────────────────────────────────────────────────

-- Q3.2: Weekly health trend — identify degrading machines
SELECT
  machine_id,
  machine_name,
  reading_week,
  reading_year,
  ROUND(avg_health_score, 1)   AS avg_health_score,
  ROUND(avg_vibration,    3)   AS avg_vibration_mm_s,
  ROUND(avg_temp_c,       1)   AS avg_temp_celsius,
  ROUND(max_tool_wear_pct,1)   AS max_tool_wear_pct,
  anomaly_count,
  maintenance_urgency
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_predictive_maintenance
ORDER BY machine_id, reading_year, reading_week;

-- ─────────────────────────────────────────────────────────────

-- Q3.3: Machines due for preventive maintenance (days since last PM > threshold)
SELECT
  machine_id,
  machine_name,
  machine_type,
  work_center,
  MAX(days_since_last_pm)      AS days_since_last_pm,
  ROUND(AVG(avg_health_score), 1) AS recent_avg_health,
  SUM(anomaly_count)           AS total_anomalies_30d,
  MAX(max_tool_wear_pct)       AS peak_tool_wear_pct
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_predictive_maintenance
GROUP BY 1,2,3,4
HAVING MAX(days_since_last_pm) > 180
ORDER BY MAX(days_since_last_pm) DESC;


-- ─────────────────────────────────────────────────────────────
-- SECTION 4 — Capacity Planning
-- ─────────────────────────────────────────────────────────────

-- Q4.1: Monthly capacity utilization by work center
SELECT
  DATE_FORMAT(period_month, 'yyyy-MM') AS month,
  work_center,
  demand_hrs,
  available_hrs,
  farm_out_hrs,
  ROUND(farm_out_cost_usd, 2)          AS farm_out_cost_usd,
  machine_count,
  ROUND(capacity_utilization_pct, 1)   AS capacity_utilization_pct,
  ROUND(farm_out_rate_pct, 1)          AS farm_out_rate_pct,
  load_status
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_capacity_planning
ORDER BY period_month DESC, capacity_utilization_pct DESC;

-- ─────────────────────────────────────────────────────────────

-- Q4.2: Yearly capacity summary — investment decision support
SELECT
  period_year,
  work_center,
  ROUND(SUM(demand_hrs), 0)              AS total_demand_hrs,
  ROUND(AVG(available_hrs), 0)           AS avg_available_hrs_per_month,
  ROUND(SUM(farm_out_hrs), 0)            AS total_farm_out_hrs,
  ROUND(SUM(farm_out_cost_usd), 0)       AS total_farm_out_cost,
  ROUND(AVG(capacity_utilization_pct),1) AS avg_utilization_pct,
  ROUND(AVG(farm_out_rate_pct), 1)       AS avg_farm_out_rate_pct
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_capacity_planning
GROUP BY period_year, work_center
ORDER BY period_year DESC, total_farm_out_cost DESC;

-- ─────────────────────────────────────────────────────────────

-- Q4.3: Overloaded work centers (candidates for capacity investment)
SELECT
  work_center,
  COUNT(DISTINCT period_month)           AS months_overloaded,
  ROUND(AVG(capacity_utilization_pct),1) AS avg_util_pct,
  ROUND(SUM(farm_out_cost_usd), 0)       AS total_farm_out_cost,
  ROUND(SUM(demand_hrs) - SUM(available_hrs), 0) AS total_hrs_over_capacity
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_capacity_planning
WHERE load_status IN ('Overloaded','High Load')
GROUP BY work_center
ORDER BY months_overloaded DESC, total_farm_out_cost DESC;


-- ─────────────────────────────────────────────────────────────
-- SECTION 5 — Farm-Out vs Make Analysis
-- ─────────────────────────────────────────────────────────────

-- Q5.1: Farm-out vendor performance — cost vs implied internal cost
SELECT
  farm_out_vendor,
  SUM(farm_out_orders)                  AS total_orders,
  SUM(total_qty_farmed)                 AS total_qty,
  ROUND(SUM(total_farm_out_cost),   0)  AS total_farm_out_cost,
  ROUND(SUM(implied_internal_cost), 0)  AS total_internal_equivalent_cost,
  ROUND(AVG(cost_premium_pct),      1)  AS avg_cost_premium_pct
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_farmout_analysis
GROUP BY farm_out_vendor
ORDER BY total_farm_out_cost DESC;

-- ─────────────────────────────────────────────────────────────

-- Q5.2: Monthly farm-out cost trend (are we reducing external machining costs?)
SELECT
  DATE_FORMAT(period_month, 'yyyy-MM')  AS month,
  ROUND(SUM(total_farm_out_cost),   0)  AS total_farm_out_cost,
  ROUND(SUM(implied_internal_cost), 0)  AS implied_internal_cost,
  SUM(farm_out_orders)                  AS total_farm_out_orders,
  SUM(total_qty_farmed)                 AS total_qty_farmed
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_farmout_analysis
GROUP BY period_month
ORDER BY period_month;

-- ─────────────────────────────────────────────────────────────

-- Q5.3: Parts with highest farm-out dependency (candidates for in-house investment)
SELECT
  part_number,
  part_description,
  SUM(farm_out_orders)                 AS total_farm_out_orders,
  ROUND(SUM(total_farm_out_cost),  0)  AS total_farm_out_cost,
  ROUND(AVG(cost_premium_pct),     1)  AS avg_premium_pct,
  ROUND(SUM(equivalent_internal_hrs),1) AS total_internal_hrs_equivalent
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_farmout_analysis
GROUP BY part_number, part_description
ORDER BY total_farm_out_cost DESC
LIMIT 10;
