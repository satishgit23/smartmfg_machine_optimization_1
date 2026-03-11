-- =============================================================
-- SmartMFG Machine Scheduling Optimization - DLT Pipeline
-- File      : 02_dlt_pipeline.sql
-- Pipeline  : smartmfg_machine_opt_pipeline
-- Language  : Spark SQL (Spark Declarative Pipelines / DLT)
-- Catalog   : satsen_catalog
-- Schema    : smartmfg_machine_optimization_1
-- =============================================================
-- ARCHITECTURE: Medallion Pattern
--   Bronze  → raw ingestion from landing volume (CSV)
--   Silver  → cleaned, typed, validated data
--   Gold    → aggregated, business-ready metrics
-- =============================================================
-- EXPECTATION STRATEGY
--   WARN  (no action)            : track violations, keep all rows
--                                  Used for business-rule anomalies that
--                                  should be visible but not removed.
--   DROP ROW                     : track violations, remove bad rows
--                                  Used for records that would corrupt
--                                  downstream joins or aggregations.
--   FAIL UPDATE                  : abort pipeline on first violation
--                                  Used for invariants that must NEVER
--                                  be false in clean source data.
-- =============================================================
-- Pipeline Configuration Parameters (set in pipeline settings):
--   catalog      = satsen_catalog
--   schema       = smartmfg_machine_optimization_1
--   volume_path  = /Volumes/satsen_catalog/smartmfg_machine_optimization_1/landing_zone
-- =============================================================


-- ─────────────────────────────────────────────────────────────
-- BRONZE LAYER — Raw ingestion (append-only streaming tables)
--   Expectations here are WARN-only: the raw layer is an
--   exact copy of the source; we observe quality but never
--   drop or fail on unvalidated data.
-- ─────────────────────────────────────────────────────────────

-- Bronze: Machines Master Data
CREATE OR REFRESH STREAMING TABLE bronze_machines
(
  -- Format guards (warn-only: raw data should not be dropped)
  CONSTRAINT bz_machine_id_not_null   EXPECT (machine_id IS NOT NULL),
  CONSTRAINT bz_machine_id_format     EXPECT (machine_id RLIKE '^MCH-[0-9]+'),
  CONSTRAINT bz_status_not_null       EXPECT (status IS NOT NULL),
  CONSTRAINT bz_hourly_rate_positive  EXPECT (CAST(hourly_rate_usd AS DOUBLE) > 0)
)
CLUSTER BY (machine_id)
COMMENT "Raw machine master data ingested from Oracle ERP CSV export"
AS
SELECT
  *,
  current_timestamp()         AS _ingested_at,
  _metadata.file_path         AS _source_file,
  _metadata.file_modification_time AS _file_ts
FROM STREAM read_files(
  '/Volumes/satsen_catalog/smartmfg_machine_optimization_1/landing_zone/machines/',
  format           => 'csv',
  header           => 'true',
  inferSchema      => 'true'
);

-- ─────────────────────────────────────────────────────────────

-- Bronze: Routings (Part-Operation Sequences)
CREATE OR REFRESH STREAMING TABLE bronze_routings
(
  CONSTRAINT bz_routing_id_not_null   EXPECT (routing_id IS NOT NULL),
  CONSTRAINT bz_part_number_not_null  EXPECT (part_number IS NOT NULL),
  CONSTRAINT bz_op_sequence_positive  EXPECT (CAST(operation_sequence AS INT) > 0),
  CONSTRAINT bz_run_time_non_negative EXPECT (CAST(run_time_hrs_per_unit AS DOUBLE) >= 0)
)
CLUSTER BY (part_number)
COMMENT "Raw part routing and operation sequence data from engineering system"
AS
SELECT
  *,
  current_timestamp()         AS _ingested_at,
  _metadata.file_path         AS _source_file
FROM STREAM read_files(
  '/Volumes/satsen_catalog/smartmfg_machine_optimization_1/landing_zone/routings/',
  format      => 'csv',
  header      => 'true',
  inferSchema => 'true'
);

-- ─────────────────────────────────────────────────────────────

-- Bronze: Work Orders (from Oracle ERP)
CREATE OR REFRESH STREAMING TABLE bronze_work_orders
(
  CONSTRAINT bz_wo_id_not_null        EXPECT (work_order_id IS NOT NULL),
  CONSTRAINT bz_wo_part_not_null      EXPECT (part_number IS NOT NULL),
  CONSTRAINT bz_wo_qty_positive       EXPECT (CAST(order_qty AS INT) > 0),
  CONSTRAINT bz_wo_status_not_null    EXPECT (status IS NOT NULL),
  CONSTRAINT bz_wo_due_date_not_null  EXPECT (due_date IS NOT NULL)
)
CLUSTER BY (work_order_id)
COMMENT "Raw work order data exported from Oracle ERP"
AS
SELECT
  *,
  current_timestamp()         AS _ingested_at,
  _metadata.file_path         AS _source_file
FROM STREAM read_files(
  '/Volumes/satsen_catalog/smartmfg_machine_optimization_1/landing_zone/work_orders/',
  format      => 'csv',
  header      => 'true',
  inferSchema => 'true'
);

-- ─────────────────────────────────────────────────────────────

-- Bronze: Machine Sensor Data (IoT / Shop-Floor Systems)
CREATE OR REFRESH STREAMING TABLE bronze_sensor_data
(
  CONSTRAINT bz_sensor_id_not_null    EXPECT (sensor_id IS NOT NULL),
  CONSTRAINT bz_sensor_machine_id     EXPECT (machine_id IS NOT NULL),
  CONSTRAINT bz_temp_parseable        EXPECT (CAST(temperature_celsius AS DOUBLE) IS NOT NULL),
  CONSTRAINT bz_ts_not_null           EXPECT (reading_timestamp IS NOT NULL),
  -- Any row without a machine ID is unroutable and must not enter the lake
  CONSTRAINT bz_machine_ref_required  EXPECT (machine_id IS NOT NULL) ON VIOLATION DROP ROW
)
CLUSTER BY (machine_id)
COMMENT "Raw IoT sensor data from shop-floor connected machines"
AS
SELECT
  *,
  current_timestamp()         AS _ingested_at,
  _metadata.file_path         AS _source_file
FROM STREAM read_files(
  '/Volumes/satsen_catalog/smartmfg_machine_optimization_1/landing_zone/sensor_data/',
  format      => 'csv',
  header      => 'true',
  inferSchema => 'true'
);


-- ─────────────────────────────────────────────────────────────
-- SILVER LAYER — Cleaned, typed, validated streaming tables
--   Richer set of expectations across all three violation modes.
-- ─────────────────────────────────────────────────────────────

-- Silver: Machines
CREATE OR REFRESH STREAMING TABLE silver_machines
(
  -- ── FAIL UPDATE ─────────────────────────────────────────────
  -- machine_id is the primary key for every downstream join;
  -- a NULL here indicates a pipeline or source-feed bug.
  CONSTRAINT sv_machine_pk_never_null
    EXPECT (machine_id IS NOT NULL)
    ON VIOLATION FAIL UPDATE,

  -- ── DROP ROW ────────────────────────────────────────────────
  -- Records with no capacity are meaningless for utilisation math.
  CONSTRAINT sv_machine_capacity_positive
    EXPECT (capacity_hrs_per_shift > 0)
    ON VIOLATION DROP ROW,
  -- Negative hourly rates would corrupt cost calculations.
  CONSTRAINT sv_hourly_rate_positive
    EXPECT (hourly_rate_usd > 0)
    ON VIOLATION DROP ROW,

  -- ── WARN (no action) ────────────────────────────────────────
  -- Standard allowed statuses
  CONSTRAINT sv_machine_status_valid
    EXPECT (status IN ('Active','Maintenance','Idle')),
  -- A shift count outside 1–3 is unusual; flag for review.
  CONSTRAINT sv_shifts_in_range
    EXPECT (num_shifts BETWEEN 1 AND 3),
  -- Capacity per shift is typically 6–10 hrs; extremes get flagged.
  CONSTRAINT sv_capacity_shift_range
    EXPECT (capacity_hrs_per_shift BETWEEN 4 AND 12),
  -- Machines installed before 2000 may need replacement planning.
  CONSTRAINT sv_installation_year_recent
    EXPECT (installation_year >= 2000),
  -- PM interval should be set to a meaningful value.
  CONSTRAINT sv_pm_interval_reasonable
    EXPECT (preventive_maint_interval_hrs BETWEEN 100 AND 3000),
  -- Last PM date must be set; missing = maintenance gap.
  CONSTRAINT sv_last_pm_date_set
    EXPECT (last_pm_date IS NOT NULL)
)
CLUSTER BY (machine_id)
COMMENT "Cleaned and typed machine master data with quality constraints"
AS
SELECT
  machine_id,
  machine_name,
  machine_type,
  work_center,
  CAST(num_shifts              AS INT)    AS num_shifts,
  CAST(capacity_hrs_per_shift  AS DOUBLE) AS capacity_hrs_per_shift,
  vendor,
  CAST(installation_year       AS INT)    AS installation_year,
  status,
  CAST(hourly_rate_usd         AS DOUBLE) AS hourly_rate_usd,
  CAST(preventive_maint_interval_hrs AS INT) AS preventive_maint_interval_hrs,
  TO_DATE(last_pm_date, 'yyyy-MM-dd')     AS last_pm_date,
  notes,
  -- Computed fields
  (CAST(num_shifts AS INT) * CAST(capacity_hrs_per_shift AS DOUBLE)) AS capacity_hrs_per_day,
  _ingested_at,
  _source_file
FROM STREAM(LIVE.bronze_machines)
WHERE machine_id IS NOT NULL;

-- ─────────────────────────────────────────────────────────────

-- Silver: Routings
CREATE OR REFRESH STREAMING TABLE silver_routings
(
  -- ── FAIL UPDATE ─────────────────────────────────────────────
  -- routing_id + part_number together identify a routing record;
  -- either being NULL breaks all downstream part-machine joins.
  CONSTRAINT sv_routing_pk_never_null
    EXPECT (routing_id IS NOT NULL AND part_number IS NOT NULL)
    ON VIOLATION FAIL UPDATE,

  -- ── DROP ROW ────────────────────────────────────────────────
  -- Operation sequence ≤ 0 is logically impossible; drop it.
  CONSTRAINT sv_routing_seq_positive
    EXPECT (operation_sequence > 0)
    ON VIOLATION DROP ROW,
  -- Negative scrap rates are data-entry errors.
  CONSTRAINT sv_scrap_rate_non_negative
    EXPECT (scrap_rate_pct >= 0)
    ON VIOLATION DROP ROW,

  -- ── WARN (no action) ────────────────────────────────────────
  -- Setup time of 0 is unusual but not impossible (minor ops).
  CONSTRAINT sv_setup_time_non_negative
    EXPECT (setup_time_hrs >= 0),
  -- Run time > 0 is expected for meaningful operations.
  CONSTRAINT sv_run_time_positive
    EXPECT (run_time_hrs_per_unit > 0),
  -- Scrap rates above 30% are operationally alarming.
  CONSTRAINT sv_scrap_rate_reasonable
    EXPECT (scrap_rate_pct BETWEEN 0 AND 30),
  -- Every routing should map to a work centre.
  CONSTRAINT sv_routing_work_center_set
    EXPECT (work_center IS NOT NULL)
)
CLUSTER BY (part_number)
COMMENT "Cleaned routing and operation data with type-safe columns"
AS
SELECT
  routing_id,
  part_number,
  part_description,
  CAST(operation_sequence AS INT)       AS operation_sequence,
  operation_code,
  operation_description,
  work_center,
  preferred_machine_id,
  CAST(setup_time_hrs          AS DOUBLE) AS setup_time_hrs,
  CAST(run_time_hrs_per_unit   AS DOUBLE) AS run_time_hrs_per_unit,
  CAST(scrap_rate_pct          AS DOUBLE) AS scrap_rate_pct,
  _ingested_at,
  _source_file
FROM STREAM(LIVE.bronze_routings)
WHERE routing_id IS NOT NULL
  AND part_number IS NOT NULL;

-- ─────────────────────────────────────────────────────────────

-- Silver: Work Orders
CREATE OR REFRESH STREAMING TABLE silver_work_orders
(
  -- ── FAIL UPDATE ─────────────────────────────────────────────
  -- A work order with no ID or part number cannot be traced
  -- back to the source system — treat as a critical feed error.
  CONSTRAINT sv_wo_pk_never_null
    EXPECT (work_order_id IS NOT NULL AND part_number IS NOT NULL)
    ON VIOLATION FAIL UPDATE,

  -- ── DROP ROW ────────────────────────────────────────────────
  -- Zero or negative quantity makes hours calculations invalid.
  CONSTRAINT sv_wo_qty_positive
    EXPECT (order_qty > 0)
    ON VIOLATION DROP ROW,
  -- A negative farm-out cost is a data error; remove the record.
  CONSTRAINT sv_farmout_cost_non_negative
    EXPECT (farm_out_cost_usd IS NULL OR farm_out_cost_usd >= 0)
    ON VIOLATION DROP ROW,
  -- Setup time cannot be negative.
  CONSTRAINT sv_setup_time_non_negative
    EXPECT (setup_time_hrs >= 0)
    ON VIOLATION DROP ROW,

  -- ── WARN (no action) ────────────────────────────────────────
  -- Allowed order statuses; others are flagged for triage.
  CONSTRAINT sv_wo_status_valid
    EXPECT (status IN ('Open','In Progress','Complete','Farm-Out')),
  -- Only three priority levels are recognised.
  CONSTRAINT sv_wo_priority_valid
    EXPECT (priority IN ('High','Medium','Low')),
  -- Recognised work centres; new ones should be registered first.
  CONSTRAINT sv_wo_work_center_known
    EXPECT (work_center IN ('WC-MILL','WC-TURN','WC-DRILL',
                             'WC-MULTITASK','WC-TRANSFER','WC-QC')),
  -- Due date must be set for scheduling.
  CONSTRAINT sv_wo_due_date_set
    EXPECT (due_date IS NOT NULL),
  -- Scheduled start must precede scheduled end.
  CONSTRAINT sv_wo_sched_dates_ordered
    EXPECT (scheduled_start IS NULL
         OR scheduled_end   IS NULL
         OR scheduled_start <= scheduled_end),
  -- Farm-out orders should always carry a vendor name.
  CONSTRAINT sv_farmout_has_vendor
    EXPECT (status != 'Farm-Out' OR farm_out_vendor IS NOT NULL),
  -- Completed orders are expected to have an actual end timestamp.
  CONSTRAINT sv_complete_has_actual_end
    EXPECT (status != 'Complete' OR actual_end IS NOT NULL),
  -- Run time per unit should be positive for production operations.
  CONSTRAINT sv_run_time_positive
    EXPECT (run_time_hrs_per_unit > 0)
)
CLUSTER BY (work_order_id, machine_id)
COMMENT "Cleaned, typed work orders with derived scheduling metrics"
AS
SELECT
  work_order_id,
  part_number,
  part_description,
  CAST(order_qty               AS INT)     AS order_qty,
  operation_code,
  operation_description,
  work_center,
  machine_id,
  TO_DATE(due_date,         'yyyy-MM-dd')  AS due_date,
  TO_TIMESTAMP(scheduled_start, 'yyyy-MM-dd') AS scheduled_start,
  TO_TIMESTAMP(scheduled_end,   'yyyy-MM-dd') AS scheduled_end,
  TO_TIMESTAMP(actual_start,    'yyyy-MM-dd') AS actual_start,
  TO_TIMESTAMP(actual_end,      'yyyy-MM-dd') AS actual_end,
  CAST(setup_time_hrs          AS DOUBLE)  AS setup_time_hrs,
  CAST(run_time_hrs_per_unit   AS DOUBLE)  AS run_time_hrs_per_unit,
  CAST(actual_total_hrs        AS DOUBLE)  AS actual_total_hrs,
  status,
  priority,
  farm_out_vendor,
  CAST(farm_out_cost_usd       AS DOUBLE)  AS farm_out_cost_usd,
  source_system,
  -- Derived columns
  CASE WHEN status = 'Farm-Out' THEN TRUE ELSE FALSE END  AS is_farm_out,
  CASE WHEN status = 'Complete'
       AND actual_end IS NOT NULL
       AND TO_TIMESTAMP(actual_end, 'yyyy-MM-dd') > TO_DATE(due_date, 'yyyy-MM-dd')
       THEN TRUE ELSE FALSE END                           AS is_late,
  ROUND(
    CAST(setup_time_hrs AS DOUBLE)
    + CAST(run_time_hrs_per_unit AS DOUBLE) * CAST(order_qty AS INT), 2
  )                                                       AS standard_total_hrs,
  _ingested_at,
  _source_file
FROM STREAM(LIVE.bronze_work_orders)
WHERE work_order_id IS NOT NULL
  AND part_number   IS NOT NULL;

-- ─────────────────────────────────────────────────────────────

-- Silver: Machine Sensor Data
CREATE OR REFRESH STREAMING TABLE silver_sensor_data
(
  -- ── FAIL UPDATE ─────────────────────────────────────────────
  -- A sensor reading with no machine reference is unroutable
  -- and indicates a feed misconfiguration, not dirty data.
  CONSTRAINT sv_sensor_machine_never_null
    EXPECT (machine_id IS NOT NULL)
    ON VIOLATION FAIL UPDATE,

  -- ── DROP ROW ────────────────────────────────────────────────
  -- Sensor ID must be present to de-duplicate readings.
  CONSTRAINT sv_sensor_id_required
    EXPECT (sensor_id IS NOT NULL)
    ON VIOLATION DROP ROW,
  -- Temperature above 130 °C is physically implausible for our
  -- machines; likely a sensor fault — remove to protect aggregates.
  CONSTRAINT sv_temp_not_extreme
    EXPECT (temperature_celsius < 130)
    ON VIOLATION DROP ROW,
  -- Vibration above 8 mm/s indicates a sensor malfunction or
  -- catastrophic fault; exclude from trend calculations.
  CONSTRAINT sv_vibration_not_extreme
    EXPECT (vibration_mm_s < 8.0)
    ON VIOLATION DROP ROW,
  -- Tool wear cannot exceed 100 % — clamp logic in generator
  -- should prevent this, but guard downstream calculations.
  CONSTRAINT sv_tool_wear_max_100
    EXPECT (tool_wear_pct <= 100)
    ON VIOLATION DROP ROW,

  -- ── WARN (no action) ────────────────────────────────────────
  -- Operational temperature alert threshold (anomaly band).
  CONSTRAINT sv_temp_operational_range
    EXPECT (temperature_celsius BETWEEN 15 AND 95),
  -- Vibration warning threshold per maintenance spec.
  CONSTRAINT sv_vibration_warning_limit
    EXPECT (vibration_mm_s <= 5.0),
  -- Spindle speed below 100 rpm while running may indicate a stall.
  CONSTRAINT sv_spindle_speed_reasonable
    EXPECT (spindle_speed_rpm >= 0),
  -- Power consumption must be non-negative.
  CONSTRAINT sv_power_non_negative
    EXPECT (power_consumption_kw >= 0),
  -- Coolant flow below 1 L/min while machine is running is a warning.
  CONSTRAINT sv_coolant_flow_running
    EXPECT (coolant_flow_lpm >= 1.0 OR machine_status != 'Running'),
  -- Tool wear above 80 % triggers a pre-emptive change advisory.
  CONSTRAINT sv_tool_wear_advisory
    EXPECT (tool_wear_pct < 80),
  -- Machine status values must be from the known set.
  CONSTRAINT sv_machine_status_valid
    EXPECT (machine_status IN ('Running','Idle','Maintenance')),
  -- Sensor timestamp must be present to produce time-series metrics.
  CONSTRAINT sv_reading_ts_not_null
    EXPECT (reading_timestamp IS NOT NULL)
)
CLUSTER BY (machine_id, reading_timestamp)
COMMENT "Cleaned sensor readings with anomaly scoring for predictive maintenance"
AS
SELECT
  sensor_id,
  machine_id,
  TO_TIMESTAMP(reading_timestamp, 'yyyy-MM-dd HH:mm:ss') AS reading_timestamp,
  CAST(temperature_celsius  AS DOUBLE) AS temperature_celsius,
  CAST(vibration_mm_s       AS DOUBLE) AS vibration_mm_s,
  CAST(spindle_speed_rpm    AS DOUBLE) AS spindle_speed_rpm,
  CAST(power_consumption_kw AS DOUBLE) AS power_consumption_kw,
  CAST(coolant_flow_lpm     AS DOUBLE) AS coolant_flow_lpm,
  CAST(tool_wear_pct        AS DOUBLE) AS tool_wear_pct,
  machine_status,
  CAST(anomaly_flag         AS BOOLEAN) AS anomaly_flag,
  anomaly_type,
  -- Health score: 100 minus weighted penalty for each anomaly dimension
  ROUND(
    GREATEST(0,
      100
      - CASE WHEN CAST(tool_wear_pct AS DOUBLE) > 80 THEN 30 ELSE 0 END
      - CASE WHEN CAST(vibration_mm_s AS DOUBLE) > 4.0 THEN 25 ELSE 0 END
      - CASE WHEN CAST(temperature_celsius AS DOUBLE) > 80 THEN 20 ELSE 0 END
      - CASE WHEN CAST(coolant_flow_lpm AS DOUBLE) < 3.0 AND machine_status = 'Running' THEN 15 ELSE 0 END
      - CASE WHEN CAST(anomaly_flag AS BOOLEAN) = TRUE THEN 10 ELSE 0 END
    ), 1
  )                                    AS health_score,
  YEAR(TO_TIMESTAMP(reading_timestamp,'yyyy-MM-dd HH:mm:ss'))  AS reading_year,
  MONTH(TO_TIMESTAMP(reading_timestamp,'yyyy-MM-dd HH:mm:ss')) AS reading_month,
  WEEKOFYEAR(TO_TIMESTAMP(reading_timestamp,'yyyy-MM-dd HH:mm:ss')) AS reading_week,
  _ingested_at,
  _source_file
FROM STREAM(LIVE.bronze_sensor_data)
WHERE sensor_id  IS NOT NULL
  AND machine_id IS NOT NULL;


-- ─────────────────────────────────────────────────────────────
-- GOLD LAYER — Business-ready aggregated materialized views
-- ─────────────────────────────────────────────────────────────

-- Gold: Machine Utilization Summary (by week/month/year)
CREATE OR REFRESH MATERIALIZED VIEW gold_machine_utilization
CLUSTER BY (machine_id, period_month)
COMMENT "Machine utilization KPIs aggregated by day, week, month, and year — feeds dashboard"
AS
WITH wo_hours AS (
  SELECT
    machine_id,
    DATE_TRUNC('DAY',   scheduled_start) AS period_day,
    DATE_TRUNC('WEEK',  scheduled_start) AS period_week,
    DATE_TRUNC('MONTH', scheduled_start) AS period_month,
    YEAR(scheduled_start)                AS period_year,
    SUM(standard_total_hrs)              AS scheduled_hrs,
    SUM(actual_total_hrs)                AS actual_hrs,
    COUNT(*)                             AS work_order_count
  FROM LIVE.silver_work_orders
  WHERE machine_id IS NOT NULL
    AND scheduled_start IS NOT NULL
  GROUP BY 1,2,3,4,5
),
machine_capacity AS (
  SELECT machine_id, capacity_hrs_per_day, hourly_rate_usd
  FROM   LIVE.silver_machines
)
SELECT
  w.machine_id,
  m.capacity_hrs_per_day,
  m.hourly_rate_usd,
  w.period_day,
  w.period_week,
  w.period_month,
  w.period_year,
  w.scheduled_hrs,
  COALESCE(w.actual_hrs, 0)            AS actual_hrs,
  w.work_order_count,
  -- Utilization % (scheduled vs total available capacity for that period)
  ROUND(w.scheduled_hrs / NULLIF(m.capacity_hrs_per_day, 0) * 100, 1) AS utilization_pct,
  -- Efficiency % (actual vs scheduled)
  CASE WHEN COALESCE(w.actual_hrs, 0) > 0 AND w.scheduled_hrs > 0
       THEN ROUND(w.scheduled_hrs / w.actual_hrs * 100, 1)
       ELSE NULL END                   AS efficiency_pct,
  -- Revenue contribution
  ROUND(COALESCE(w.actual_hrs, w.scheduled_hrs) * m.hourly_rate_usd, 2) AS revenue_usd
FROM wo_hours w
JOIN machine_capacity m USING (machine_id);

-- ─────────────────────────────────────────────────────────────

-- Gold: Scheduling Performance (on-time delivery, farm-out, late orders)
CREATE OR REFRESH MATERIALIZED VIEW gold_scheduling_performance
CLUSTER BY (period_month)
COMMENT "Weekly/monthly/yearly scheduling KPIs: OTD, farm-out cost, late count"
AS
SELECT
  DATE_TRUNC('WEEK',  due_date)  AS period_week,
  DATE_TRUNC('MONTH', due_date)  AS period_month,
  YEAR(due_date)                  AS period_year,
  work_center,
  priority,
  COUNT(*)                        AS total_orders,
  SUM(CASE WHEN is_farm_out THEN 1 ELSE 0 END) AS farm_out_count,
  SUM(CASE WHEN is_late     THEN 1 ELSE 0 END) AS late_count,
  SUM(CASE WHEN status = 'Complete' THEN 1 ELSE 0 END) AS completed_count,
  ROUND(SUM(COALESCE(farm_out_cost_usd, 0)), 2) AS total_farm_out_cost,
  ROUND(AVG(CASE WHEN status = 'Complete'
                 THEN actual_total_hrs END), 2)  AS avg_actual_hrs,
  ROUND(SUM(standard_total_hrs), 2)             AS total_standard_hrs,
  ROUND(
    SUM(CASE WHEN status = 'Complete' AND NOT is_late THEN 1 ELSE 0 END)
    / NULLIF(SUM(CASE WHEN status = 'Complete' THEN 1 ELSE 0 END), 0) * 100, 1
  )                                              AS on_time_delivery_pct
FROM LIVE.silver_work_orders
WHERE due_date IS NOT NULL
GROUP BY 1,2,3,4,5;

-- ─────────────────────────────────────────────────────────────

-- Gold: Predictive Maintenance — Machine Health Trends
CREATE OR REFRESH MATERIALIZED VIEW gold_predictive_maintenance
CLUSTER BY (machine_id, reading_week)
COMMENT "Hourly-to-daily aggregated sensor health metrics for predictive maintenance model inputs"
AS
SELECT
  s.machine_id,
  m.machine_name,
  m.machine_type,
  m.work_center,
  s.reading_year,
  s.reading_month,
  s.reading_week,
  DATE_TRUNC('DAY', s.reading_timestamp) AS reading_date,
  COUNT(*)                               AS sensor_readings,
  ROUND(AVG(s.temperature_celsius),  2)  AS avg_temp_c,
  ROUND(MAX(s.temperature_celsius),  2)  AS max_temp_c,
  ROUND(AVG(s.vibration_mm_s),       3)  AS avg_vibration,
  ROUND(MAX(s.vibration_mm_s),       3)  AS max_vibration,
  ROUND(AVG(s.spindle_speed_rpm),    0)  AS avg_spindle_rpm,
  ROUND(AVG(s.power_consumption_kw), 2)  AS avg_power_kw,
  ROUND(AVG(s.coolant_flow_lpm),     2)  AS avg_coolant_lpm,
  ROUND(MAX(s.tool_wear_pct),        2)  AS max_tool_wear_pct,
  ROUND(AVG(s.health_score),         1)  AS avg_health_score,
  ROUND(MIN(s.health_score),         1)  AS min_health_score,
  SUM(CASE WHEN s.anomaly_flag THEN 1 ELSE 0 END) AS anomaly_count,
  -- Maintenance urgency: Critical / Warning / Normal
  CASE
    WHEN ROUND(MIN(s.health_score), 1) < 40 OR MAX(s.tool_wear_pct) > 90 THEN 'Critical'
    WHEN ROUND(MIN(s.health_score), 1) < 65 OR MAX(s.tool_wear_pct) > 75 THEN 'Warning'
    ELSE 'Normal'
  END                                    AS maintenance_urgency,
  -- Days since last PM (static from master data)
  DATEDIFF(
    DATE_TRUNC('DAY', s.reading_timestamp),
    m.last_pm_date
  )                                      AS days_since_last_pm
FROM LIVE.silver_sensor_data s
JOIN LIVE.silver_machines     m ON s.machine_id = m.machine_id
GROUP BY 1,2,3,4,5,6,7,8, m.last_pm_date;

-- ─────────────────────────────────────────────────────────────

-- Gold: Capacity Planning — Demand vs Available Capacity
CREATE OR REFRESH MATERIALIZED VIEW gold_capacity_planning
CLUSTER BY (period_month, work_center)
COMMENT "Monthly capacity demand vs supply analysis to support investment decisions"
AS
WITH demand AS (
  SELECT
    work_center,
    DATE_TRUNC('MONTH', scheduled_start) AS period_month,
    YEAR(scheduled_start)                AS period_year,
    SUM(standard_total_hrs)              AS demand_hrs,
    SUM(CASE WHEN is_farm_out THEN standard_total_hrs ELSE 0 END) AS farm_out_hrs,
    SUM(CASE WHEN is_farm_out THEN COALESCE(farm_out_cost_usd,0)  ELSE 0 END) AS farm_out_cost,
    COUNT(DISTINCT machine_id)           AS machines_used
  FROM LIVE.silver_work_orders
  WHERE scheduled_start IS NOT NULL
  GROUP BY 1,2,3
),
capacity AS (
  SELECT
    work_center,
    -- Available hours per month per work center (avg 22 working days × capacity)
    SUM(capacity_hrs_per_day) * 22       AS available_hrs_per_month,
    COUNT(*)                             AS machine_count
  FROM LIVE.silver_machines
  WHERE status = 'Active'
  GROUP BY 1
)
SELECT
  d.work_center,
  d.period_month,
  d.period_year,
  ROUND(d.demand_hrs,           2) AS demand_hrs,
  ROUND(c.available_hrs_per_month, 2) AS available_hrs,
  ROUND(d.farm_out_hrs,         2) AS farm_out_hrs,
  ROUND(d.farm_out_cost,        2) AS farm_out_cost_usd,
  c.machine_count,
  d.machines_used,
  ROUND(d.demand_hrs / NULLIF(c.available_hrs_per_month, 0) * 100, 1) AS capacity_utilization_pct,
  ROUND(d.farm_out_hrs / NULLIF(d.demand_hrs, 0) * 100, 1)            AS farm_out_rate_pct,
  -- Load status
  CASE
    WHEN d.demand_hrs / NULLIF(c.available_hrs_per_month, 0) > 0.90 THEN 'Overloaded'
    WHEN d.demand_hrs / NULLIF(c.available_hrs_per_month, 0) > 0.75 THEN 'High Load'
    WHEN d.demand_hrs / NULLIF(c.available_hrs_per_month, 0) > 0.50 THEN 'Normal'
    ELSE 'Underutilized'
  END                              AS load_status
FROM demand d
LEFT JOIN capacity c USING (work_center);

-- ─────────────────────────────────────────────────────────────

-- Gold: Farm-Out Analysis — Vendor Performance & Cost
CREATE OR REFRESH MATERIALIZED VIEW gold_farmout_analysis
CLUSTER BY (farm_out_vendor)
COMMENT "Farm-out vendor cost analysis to support make-vs-buy decisions"
AS
SELECT
  farm_out_vendor,
  part_number,
  part_description,
  DATE_TRUNC('MONTH', due_date)           AS period_month,
  YEAR(due_date)                          AS period_year,
  COUNT(*)                                AS farm_out_orders,
  SUM(order_qty)                          AS total_qty_farmed,
  ROUND(SUM(farm_out_cost_usd), 2)        AS total_farm_out_cost,
  ROUND(AVG(farm_out_cost_usd), 2)        AS avg_cost_per_order,
  ROUND(SUM(standard_total_hrs), 2)       AS equivalent_internal_hrs,
  -- Implied internal cost if done in-house — blended machine rate ~$65/hr
  -- (reflects the lower capital-depreciation-adjusted rate after recent investment)
  ROUND(SUM(standard_total_hrs) * 65, 2) AS implied_internal_cost,
  ROUND(
    (SUM(farm_out_cost_usd) - SUM(standard_total_hrs) * 65)
    / NULLIF(SUM(farm_out_cost_usd), 0) * 100, 1
  )                                       AS cost_premium_pct
FROM LIVE.silver_work_orders
WHERE is_farm_out = TRUE
  AND farm_out_vendor IS NOT NULL
GROUP BY 1,2,3,4,5;
