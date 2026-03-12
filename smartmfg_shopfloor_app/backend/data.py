"""
SmartMFG Shop Floor App — Data Backend
Queries Silver and Gold tables in satsen_catalog.smartmfg_machine_optimization_1
via the Databricks SQL connector using app-auth credentials from Config().
"""

import os
import pandas as pd
from databricks.sdk.core import Config
from databricks import sql as dbsql

CATALOG = "satsen_catalog"
SCHEMA  = "smartmfg_machine_optimization_1"
FQN     = f"{CATALOG}.{SCHEMA}"


class Backend:
    def __init__(self):
        self._cfg  = Config()
        self._conn = self._make_conn()

    # ── Connection ─────────────────────────────────────────────────────────

    def _make_conn(self):
        return dbsql.connect(
            server_hostname=self._cfg.host,
            http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
            credentials_provider=lambda: self._cfg.authenticate,
        )

    def _query(self, sql: str) -> pd.DataFrame:
        try:
            return self._run(sql)
        except Exception:
            self._conn = self._make_conn()
            return self._run(sql)

    def _run(self, sql: str) -> pd.DataFrame:
        with self._conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)

    def _scalar(self, sql: str, default=None):
        df = self._query(sql)
        if df.empty:
            return default
        return df.iloc[0, 0]

    # ── KPIs ───────────────────────────────────────────────────────────────

    def get_machine_status_counts(self) -> dict:
        df = self._query(f"""
            SELECT
              COUNT(*)                                               AS total,
              SUM(CASE WHEN status='Active'      THEN 1 ELSE 0 END) AS active,
              SUM(CASE WHEN status='Maintenance' THEN 1 ELSE 0 END) AS maintenance,
              SUM(CASE WHEN status='Idle'        THEN 1 ELSE 0 END) AS idle
            FROM {FQN}.silver_machines
        """)
        return df.iloc[0].to_dict() if not df.empty else {}

    def get_performance_kpis(self) -> dict:
        df = self._query(f"""
            SELECT
              ROUND(util.avg_util, 1)    AS avg_utilization_pct,
              ROUND(otd.avg_otd,  1)     AS avg_otd_pct,
              ROUND(fo.ytd_cost,  0)     AS ytd_farmout_cost,
              ROUND(fo.avg_prem,  1)     AS avg_cost_premium_pct,
              maint.critical_cnt         AS critical_machines,
              maint.warning_cnt          AS warning_machines,
              ROUND(active_wo.in_prog, 0) AS orders_in_progress
            FROM (
              SELECT AVG(utilization_pct) AS avg_util
              FROM {FQN}.gold_machine_utilization
              WHERE period_month = (SELECT MAX(period_month) FROM {FQN}.gold_machine_utilization)
            ) util
            CROSS JOIN (
              -- True weighted OTD: on-time completions / total completions
              -- Avoids per-group average distortion when the latest month has few orders
              SELECT ROUND(
                SUM(CASE WHEN is_late = FALSE THEN 1.0 ELSE 0 END) * 100
                / NULLIF(COUNT(*), 0), 1
              ) AS avg_otd
              FROM {FQN}.silver_work_orders
              WHERE status = 'Complete'
            ) otd
            CROSS JOIN (
              SELECT
                SUM(total_farm_out_cost)  AS ytd_cost,
                AVG(cost_premium_pct)     AS avg_prem
              FROM {FQN}.gold_farmout_analysis
              WHERE period_year = (SELECT MAX(period_year) FROM {FQN}.gold_farmout_analysis)
            ) fo
            CROSS JOIN (
              SELECT
                SUM(CASE WHEN maintenance_urgency='Critical' THEN 1 ELSE 0 END) AS critical_cnt,
                SUM(CASE WHEN maintenance_urgency='Warning'  THEN 1 ELSE 0 END) AS warning_cnt
              FROM {FQN}.gold_predictive_maintenance
              WHERE reading_date = (SELECT MAX(reading_date) FROM {FQN}.gold_predictive_maintenance)
            ) maint
            CROSS JOIN (
              SELECT COUNT(*) AS in_prog
              FROM {FQN}.silver_work_orders
              WHERE status IN ('Open','In Progress')
            ) active_wo
        """)
        return df.iloc[0].to_dict() if not df.empty else {}

    # ── Charts ─────────────────────────────────────────────────────────────

    def get_utilization_by_machine(self) -> pd.DataFrame:
        return self._query(f"""
            SELECT
              m.machine_id,
              m.machine_name,
              m.work_center,
              ROUND(AVG(g.utilization_pct), 1) AS avg_utilization_pct,
              ROUND(AVG(g.efficiency_pct),  1) AS avg_efficiency_pct
            FROM {FQN}.gold_machine_utilization g
            JOIN {FQN}.silver_machines m ON g.machine_id = m.machine_id
            GROUP BY m.machine_id, m.machine_name, m.work_center
            ORDER BY avg_utilization_pct DESC
        """)

    def get_scheduling_trend(self) -> pd.DataFrame:
        return self._query(f"""
            SELECT
              date_format(period_month, 'yyyy-MM') AS month,
              ROUND(AVG(on_time_delivery_pct), 1)  AS avg_otd_pct,
              SUM(total_orders)                     AS total_orders,
              SUM(late_count)                       AS late_orders,
              SUM(farm_out_count)                   AS farm_out_orders,
              ROUND(SUM(total_farm_out_cost), 0)    AS farm_out_cost
            FROM {FQN}.gold_scheduling_performance
            WHERE period_month IS NOT NULL
            GROUP BY month
            ORDER BY month
        """)

    def get_farmout_by_vendor(self) -> pd.DataFrame:
        return self._query(f"""
            SELECT
              farm_out_vendor,
              ROUND(SUM(total_farm_out_cost),   0) AS total_farmout_cost,
              ROUND(SUM(implied_internal_cost),  0) AS implied_inhouse_cost,
              ROUND(AVG(cost_premium_pct),       1) AS avg_premium_pct,
              SUM(farm_out_orders)                  AS total_orders
            FROM {FQN}.gold_farmout_analysis
            GROUP BY farm_out_vendor
            ORDER BY total_farmout_cost DESC
        """)

    def get_maintenance_summary(self) -> pd.DataFrame:
        return self._query(f"""
            SELECT
              machine_id,
              machine_name,
              work_center,
              ROUND(MIN(min_health_score), 1)  AS worst_health_score,
              ROUND(MAX(max_tool_wear_pct), 1) AS max_tool_wear_pct,
              SUM(anomaly_count)               AS total_anomalies,
              maintenance_urgency
            FROM {FQN}.gold_predictive_maintenance
            WHERE reading_date = (SELECT MAX(reading_date) FROM {FQN}.gold_predictive_maintenance)
            GROUP BY machine_id, machine_name, work_center, maintenance_urgency
            ORDER BY
              CASE maintenance_urgency
                WHEN 'Critical' THEN 1
                WHEN 'Warning'  THEN 2
                ELSE 3
              END,
              worst_health_score
        """)

    def get_capacity_by_workcenter(self) -> pd.DataFrame:
        return self._query(f"""
            SELECT
              work_center,
              ROUND(AVG(capacity_utilization_pct), 1) AS avg_capacity_pct,
              load_status
            FROM {FQN}.gold_capacity_planning
            WHERE period_month = (SELECT MAX(period_month) FROM {FQN}.gold_capacity_planning)
            GROUP BY work_center, load_status
            ORDER BY avg_capacity_pct DESC
        """)

    def get_machine_kpis(self, machine_id: str) -> dict:
        """Return KPIs scoped to a single machine for the Command Centre filter."""
        df = self._query(f"""
            SELECT
              m.machine_name,
              m.machine_type,
              m.work_center,
              m.status                                             AS machine_status,
              ROUND(COALESCE(util.avg_util, 0), 1)                AS avg_utilization_pct,
              otd.avg_otd                                          AS avg_otd_pct,
              COALESCE(maint.urgency, 'Unknown')                   AS maintenance_urgency,
              ROUND(COALESCE(maint.health, 0), 1)                 AS health_score,
              COALESCE(wo.in_prog, 0)                             AS orders_in_progress,
              COALESCE(fo.wc_farmout, 0)                          AS wc_farmout_cost
            FROM (
              SELECT machine_id, machine_name, machine_type, work_center, status
              FROM {FQN}.silver_machines WHERE machine_id = '{machine_id}'
            ) m
            LEFT JOIN (
              SELECT ROUND(AVG(utilization_pct), 1) AS avg_util
              FROM {FQN}.gold_machine_utilization
              WHERE machine_id = '{machine_id}'
            ) util ON TRUE
            LEFT JOIN (
              SELECT ROUND(
                SUM(CASE WHEN is_late = FALSE THEN 1.0 ELSE 0 END) * 100
                / NULLIF(COUNT(*), 0), 1
              ) AS avg_otd
              FROM {FQN}.silver_work_orders
              WHERE status = 'Complete' AND machine_id = '{machine_id}'
            ) otd ON TRUE
            LEFT JOIN (
              SELECT maintenance_urgency AS urgency,
                     ROUND(MIN(min_health_score), 1) AS health
              FROM {FQN}.gold_predictive_maintenance
              WHERE machine_id = '{machine_id}'
                AND reading_date = (
                  SELECT MAX(reading_date)
                  FROM {FQN}.gold_predictive_maintenance
                  WHERE machine_id = '{machine_id}'
                )
              GROUP BY maintenance_urgency
              LIMIT 1
            ) maint ON TRUE
            LEFT JOIN (
              SELECT COUNT(*) AS in_prog
              FROM {FQN}.silver_work_orders
              WHERE machine_id = '{machine_id}'
                AND status IN ('Open', 'In Progress')
            ) wo ON TRUE
            LEFT JOIN (
              SELECT ROUND(SUM(total_farm_out_cost), 0) AS wc_farmout
              FROM {FQN}.gold_farmout_analysis
              WHERE work_center = (
                SELECT work_center FROM {FQN}.silver_machines WHERE machine_id = '{machine_id}'
              )
            ) fo ON TRUE
        """)
        return df.iloc[0].to_dict() if not df.empty else {}

    # ── Machine Fleet ──────────────────────────────────────────────────────

    def get_machine_fleet(self) -> pd.DataFrame:
        return self._query(f"""
            SELECT
              m.machine_id,
              m.machine_name,
              m.machine_type,
              m.work_center,
              m.status,
              m.num_shifts,
              ROUND(m.hourly_rate_usd, 2)       AS hourly_rate_usd,
              ROUND(m.capacity_hrs_per_day, 1)  AS capacity_hrs_per_day,
              COALESCE(wo.active_orders, 0)     AS active_orders,
              COALESCE(wo.total_qty, 0)         AS total_qty_in_progress,
              COALESCE(wo.parts_list, '')       AS parts_in_progress,
              COALESCE(s.health_score, 0)       AS latest_health_score,
              s.machine_status                  AS sensor_status,
              s.tool_wear_pct,
              s.anomaly_flag
            FROM {FQN}.silver_machines m
            LEFT JOIN (
              SELECT
                machine_id,
                COUNT(DISTINCT work_order_id)                       AS active_orders,
                SUM(order_qty)                                      AS total_qty,
                ARRAY_JOIN(ARRAY_DISTINCT(COLLECT_LIST(part_description)), ', ') AS parts_list
              FROM {FQN}.silver_work_orders
              WHERE status IN ('Open','In Progress')
              GROUP BY machine_id
            ) wo ON m.machine_id = wo.machine_id
            LEFT JOIN (
              SELECT machine_id, health_score, machine_status, tool_wear_pct, anomaly_flag
              FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY machine_id ORDER BY reading_timestamp DESC) AS rn
                FROM {FQN}.silver_sensor_data
              ) WHERE rn = 1
            ) s ON m.machine_id = s.machine_id
            ORDER BY m.work_center, m.status, m.machine_id
        """)

    # ── Machine Inspector ──────────────────────────────────────────────────

    def get_machines_list(self) -> pd.DataFrame:
        return self._query(f"""
            SELECT machine_id, machine_name, machine_type, work_center, status
            FROM {FQN}.silver_machines
            ORDER BY work_center, machine_id
        """)

    def get_machine_detail(self, machine_id: str) -> dict:
        df = self._query(f"""
            SELECT
              m.machine_id, m.machine_name, m.machine_type, m.work_center,
              m.status, m.num_shifts, m.vendor, m.installation_year,
              ROUND(m.hourly_rate_usd,        2) AS hourly_rate_usd,
              ROUND(m.capacity_hrs_per_day,   1) AS capacity_hrs_per_day,
              m.preventive_maint_interval_hrs,
              CAST(m.last_pm_date AS STRING)     AS last_pm_date,
              m.notes,
              ROUND(s.temperature_celsius,  2)   AS temperature_celsius,
              ROUND(s.vibration_mm_s,       3)   AS vibration_mm_s,
              ROUND(s.spindle_speed_rpm,    0)   AS spindle_speed_rpm,
              ROUND(s.power_consumption_kw, 2)   AS power_consumption_kw,
              ROUND(s.coolant_flow_lpm,     2)   AS coolant_flow_lpm,
              ROUND(s.tool_wear_pct,        1)   AS tool_wear_pct,
              ROUND(s.health_score,         1)   AS health_score,
              s.machine_status                   AS sensor_machine_status,
              CAST(s.anomaly_flag AS BOOLEAN)    AS anomaly_flag,
              s.anomaly_type,
              CAST(s.reading_timestamp AS STRING) AS last_reading_ts
            FROM {FQN}.silver_machines m
            LEFT JOIN (
              SELECT *
              FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY machine_id ORDER BY reading_timestamp DESC) AS rn
                FROM {FQN}.silver_sensor_data
              ) WHERE rn = 1
            ) s ON m.machine_id = s.machine_id
            WHERE m.machine_id = '{machine_id}'
        """)
        return df.iloc[0].to_dict() if not df.empty else {}

    def get_machine_orders(self, machine_id: str) -> pd.DataFrame:
        return self._query(f"""
            SELECT
              work_order_id,
              part_number,
              part_description,
              order_qty,
              operation_description,
              status,
              priority,
              CAST(due_date        AS STRING) AS due_date,
              CAST(scheduled_start AS STRING) AS scheduled_start,
              ROUND(standard_total_hrs, 2)    AS standard_total_hrs
            FROM {FQN}.silver_work_orders
            WHERE machine_id = '{machine_id}'
              AND status IN ('Open','In Progress')
            ORDER BY
              CASE priority WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END,
              due_date
        """)

    def get_alternative_machines(self, work_center: str, exclude_id: str) -> pd.DataFrame:
        return self._query(f"""
            SELECT
              m.machine_id,
              m.machine_name,
              m.machine_type,
              ROUND(m.hourly_rate_usd,       2) AS hourly_rate_usd,
              ROUND(m.capacity_hrs_per_day,  1) AS capacity_hrs_per_day,
              COALESCE(wo.active_orders,     0) AS current_orders,
              ROUND(
                m.capacity_hrs_per_day - COALESCE(SUM_hrs.hrs_loaded, 0),
                1
              ) AS available_hrs
            FROM {FQN}.silver_machines m
            LEFT JOIN (
              SELECT machine_id, COUNT(*) AS active_orders
              FROM {FQN}.silver_work_orders
              WHERE status IN ('Open','In Progress')
              GROUP BY machine_id
            ) wo ON m.machine_id = wo.machine_id
            LEFT JOIN (
              SELECT machine_id, SUM(standard_total_hrs) AS hrs_loaded
              FROM {FQN}.silver_work_orders
              WHERE status IN ('Open','In Progress')
              GROUP BY machine_id
            ) SUM_hrs ON m.machine_id = SUM_hrs.machine_id
            WHERE m.work_center = '{work_center}'
              AND m.machine_id  != '{exclude_id}'
              AND m.status       = 'Active'
            ORDER BY available_hrs DESC
        """)
