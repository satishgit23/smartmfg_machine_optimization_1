# Databricks notebook source
# MAGIC %md
# MAGIC # SmartMFG Machine Scheduling Optimization - Lakeview Dashboard
# MAGIC **Notebook:** 04_dashboard.py
# MAGIC
# MAGIC Creates the AI/BI (Lakeview) dashboard: **smartmfg_machine_optimization_dashboard**
# MAGIC
# MAGIC Pages:
# MAGIC | # | Page | Description |
# MAGIC |---|------|-------------|
# MAGIC | 1 | Machine Utilization | Utilization % and revenue trends by machine |
# MAGIC | 2 | Scheduling Performance | OTD, farm-out cost, late orders by work center |
# MAGIC | 3 | Predictive Maintenance | Health scores, anomaly counts, PM urgency |
# MAGIC | 4 | Capacity Planning | Demand vs supply, overloaded work centers |
# MAGIC | 5 | Farm-Out Analysis | Vendor cost comparison, make-vs-buy insights |

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

CATALOG = "satsen_catalog"
SCHEMA  = "smartmfg_machine_optimization_1"

# COMMAND ----------
# MAGIC %md ## Step 1: Get Best Warehouse

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Get an available warehouse
warehouses = list(w.warehouses.list())
warehouse  = next(
    (wh for wh in warehouses if wh.state and wh.state.name == "RUNNING"),
    warehouses[0] if warehouses else None
)
if not warehouse:
    raise RuntimeError("No SQL warehouse available. Start one in the SQL Warehouse UI.")

WAREHOUSE_ID = warehouse.id
print(f"✅ Using warehouse: {warehouse.name}  (id={WAREHOUSE_ID})")

# COMMAND ----------
# MAGIC %md ## Step 2: Validate Gold Table Queries

# COMMAND ----------

# Test each dataset query before building dashboard
test_queries = {
    "utilization_summary": f"""
        SELECT machine_id,
               ROUND(AVG(utilization_pct), 1)  AS avg_utilization_pct,
               ROUND(AVG(efficiency_pct),  1)  AS avg_efficiency_pct,
               ROUND(SUM(revenue_usd),     0)  AS total_revenue_usd,
               SUM(work_order_count)           AS total_work_orders
        FROM {CATALOG}.{SCHEMA}.gold_machine_utilization
        GROUP BY machine_id
        ORDER BY avg_utilization_pct DESC
        LIMIT 12
    """,
    "utilization_monthly": f"""
        SELECT DATE_FORMAT(period_month, 'yyyy-MM') AS month,
               machine_id,
               ROUND(utilization_pct, 1)            AS utilization_pct,
               ROUND(revenue_usd, 0)                AS revenue_usd,
               work_order_count
        FROM {CATALOG}.{SCHEMA}.gold_machine_utilization
        ORDER BY period_month, machine_id
    """,
    "scheduling_monthly": f"""
        SELECT DATE_FORMAT(period_month, 'yyyy-MM') AS month,
               work_center,
               total_orders,
               late_count,
               farm_out_count,
               ROUND(total_farm_out_cost, 0)        AS farm_out_cost_usd,
               ROUND(on_time_delivery_pct, 1)       AS otd_pct
        FROM {CATALOG}.{SCHEMA}.gold_scheduling_performance
        ORDER BY period_month DESC, work_center
    """,
    "maintenance_snapshot": f"""
        WITH latest AS (
          SELECT machine_id, MAX(reading_date) AS latest_date
          FROM {CATALOG}.{SCHEMA}.gold_predictive_maintenance
          GROUP BY machine_id
        )
        SELECT p.machine_id, p.machine_name, p.work_center,
               p.avg_health_score, p.max_tool_wear_pct,
               p.anomaly_count, p.maintenance_urgency, p.days_since_last_pm
        FROM {CATALOG}.{SCHEMA}.gold_predictive_maintenance p
        JOIN latest l ON p.machine_id = l.machine_id AND p.reading_date = l.latest_date
        ORDER BY CASE p.maintenance_urgency WHEN 'Critical' THEN 1 WHEN 'Warning' THEN 2 ELSE 3 END
    """,
    "capacity_planning": f"""
        SELECT DATE_FORMAT(period_month, 'yyyy-MM') AS month,
               work_center,
               ROUND(demand_hrs, 0)                 AS demand_hrs,
               ROUND(available_hrs, 0)              AS available_hrs,
               ROUND(capacity_utilization_pct, 1)   AS capacity_util_pct,
               ROUND(farm_out_cost_usd, 0)          AS farm_out_cost_usd,
               load_status
        FROM {CATALOG}.{SCHEMA}.gold_capacity_planning
        ORDER BY period_month DESC, work_center
    """,
    "farmout_vendor": f"""
        SELECT farm_out_vendor,
               SUM(farm_out_orders)                 AS total_orders,
               ROUND(SUM(total_farm_out_cost),  0)  AS total_farm_out_cost,
               ROUND(SUM(implied_internal_cost),0)  AS implied_internal_cost,
               ROUND(AVG(cost_premium_pct), 1)      AS avg_cost_premium_pct
        FROM {CATALOG}.{SCHEMA}.gold_farmout_analysis
        GROUP BY farm_out_vendor
        ORDER BY total_farm_out_cost DESC
    """,
    "kpi_summary": f"""
        SELECT
          COUNT(DISTINCT machine_id)                          AS total_machines,
          ROUND(AVG(utilization_pct), 1)                     AS avg_utilization_pct,
          ROUND(SUM(revenue_usd), 0)                         AS total_revenue_usd,
          SUM(work_order_count)                               AS total_work_orders
        FROM {CATALOG}.{SCHEMA}.gold_machine_utilization
    """,
}

print("Testing dataset queries against Gold tables...\n")
for name, sql in test_queries.items():
    try:
        df = spark.sql(sql)
        row_count = df.count()
        print(f"  ✅ {name}: {row_count} rows")
    except Exception as e:
        print(f"  ❌ {name}: {e}")

# COMMAND ----------
# MAGIC %md ## Step 3: Build Dashboard JSON

# COMMAND ----------

dashboard_def = {
    "datasets": [
        # ── Dataset 1: KPI Summary (1 row) ──────────────────────────────
        {
            "name": "ds_kpi_summary",
            "displayName": "KPI Summary",
            "queryLines": [
                f"SELECT ",
                f"  COUNT(DISTINCT machine_id)         AS total_machines, ",
                f"  ROUND(AVG(utilization_pct), 1)     AS avg_utilization_pct, ",
                f"  ROUND(SUM(revenue_usd), 0)         AS total_revenue_usd, ",
                f"  SUM(work_order_count)               AS total_work_orders ",
                f"FROM {CATALOG}.{SCHEMA}.gold_machine_utilization ",
            ]
        },
        # ── Dataset 2: Machine Utilization Monthly ───────────────────────
        {
            "name": "ds_util_monthly",
            "displayName": "Utilization Monthly",
            "queryLines": [
                f"SELECT DATE_FORMAT(period_month, 'yyyy-MM') AS month, ",
                f"  machine_id, ",
                f"  ROUND(utilization_pct, 1) AS utilization_pct, ",
                f"  ROUND(revenue_usd, 0)     AS revenue_usd, ",
                f"  work_order_count ",
                f"FROM {CATALOG}.{SCHEMA}.gold_machine_utilization ",
                f"ORDER BY period_month, machine_id ",
            ]
        },
        # ── Dataset 3: Scheduling Performance Monthly ────────────────────
        {
            "name": "ds_scheduling",
            "displayName": "Scheduling Performance",
            "queryLines": [
                f"SELECT DATE_FORMAT(period_month, 'yyyy-MM') AS month, ",
                f"  work_center, ",
                f"  total_orders, late_count, farm_out_count, ",
                f"  ROUND(total_farm_out_cost, 0) AS farm_out_cost_usd, ",
                f"  ROUND(on_time_delivery_pct,1) AS otd_pct ",
                f"FROM {CATALOG}.{SCHEMA}.gold_scheduling_performance ",
                f"ORDER BY period_month DESC, work_center ",
            ]
        },
        # ── Dataset 4: Predictive Maintenance Snapshot ───────────────────
        {
            "name": "ds_maintenance",
            "displayName": "Machine Health Snapshot",
            "queryLines": [
                f"WITH latest AS ( ",
                f"  SELECT machine_id, MAX(reading_date) AS latest_date ",
                f"  FROM {CATALOG}.{SCHEMA}.gold_predictive_maintenance ",
                f"  GROUP BY machine_id ",
                f") ",
                f"SELECT p.machine_id, p.machine_name, p.work_center, ",
                f"  ROUND(p.avg_health_score,1) AS avg_health_score, ",
                f"  ROUND(p.max_tool_wear_pct,1) AS max_tool_wear_pct, ",
                f"  p.anomaly_count, p.maintenance_urgency, p.days_since_last_pm ",
                f"FROM {CATALOG}.{SCHEMA}.gold_predictive_maintenance p ",
                f"JOIN latest l ON p.machine_id=l.machine_id AND p.reading_date=l.latest_date ",
                f"ORDER BY CASE p.maintenance_urgency WHEN 'Critical' THEN 1 WHEN 'Warning' THEN 2 ELSE 3 END ",
            ]
        },
        # ── Dataset 5: Capacity Planning Monthly ─────────────────────────
        {
            "name": "ds_capacity",
            "displayName": "Capacity Planning",
            "queryLines": [
                f"SELECT DATE_FORMAT(period_month, 'yyyy-MM') AS month, ",
                f"  work_center, ",
                f"  ROUND(demand_hrs, 0)               AS demand_hrs, ",
                f"  ROUND(available_hrs, 0)             AS available_hrs, ",
                f"  ROUND(capacity_utilization_pct, 1)  AS capacity_util_pct, ",
                f"  ROUND(farm_out_cost_usd, 0)         AS farm_out_cost_usd, ",
                f"  load_status ",
                f"FROM {CATALOG}.{SCHEMA}.gold_capacity_planning ",
                f"ORDER BY period_month DESC, work_center ",
            ]
        },
        # ── Dataset 6: Farm-Out Vendor ────────────────────────────────────
        {
            "name": "ds_farmout",
            "displayName": "Farm-Out Vendor Analysis",
            "queryLines": [
                f"SELECT farm_out_vendor, ",
                f"  SUM(farm_out_orders)                  AS total_orders, ",
                f"  ROUND(SUM(total_farm_out_cost),  0)   AS total_farm_out_cost, ",
                f"  ROUND(SUM(implied_internal_cost), 0)  AS implied_internal_cost, ",
                f"  ROUND(AVG(cost_premium_pct), 1)       AS avg_cost_premium_pct ",
                f"FROM {CATALOG}.{SCHEMA}.gold_farmout_analysis ",
                f"GROUP BY farm_out_vendor ",
                f"ORDER BY total_farm_out_cost DESC ",
            ]
        },
    ],

    "pages": [
        # ═══════════════════════════════════════════════════════
        # PAGE 1: Machine Utilization
        # ═══════════════════════════════════════════════════════
        {
            "name": "machine_utilization",
            "displayName": "Machine Utilization",
            "pageType": "PAGE_TYPE_CANVAS",
            "layout": [
                # Title
                {
                    "widget": {
                        "name": "title_util",
                        "multilineTextboxSpec": {"lines": ["## Machine Utilization Dashboard"]}
                    },
                    "position": {"x": 0, "y": 0, "width": 6, "height": 1}
                },
                # Subtitle
                {
                    "widget": {
                        "name": "subtitle_util",
                        "multilineTextboxSpec": {"lines": ["Track machine utilization %, efficiency, and revenue contribution by week, month, and year"]}
                    },
                    "position": {"x": 0, "y": 1, "width": 6, "height": 1}
                },
                # KPI: Total Machines
                {
                    "widget": {
                        "name": "kpi-total-machines",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_kpi_summary",
                            "fields": [{"name": "total_machines", "expression": "`total_machines`"}],
                            "disaggregated": True
                        }}],
                        "spec": {"version": 2, "widgetType": "counter",
                                 "encodings": {"value": {"fieldName": "total_machines", "displayName": "Total Machines"}},
                                 "frame": {"showTitle": True, "title": "Total Machines"}}
                    },
                    "position": {"x": 0, "y": 2, "width": 2, "height": 3}
                },
                # KPI: Avg Utilization
                {
                    "widget": {
                        "name": "kpi-avg-util",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_kpi_summary",
                            "fields": [{"name": "avg_utilization_pct", "expression": "`avg_utilization_pct`"}],
                            "disaggregated": True
                        }}],
                        "spec": {"version": 2, "widgetType": "counter",
                                 "encodings": {"value": {"fieldName": "avg_utilization_pct", "displayName": "Avg Utilization %"}},
                                 "frame": {"showTitle": True, "title": "Avg Utilization %"}}
                    },
                    "position": {"x": 2, "y": 2, "width": 2, "height": 3}
                },
                # KPI: Total Revenue
                {
                    "widget": {
                        "name": "kpi-revenue",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_kpi_summary",
                            "fields": [{"name": "total_revenue_usd", "expression": "`total_revenue_usd`"}],
                            "disaggregated": True
                        }}],
                        "spec": {"version": 2, "widgetType": "counter",
                                 "encodings": {"value": {"fieldName": "total_revenue_usd", "displayName": "Total Revenue $"}},
                                 "frame": {"showTitle": True, "title": "Total Revenue (USD)"}}
                    },
                    "position": {"x": 4, "y": 2, "width": 2, "height": 3}
                },
                # Chart: Utilization % by machine (bar)
                {
                    "widget": {
                        "name": "chart-util-by-machine",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_util_monthly",
                            "fields": [
                                {"name": "machine_id",     "expression": "`machine_id`"},
                                {"name": "sum(revenue_usd)", "expression": "SUM(`revenue_usd`)"}
                            ],
                            "disaggregated": False
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "machine_id",       "scale": {"type": "categorical"}, "displayName": "Machine"},
                                "y": {"fieldName": "sum(revenue_usd)", "scale": {"type": "quantitative"}, "displayName": "Revenue (USD)"}
                            },
                            "frame": {"showTitle": True, "title": "Revenue by Machine"}
                        }
                    },
                    "position": {"x": 0, "y": 5, "width": 3, "height": 5}
                },
                # Chart: Utilization trend over time (line)
                {
                    "widget": {
                        "name": "chart-util-trend",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_util_monthly",
                            "fields": [
                                {"name": "month",             "expression": "`month`"},
                                {"name": "avg(utilization_pct)", "expression": "AVG(`utilization_pct`)"}
                            ],
                            "disaggregated": False
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "line",
                            "encodings": {
                                "x": {"fieldName": "month",                "scale": {"type": "categorical"}, "displayName": "Month"},
                                "y": {"fieldName": "avg(utilization_pct)", "scale": {"type": "quantitative"}, "displayName": "Avg Utilization %"}
                            },
                            "frame": {"showTitle": True, "title": "Monthly Avg Utilization Trend"}
                        }
                    },
                    "position": {"x": 3, "y": 5, "width": 3, "height": 5}
                },
                # Table: Machine utilization detail
                {
                    "widget": {
                        "name": "table-util-detail",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_util_monthly",
                            "fields": [
                                {"name": "month",            "expression": "`month`"},
                                {"name": "machine_id",       "expression": "`machine_id`"},
                                {"name": "utilization_pct",  "expression": "`utilization_pct`"},
                                {"name": "revenue_usd",      "expression": "`revenue_usd`"},
                                {"name": "work_order_count", "expression": "`work_order_count`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 2, "widgetType": "table",
                            "encodings": {"columns": [
                                {"fieldName": "month",            "displayName": "Month"},
                                {"fieldName": "machine_id",       "displayName": "Machine ID"},
                                {"fieldName": "utilization_pct",  "displayName": "Utilization %"},
                                {"fieldName": "revenue_usd",      "displayName": "Revenue (USD)"},
                                {"fieldName": "work_order_count", "displayName": "Work Orders"}
                            ]},
                            "frame": {"showTitle": True, "title": "Utilization Detail (Monthly)"}
                        }
                    },
                    "position": {"x": 0, "y": 10, "width": 6, "height": 6}
                },
            ]
        },

        # ═══════════════════════════════════════════════════════
        # PAGE 2: Scheduling Performance
        # ═══════════════════════════════════════════════════════
        {
            "name": "scheduling_performance",
            "displayName": "Scheduling Performance",
            "pageType": "PAGE_TYPE_CANVAS",
            "layout": [
                {"widget": {"name": "title_sched", "multilineTextboxSpec": {"lines": ["## Scheduling Performance"]}},
                 "position": {"x": 0, "y": 0, "width": 6, "height": 1}},
                {"widget": {"name": "subtitle_sched", "multilineTextboxSpec": {"lines": ["On-time delivery, farm-out costs, and late order analysis by work center"]}},
                 "position": {"x": 0, "y": 1, "width": 6, "height": 1}},
                # KPI: Total Orders
                {
                    "widget": {
                        "name": "kpi-total-orders",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_scheduling",
                            "fields": [{"name": "sum(total_orders)", "expression": "SUM(`total_orders`)"}],
                            "disaggregated": False
                        }}],
                        "spec": {"version": 2, "widgetType": "counter",
                                 "encodings": {"value": {"fieldName": "sum(total_orders)", "displayName": "Total Orders"}},
                                 "frame": {"showTitle": True, "title": "Total Work Orders"}}
                    },
                    "position": {"x": 0, "y": 2, "width": 2, "height": 3}
                },
                # KPI: Farm-Out Cost
                {
                    "widget": {
                        "name": "kpi-farmout-cost",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_scheduling",
                            "fields": [{"name": "sum(farm_out_cost_usd)", "expression": "SUM(`farm_out_cost_usd`)"}],
                            "disaggregated": False
                        }}],
                        "spec": {"version": 2, "widgetType": "counter",
                                 "encodings": {"value": {"fieldName": "sum(farm_out_cost_usd)", "displayName": "Farm-Out Cost $"}},
                                 "frame": {"showTitle": True, "title": "Total Farm-Out Cost (USD)"}}
                    },
                    "position": {"x": 2, "y": 2, "width": 2, "height": 3}
                },
                # KPI: Avg OTD
                {
                    "widget": {
                        "name": "kpi-avg-otd",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_scheduling",
                            "fields": [{"name": "avg(otd_pct)", "expression": "AVG(`otd_pct`)"}],
                            "disaggregated": False
                        }}],
                        "spec": {"version": 2, "widgetType": "counter",
                                 "encodings": {"value": {"fieldName": "avg(otd_pct)", "displayName": "OTD %"}},
                                 "frame": {"showTitle": True, "title": "Avg On-Time Delivery %"}}
                    },
                    "position": {"x": 4, "y": 2, "width": 2, "height": 3}
                },
                # Chart: OTD by month (line)
                {
                    "widget": {
                        "name": "chart-otd-trend",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_scheduling",
                            "fields": [
                                {"name": "month",          "expression": "`month`"},
                                {"name": "avg(otd_pct)",   "expression": "AVG(`otd_pct`)"}
                            ],
                            "disaggregated": False
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "line",
                            "encodings": {
                                "x": {"fieldName": "month",        "scale": {"type": "categorical"}, "displayName": "Month"},
                                "y": {"fieldName": "avg(otd_pct)", "scale": {"type": "quantitative"}, "displayName": "OTD %"}
                            },
                            "frame": {"showTitle": True, "title": "On-Time Delivery Trend"}
                        }
                    },
                    "position": {"x": 0, "y": 5, "width": 3, "height": 5}
                },
                # Chart: Farm-out cost by work center (bar)
                {
                    "widget": {
                        "name": "chart-farmout-wc",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_scheduling",
                            "fields": [
                                {"name": "work_center",               "expression": "`work_center`"},
                                {"name": "sum(farm_out_cost_usd)",    "expression": "SUM(`farm_out_cost_usd`)"}
                            ],
                            "disaggregated": False
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "work_center",            "scale": {"type": "categorical"}, "displayName": "Work Center"},
                                "y": {"fieldName": "sum(farm_out_cost_usd)", "scale": {"type": "quantitative"}, "displayName": "Farm-Out Cost $"}
                            },
                            "frame": {"showTitle": True, "title": "Farm-Out Cost by Work Center"}
                        }
                    },
                    "position": {"x": 3, "y": 5, "width": 3, "height": 5}
                },
                # Table: Scheduling detail
                {
                    "widget": {
                        "name": "table-sched-detail",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_scheduling",
                            "fields": [
                                {"name": "month",             "expression": "`month`"},
                                {"name": "work_center",       "expression": "`work_center`"},
                                {"name": "total_orders",      "expression": "`total_orders`"},
                                {"name": "late_count",        "expression": "`late_count`"},
                                {"name": "farm_out_count",    "expression": "`farm_out_count`"},
                                {"name": "farm_out_cost_usd", "expression": "`farm_out_cost_usd`"},
                                {"name": "otd_pct",           "expression": "`otd_pct`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 2, "widgetType": "table",
                            "encodings": {"columns": [
                                {"fieldName": "month",             "displayName": "Month"},
                                {"fieldName": "work_center",       "displayName": "Work Center"},
                                {"fieldName": "total_orders",      "displayName": "Total Orders"},
                                {"fieldName": "late_count",        "displayName": "Late Orders"},
                                {"fieldName": "farm_out_count",    "displayName": "Farm-Outs"},
                                {"fieldName": "farm_out_cost_usd", "displayName": "Farm-Out Cost $"},
                                {"fieldName": "otd_pct",           "displayName": "OTD %"}
                            ]},
                            "frame": {"showTitle": True, "title": "Scheduling Detail"}
                        }
                    },
                    "position": {"x": 0, "y": 10, "width": 6, "height": 6}
                },
            ]
        },

        # ═══════════════════════════════════════════════════════
        # PAGE 3: Predictive Maintenance
        # ═══════════════════════════════════════════════════════
        {
            "name": "predictive_maintenance",
            "displayName": "Predictive Maintenance",
            "pageType": "PAGE_TYPE_CANVAS",
            "layout": [
                {"widget": {"name": "title_pm", "multilineTextboxSpec": {"lines": ["## Predictive Maintenance & Machine Health"]}},
                 "position": {"x": 0, "y": 0, "width": 6, "height": 1}},
                {"widget": {"name": "subtitle_pm", "multilineTextboxSpec": {"lines": ["Real-time health scores, anomaly detection, and maintenance urgency from IoT sensor data"]}},
                 "position": {"x": 0, "y": 1, "width": 6, "height": 1}},
                # KPI: Avg Health Score
                {
                    "widget": {
                        "name": "kpi-health",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_maintenance",
                            "fields": [{"name": "avg(avg_health_score)", "expression": "AVG(`avg_health_score`)"}],
                            "disaggregated": False
                        }}],
                        "spec": {"version": 2, "widgetType": "counter",
                                 "encodings": {"value": {"fieldName": "avg(avg_health_score)", "displayName": "Avg Health"}},
                                 "frame": {"showTitle": True, "title": "Fleet Avg Health Score"}}
                    },
                    "position": {"x": 0, "y": 2, "width": 2, "height": 3}
                },
                # KPI: Total Anomalies
                {
                    "widget": {
                        "name": "kpi-anomalies",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_maintenance",
                            "fields": [{"name": "sum(anomaly_count)", "expression": "SUM(`anomaly_count`)"}],
                            "disaggregated": False
                        }}],
                        "spec": {"version": 2, "widgetType": "counter",
                                 "encodings": {"value": {"fieldName": "sum(anomaly_count)", "displayName": "Total Anomalies"}},
                                 "frame": {"showTitle": True, "title": "Total Anomalies"}}
                    },
                    "position": {"x": 2, "y": 2, "width": 2, "height": 3}
                },
                # KPI: Max Tool Wear
                {
                    "widget": {
                        "name": "kpi-tool-wear",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_maintenance",
                            "fields": [{"name": "avg(max_tool_wear_pct)", "expression": "AVG(`max_tool_wear_pct`)"}],
                            "disaggregated": False
                        }}],
                        "spec": {"version": 2, "widgetType": "counter",
                                 "encodings": {"value": {"fieldName": "avg(max_tool_wear_pct)", "displayName": "Avg Tool Wear %"}},
                                 "frame": {"showTitle": True, "title": "Avg Tool Wear %"}}
                    },
                    "position": {"x": 4, "y": 2, "width": 2, "height": 3}
                },
                # Chart: Health score by machine (bar)
                {
                    "widget": {
                        "name": "chart-health-by-machine",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_maintenance",
                            "fields": [
                                {"name": "machine_id",        "expression": "`machine_id`"},
                                {"name": "avg_health_score",  "expression": "`avg_health_score`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "machine_id",       "scale": {"type": "categorical"}, "displayName": "Machine"},
                                "y": {"fieldName": "avg_health_score", "scale": {"type": "quantitative"}, "displayName": "Health Score"}
                            },
                            "frame": {"showTitle": True, "title": "Current Health Score by Machine"}
                        }
                    },
                    "position": {"x": 0, "y": 5, "width": 3, "height": 5}
                },
                # Chart: Maintenance urgency distribution (pie)
                {
                    "widget": {
                        "name": "chart-urgency-pie",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_maintenance",
                            "fields": [
                                {"name": "maintenance_urgency",             "expression": "`maintenance_urgency`"},
                                {"name": "count(machine_id)", "expression": "COUNT(`machine_id`)"}
                            ],
                            "disaggregated": False
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "pie",
                            "encodings": {
                                "angle":  {"fieldName": "count(machine_id)",    "scale": {"type": "quantitative"}, "displayName": "Machines"},
                                "color":  {"fieldName": "maintenance_urgency",  "scale": {"type": "categorical"},  "displayName": "Urgency"}
                            },
                            "frame": {"showTitle": True, "title": "Maintenance Urgency Distribution"}
                        }
                    },
                    "position": {"x": 3, "y": 5, "width": 3, "height": 5}
                },
                # Table: Machine health snapshot
                {
                    "widget": {
                        "name": "table-health-snapshot",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_maintenance",
                            "fields": [
                                {"name": "machine_id",          "expression": "`machine_id`"},
                                {"name": "machine_name",        "expression": "`machine_name`"},
                                {"name": "work_center",         "expression": "`work_center`"},
                                {"name": "avg_health_score",    "expression": "`avg_health_score`"},
                                {"name": "max_tool_wear_pct",   "expression": "`max_tool_wear_pct`"},
                                {"name": "anomaly_count",       "expression": "`anomaly_count`"},
                                {"name": "maintenance_urgency", "expression": "`maintenance_urgency`"},
                                {"name": "days_since_last_pm",  "expression": "`days_since_last_pm`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 2, "widgetType": "table",
                            "encodings": {"columns": [
                                {"fieldName": "machine_id",          "displayName": "Machine ID"},
                                {"fieldName": "machine_name",        "displayName": "Machine Name"},
                                {"fieldName": "work_center",         "displayName": "Work Center"},
                                {"fieldName": "avg_health_score",    "displayName": "Health Score"},
                                {"fieldName": "max_tool_wear_pct",   "displayName": "Tool Wear %"},
                                {"fieldName": "anomaly_count",       "displayName": "Anomalies"},
                                {"fieldName": "maintenance_urgency", "displayName": "Urgency"},
                                {"fieldName": "days_since_last_pm",  "displayName": "Days Since PM"}
                            ]},
                            "frame": {"showTitle": True, "title": "Machine Health Snapshot"}
                        }
                    },
                    "position": {"x": 0, "y": 10, "width": 6, "height": 6}
                },
            ]
        },

        # ═══════════════════════════════════════════════════════
        # PAGE 4: Capacity Planning
        # ═══════════════════════════════════════════════════════
        {
            "name": "capacity_planning",
            "displayName": "Capacity Planning",
            "pageType": "PAGE_TYPE_CANVAS",
            "layout": [
                {"widget": {"name": "title_cap", "multilineTextboxSpec": {"lines": ["## Capacity Planning"]}},
                 "position": {"x": 0, "y": 0, "width": 6, "height": 1}},
                {"widget": {"name": "subtitle_cap", "multilineTextboxSpec": {"lines": ["Monthly demand vs. available capacity by work center — identifies overloaded areas for investment decisions"]}},
                 "position": {"x": 0, "y": 1, "width": 6, "height": 1}},
                # KPI: Avg Capacity Util
                {
                    "widget": {
                        "name": "kpi-cap-util",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_capacity",
                            "fields": [{"name": "avg(capacity_util_pct)", "expression": "AVG(`capacity_util_pct`)"}],
                            "disaggregated": False
                        }}],
                        "spec": {"version": 2, "widgetType": "counter",
                                 "encodings": {"value": {"fieldName": "avg(capacity_util_pct)", "displayName": "Avg Capacity Util %"}},
                                 "frame": {"showTitle": True, "title": "Avg Capacity Utilization %"}}
                    },
                    "position": {"x": 0, "y": 2, "width": 2, "height": 3}
                },
                # KPI: Total Farm-Out Cost
                {
                    "widget": {
                        "name": "kpi-cap-farmout",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_capacity",
                            "fields": [{"name": "sum(farm_out_cost_usd)", "expression": "SUM(`farm_out_cost_usd`)"}],
                            "disaggregated": False
                        }}],
                        "spec": {"version": 2, "widgetType": "counter",
                                 "encodings": {"value": {"fieldName": "sum(farm_out_cost_usd)", "displayName": "Farm-Out Cost $"}},
                                 "frame": {"showTitle": True, "title": "Total Farm-Out Cost (USD)"}}
                    },
                    "position": {"x": 2, "y": 2, "width": 2, "height": 3}
                },
                # KPI: Total Demand Hrs
                {
                    "widget": {
                        "name": "kpi-demand-hrs",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_capacity",
                            "fields": [{"name": "sum(demand_hrs)", "expression": "SUM(`demand_hrs`)"}],
                            "disaggregated": False
                        }}],
                        "spec": {"version": 2, "widgetType": "counter",
                                 "encodings": {"value": {"fieldName": "sum(demand_hrs)", "displayName": "Demand Hrs"}},
                                 "frame": {"showTitle": True, "title": "Total Demand Hours"}}
                    },
                    "position": {"x": 4, "y": 2, "width": 2, "height": 3}
                },
                # Chart: Demand vs Available by Work Center (bar grouped)
                {
                    "widget": {
                        "name": "chart-demand-vs-cap",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_capacity",
                            "fields": [
                                {"name": "work_center",     "expression": "`work_center`"},
                                {"name": "sum(demand_hrs)", "expression": "SUM(`demand_hrs`)"},
                                {"name": "sum(available_hrs)", "expression": "SUM(`available_hrs`)"}
                            ],
                            "disaggregated": False
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "bar",
                            "mark": {"layout": "group"},
                            "encodings": {
                                "x": {"fieldName": "work_center",        "scale": {"type": "categorical"}, "displayName": "Work Center"},
                                "y": {"fields": [
                                    {"fieldName": "sum(demand_hrs)",     "displayName": "Demand Hrs"},
                                    {"fieldName": "sum(available_hrs)",  "displayName": "Available Hrs"}
                                ], "scale": {"type": "quantitative"}}
                            },
                            "frame": {"showTitle": True, "title": "Demand vs Capacity by Work Center"}
                        }
                    },
                    "position": {"x": 0, "y": 5, "width": 3, "height": 5}
                },
                # Chart: Load status distribution (pie)
                {
                    "widget": {
                        "name": "chart-load-status",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_capacity",
                            "fields": [
                                {"name": "load_status",       "expression": "`load_status`"},
                                {"name": "count(work_center)", "expression": "COUNT(`work_center`)"}
                            ],
                            "disaggregated": False
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "pie",
                            "encodings": {
                                "angle": {"fieldName": "count(work_center)", "scale": {"type": "quantitative"}, "displayName": "Count"},
                                "color": {"fieldName": "load_status",        "scale": {"type": "categorical"},  "displayName": "Load Status"}
                            },
                            "frame": {"showTitle": True, "title": "Load Status Distribution"}
                        }
                    },
                    "position": {"x": 3, "y": 5, "width": 3, "height": 5}
                },
                # Table
                {
                    "widget": {
                        "name": "table-capacity-detail",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_capacity",
                            "fields": [
                                {"name": "month",               "expression": "`month`"},
                                {"name": "work_center",         "expression": "`work_center`"},
                                {"name": "demand_hrs",          "expression": "`demand_hrs`"},
                                {"name": "available_hrs",       "expression": "`available_hrs`"},
                                {"name": "capacity_util_pct",   "expression": "`capacity_util_pct`"},
                                {"name": "farm_out_cost_usd",   "expression": "`farm_out_cost_usd`"},
                                {"name": "load_status",         "expression": "`load_status`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 2, "widgetType": "table",
                            "encodings": {"columns": [
                                {"fieldName": "month",             "displayName": "Month"},
                                {"fieldName": "work_center",       "displayName": "Work Center"},
                                {"fieldName": "demand_hrs",        "displayName": "Demand Hrs"},
                                {"fieldName": "available_hrs",     "displayName": "Available Hrs"},
                                {"fieldName": "capacity_util_pct", "displayName": "Util %"},
                                {"fieldName": "farm_out_cost_usd", "displayName": "Farm-Out Cost $"},
                                {"fieldName": "load_status",       "displayName": "Load Status"}
                            ]},
                            "frame": {"showTitle": True, "title": "Capacity Planning Detail"}
                        }
                    },
                    "position": {"x": 0, "y": 10, "width": 6, "height": 6}
                },
            ]
        },

        # ═══════════════════════════════════════════════════════
        # PAGE 5: Farm-Out Analysis
        # ═══════════════════════════════════════════════════════
        {
            "name": "farmout_analysis",
            "displayName": "Farm-Out Analysis",
            "pageType": "PAGE_TYPE_CANVAS",
            "layout": [
                {"widget": {"name": "title_fo", "multilineTextboxSpec": {"lines": ["## Farm-Out vs Make Analysis"]}},
                 "position": {"x": 0, "y": 0, "width": 6, "height": 1}},
                {"widget": {"name": "subtitle_fo", "multilineTextboxSpec": {"lines": ["Vendor cost comparison and make-vs-buy insights to optimize external machining decisions"]}},
                 "position": {"x": 0, "y": 1, "width": 6, "height": 1}},
                # KPI: Total Farm-Out Cost
                {
                    "widget": {
                        "name": "kpi-fo-total-cost",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_farmout",
                            "fields": [{"name": "sum(total_farm_out_cost)", "expression": "SUM(`total_farm_out_cost`)"}],
                            "disaggregated": False
                        }}],
                        "spec": {"version": 2, "widgetType": "counter",
                                 "encodings": {"value": {"fieldName": "sum(total_farm_out_cost)", "displayName": "Total Farm-Out $"}},
                                 "frame": {"showTitle": True, "title": "Total Farm-Out Cost (USD)"}}
                    },
                    "position": {"x": 0, "y": 2, "width": 2, "height": 3}
                },
                # KPI: Implied Internal Cost
                {
                    "widget": {
                        "name": "kpi-fo-internal-cost",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_farmout",
                            "fields": [{"name": "sum(implied_internal_cost)", "expression": "SUM(`implied_internal_cost`)"}],
                            "disaggregated": False
                        }}],
                        "spec": {"version": 2, "widgetType": "counter",
                                 "encodings": {"value": {"fieldName": "sum(implied_internal_cost)", "displayName": "Internal Cost $"}},
                                 "frame": {"showTitle": True, "title": "Implied Internal Cost (USD)"}}
                    },
                    "position": {"x": 2, "y": 2, "width": 2, "height": 3}
                },
                # KPI: Avg Cost Premium
                {
                    "widget": {
                        "name": "kpi-cost-premium",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_farmout",
                            "fields": [{"name": "avg(avg_cost_premium_pct)", "expression": "AVG(`avg_cost_premium_pct`)"}],
                            "disaggregated": False
                        }}],
                        "spec": {"version": 2, "widgetType": "counter",
                                 "encodings": {"value": {"fieldName": "avg(avg_cost_premium_pct)", "displayName": "Cost Premium %"}},
                                 "frame": {"showTitle": True, "title": "Avg Cost Premium %"}}
                    },
                    "position": {"x": 4, "y": 2, "width": 2, "height": 3}
                },
                # Chart: Farm-Out vs Internal cost by vendor (bar grouped)
                {
                    "widget": {
                        "name": "chart-fo-vs-internal",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_farmout",
                            "fields": [
                                {"name": "farm_out_vendor",          "expression": "`farm_out_vendor`"},
                                {"name": "total_farm_out_cost",      "expression": "`total_farm_out_cost`"},
                                {"name": "implied_internal_cost",    "expression": "`implied_internal_cost`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "bar",
                            "mark": {"layout": "group"},
                            "encodings": {
                                "x": {"fieldName": "farm_out_vendor",       "scale": {"type": "categorical"},  "displayName": "Vendor"},
                                "y": {"fields": [
                                    {"fieldName": "total_farm_out_cost",    "displayName": "Farm-Out Cost $"},
                                    {"fieldName": "implied_internal_cost",  "displayName": "Internal Cost $"}
                                ], "scale": {"type": "quantitative"}}
                            },
                            "frame": {"showTitle": True, "title": "Farm-Out vs Internal Cost by Vendor"}
                        }
                    },
                    "position": {"x": 0, "y": 5, "width": 3, "height": 5}
                },
                # Chart: Farm-out orders by vendor (bar)
                {
                    "widget": {
                        "name": "chart-fo-orders",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_farmout",
                            "fields": [
                                {"name": "farm_out_vendor", "expression": "`farm_out_vendor`"},
                                {"name": "total_orders",    "expression": "`total_orders`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "farm_out_vendor", "scale": {"type": "categorical"},  "displayName": "Vendor"},
                                "y": {"fieldName": "total_orders",    "scale": {"type": "quantitative"}, "displayName": "Total Orders"}
                            },
                            "frame": {"showTitle": True, "title": "Farm-Out Orders by Vendor"}
                        }
                    },
                    "position": {"x": 3, "y": 5, "width": 3, "height": 5}
                },
                # Table: Farm-out vendor detail
                {
                    "widget": {
                        "name": "table-farmout-detail",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_farmout",
                            "fields": [
                                {"name": "farm_out_vendor",       "expression": "`farm_out_vendor`"},
                                {"name": "total_orders",          "expression": "`total_orders`"},
                                {"name": "total_farm_out_cost",   "expression": "`total_farm_out_cost`"},
                                {"name": "implied_internal_cost", "expression": "`implied_internal_cost`"},
                                {"name": "avg_cost_premium_pct",  "expression": "`avg_cost_premium_pct`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 2, "widgetType": "table",
                            "encodings": {"columns": [
                                {"fieldName": "farm_out_vendor",       "displayName": "Vendor"},
                                {"fieldName": "total_orders",          "displayName": "Total Orders"},
                                {"fieldName": "total_farm_out_cost",   "displayName": "Farm-Out Cost $"},
                                {"fieldName": "implied_internal_cost", "displayName": "Internal Equiv $"},
                                {"fieldName": "avg_cost_premium_pct",  "displayName": "Cost Premium %"}
                            ]},
                            "frame": {"showTitle": True, "title": "Farm-Out Vendor Detail"}
                        }
                    },
                    "position": {"x": 0, "y": 10, "width": 6, "height": 5}
                },
            ]
        },
    ]
}

print("✅ Dashboard JSON built successfully")
print(f"  Pages   : {len(dashboard_def['pages'])}")
print(f"  Datasets: {len(dashboard_def['datasets'])}")

# COMMAND ----------
# MAGIC %md ## Step 4: Deploy Dashboard

# COMMAND ----------

import requests, os

# Get host from Spark conf (works reliably in both interactive and Jobs contexts)
workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "")
host          = f"https://{workspace_url}" if workspace_url else w.config.host.rstrip("/")

# Get token from notebook context with fallback to env var
try:
    ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    token = ctx.apiToken().getOrElse(None) or os.environ.get("DATABRICKS_TOKEN", w.config.token or "")
except Exception:
    token = os.environ.get("DATABRICKS_TOKEN", w.config.token or "")

me     = w.current_user.me()
parent = f"/Workspace/Users/{me.user_name}"

print(f"Host : {host}")
print(f"User : {me.user_name}")

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Create dashboard via Lakeview REST API
payload = {
    "display_name":        "smartmfg_machine_optimization_dashboard",
    "parent_path":         parent,
    "serialized_dashboard": json.dumps(dashboard_def),
    "warehouse_id":        WAREHOUSE_ID,
}
resp = requests.post(f"{host}/api/2.0/lakeview/dashboards", headers=headers, json=payload)
resp.raise_for_status()
result = resp.json()

DASHBOARD_ID  = result["dashboard_id"]
DASHBOARD_URL = f"{host}/sql/dashboardsv3/{DASHBOARD_ID}"

print(f"✅ Dashboard created successfully!")
print(f"   Name : smartmfg_machine_optimization_dashboard")
print(f"   ID   : {DASHBOARD_ID}")
print(f"   URL  : {DASHBOARD_URL}")

# COMMAND ----------
# MAGIC %md ## Step 5: Publish Dashboard

# COMMAND ----------

pub_resp = requests.post(
    f"{host}/api/2.0/lakeview/dashboards/{DASHBOARD_ID}/published",
    headers=headers,
    json={"warehouse_id": WAREHOUSE_ID, "embed_credentials": False}
)
if pub_resp.status_code in (200, 201):
    print(f"✅ Dashboard published!")
else:
    print(f"⚠️  Publish returned {pub_resp.status_code}: {pub_resp.text[:200]}")

print(f"   URL: {DASHBOARD_URL}")
