# Databricks notebook source
# MAGIC %md
# MAGIC # SmartMFG Machine Scheduling Optimization - Genie Space
# MAGIC **Notebook:** 06_genie_space.py
# MAGIC
# MAGIC Creates and configures the **SmartMFG Genie** AI/BI assistant with:
# MAGIC - 9 Silver + Gold tables from `satsen_catalog.smartmfg_machine_optimization_1`
# MAGIC - Text instructions for accurate natural language to SQL generation
# MAGIC - Join specifications for table relationships
# MAGIC - 10 curated example question → SQL pairs
# MAGIC - SQL snippets: 5 measures, 6 filters, 4 expressions
# MAGIC - 12 sample questions covering all 5 use case areas

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

import requests, json, os, uuid

CATALOG    = "satsen_catalog"
SCHEMA     = "smartmfg_machine_optimization_1"
SPACE_NAME = "SmartMFG Genie"
SPACE_ID   = "01f11d7c958210c893bc6b2289a35847"   # pre-created blank space

workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "")
host  = f"https://{workspace_url}" if workspace_url else ""

try:
    ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    token = ctx.apiToken().getOrElse(None) or os.environ.get("DATABRICKS_TOKEN", "")
except Exception:
    token = os.environ.get("DATABRICKS_TOKEN", "")

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

warehouses   = requests.get(f"{host}/api/2.0/sql/warehouses", headers=headers).json()
wh_list      = warehouses.get("warehouses", [])
warehouse    = next(
    (w for w in wh_list if w.get("state") == "RUNNING"),
    next((w for w in wh_list if w.get("state") in ("STARTING", "STOPPED")), wh_list[0] if wh_list else None)
)
WAREHOUSE_ID = warehouse["id"] if warehouse else "03560442e95cb440"

def uid():
    return uuid.uuid4().hex

print(f"Host      : {host}")
print(f"Warehouse : {WAREHOUSE_ID}")

# COMMAND ----------
# MAGIC %md ## Table Configuration

# COMMAND ----------

# Silver and Gold tables (must be sorted by identifier)
tables = sorted([
    {"identifier": f"{CATALOG}.{SCHEMA}.silver_machines",
     "description": ["Machine master: 12 CNC machines, 6 work centers (WC-MILL, WC-TURN, WC-DRILL, WC-MULTITASK, WC-TRANSFER, WC-QC). Fields: capacity, shifts, hourly rate, PM interval, status."]},
    {"identifier": f"{CATALOG}.{SCHEMA}.silver_routings",
     "description": ["Part-operation routing from engineering. Defines which machine runs each operation per part. Fields: part_number, operation_sequence, work_center, preferred_machine_id, setup_time_hrs, run_time_hrs_per_unit."]},
    {"identifier": f"{CATALOG}.{SCHEMA}.silver_work_orders",
     "description": ["Work orders from Oracle ERP. Core scheduling table. Fields: work_order_id, part_number, machine_id, due_date, scheduled_start/end, actual_start/end, status, priority, is_farm_out, farm_out_vendor, farm_out_cost_usd, is_late, standard_total_hrs."]},
    {"identifier": f"{CATALOG}.{SCHEMA}.silver_sensor_data",
     "description": ["Hourly IoT sensor readings from connected machines. Fields: machine_id, reading_timestamp, temperature_celsius, vibration_mm_s, spindle_speed_rpm, power_consumption_kw, tool_wear_pct, health_score, anomaly_flag, anomaly_type."]},
    {"identifier": f"{CATALOG}.{SCHEMA}.gold_machine_utilization",
     "description": ["Machine KPIs by period. Fields: machine_id, period_day/week/month/year, scheduled_hrs, actual_hrs, utilization_pct, efficiency_pct, revenue_usd, work_order_count, capacity_hrs_per_day."]},
    {"identifier": f"{CATALOG}.{SCHEMA}.gold_scheduling_performance",
     "description": ["Scheduling KPIs by work center and priority. Fields: period_week/month/year, work_center, priority, total_orders, farm_out_count, late_count, completed_count, total_farm_out_cost, on_time_delivery_pct."]},
    {"identifier": f"{CATALOG}.{SCHEMA}.gold_predictive_maintenance",
     "description": ["Daily sensor aggregates per machine. Fields: machine_id, reading_date, avg_health_score, max_tool_wear_pct, avg_vibration, anomaly_count, maintenance_urgency (Normal/Warning/Critical), days_since_last_pm."]},
    {"identifier": f"{CATALOG}.{SCHEMA}.gold_capacity_planning",
     "description": ["Monthly capacity by work center. Fields: work_center, period_month/year, demand_hrs, available_hrs, farm_out_hrs, farm_out_cost_usd, capacity_utilization_pct, farm_out_rate_pct, load_status."]},
    {"identifier": f"{CATALOG}.{SCHEMA}.gold_farmout_analysis",
     "description": ["Farm-out vendor cost by part. Fields: farm_out_vendor, part_number, period_month/year, farm_out_orders, total_farm_out_cost, implied_internal_cost, cost_premium_pct."]},
], key=lambda x: x["identifier"])

# COMMAND ----------
# MAGIC %md ## Instructions

# COMMAND ----------

text_instructions = [{
    "id": uid(),
    "content": [
        "You are an expert analytics assistant for SmartMFG precision machining.\n",
        "The data covers 12 CNC machines across 6 work centers: WC-MILL, WC-TURN, WC-DRILL, WC-MULTITASK, WC-TRANSFER, WC-QC.\n",
        "\n",
        "ALWAYS use fully-qualified table names: satsen_catalog.smartmfg_machine_optimization_1.<table_name>\n",
        "\n",
        "KEY BUSINESS TERMS:\n",
        "- Farm-Out: Work sent to external vendors. Filter: is_farm_out = TRUE in silver_work_orders\n",
        "- OTD (On-Time Delivery): % of orders completed by due_date. Use on_time_delivery_pct from gold_scheduling_performance\n",
        "- PM (Preventive Maintenance): days_since_last_pm > 180 = overdue\n",
        "- Health Score: 0-100 (Critical < 40, Warning 40-65, Normal > 65)\n",
        "- Load Status: Overloaded > 90%, High Load > 75%, Normal > 50%, Underutilized < 50%\n",
        "\n",
        "TABLE GUIDE:\n",
        "- gold_machine_utilization: utilization %, efficiency %, revenue by machine/period\n",
        "- gold_scheduling_performance: OTD %, farm-out cost, late orders by work_center + priority\n",
        "- gold_predictive_maintenance: health scores, anomaly counts, tool wear, maintenance urgency\n",
        "- gold_capacity_planning: demand vs available capacity, farm-out rate, load status\n",
        "- gold_farmout_analysis: vendor cost comparison, make-vs-buy premium\n",
        "- silver_work_orders: granular work order detail, individual status and costs\n",
        "\n",
        "DATE RULES:\n",
        "- period_month and period_week in Gold tables are DATE types (DATE_TRUNC)\n",
        "- Display format: DATE_FORMAT(period_month, 'yyyy-MM')\n",
        "- For this year: period_year = YEAR(CURRENT_DATE())\n",
        "- For latest health: WITH latest AS (SELECT machine_id, MAX(reading_date) as d FROM gold_predictive_maintenance GROUP BY 1) JOIN on machine_id and reading_date\n",
        "\n",
        "FORMATTING: ROUND percentages to 1 decimal, monetary to 0-2 decimals. COALESCE(column, 0) for NULLs."
    ]
}]

# COMMAND ----------
# MAGIC %md ## SQL Snippets

# COMMAND ----------

sql_measures = sorted([
    {"id": uid(), "alias": "utilization_pct_rounded",  "sql": ["ROUND(utilization_pct, 1)"]},
    {"id": uid(), "alias": "farm_out_rate_pct",        "sql": ["ROUND(farm_out_hrs / NULLIF(demand_hrs,0) * 100, 1)"]},
    {"id": uid(), "alias": "standard_total_hrs",       "sql": ["ROUND(setup_time_hrs + run_time_hrs_per_unit * order_qty, 2)"]},
    {"id": uid(), "alias": "days_since_last_pm_calc",  "sql": ["DATEDIFF(CURRENT_DATE(), last_pm_date)"]},
    {"id": uid(), "alias": "cost_premium_pct_calc",    "sql": ["ROUND((total_farm_out_cost - implied_internal_cost) / NULLIF(total_farm_out_cost,0) * 100, 1)"]},
], key=lambda x: x["id"])

sql_filters = sorted([
    {"id": uid(), "display_name": "farm_out_orders",     "sql": ["is_farm_out = TRUE"]},
    {"id": uid(), "display_name": "late_orders",         "sql": ["is_late = TRUE"]},
    {"id": uid(), "display_name": "active_machines",     "sql": ["status = 'Active'"]},
    {"id": uid(), "display_name": "critical_health",     "sql": ["maintenance_urgency = 'Critical'"]},
    {"id": uid(), "display_name": "overloaded_wc",       "sql": ["load_status = 'Overloaded'"]},
    {"id": uid(), "display_name": "high_priority_orders","sql": ["priority = 'High'"]},
], key=lambda x: x["id"])

sql_expressions = sorted([
    {"id": uid(), "alias": "health_category",    "sql": ["CASE WHEN avg_health_score < 40 THEN 'Critical' WHEN avg_health_score < 65 THEN 'Warning' ELSE 'Normal' END"]},
    {"id": uid(), "alias": "load_status_derived","sql": ["CASE WHEN demand_hrs/NULLIF(available_hrs,0) > 0.90 THEN 'Overloaded' WHEN demand_hrs/NULLIF(available_hrs,0) > 0.75 THEN 'High Load' WHEN demand_hrs/NULLIF(available_hrs,0) > 0.50 THEN 'Normal' ELSE 'Underutilized' END"]},
    {"id": uid(), "alias": "month_display",      "sql": ["DATE_FORMAT(period_month, 'yyyy-MM')"]},
    {"id": uid(), "alias": "otd_pct_computed",   "sql": ["ROUND(SUM(CASE WHEN status='Complete' AND is_late=FALSE THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN status='Complete' THEN 1 ELSE 0 END),0)*100,1)"]},
], key=lambda x: x["id"])

# COMMAND ----------
# MAGIC %md ## Example Question → SQL Pairs

# COMMAND ----------

example_sqls = sorted([
    {"id": uid(),
     "question": ["Which machines are in Critical health status right now?"],
     "sql": [f"WITH latest AS (\n  SELECT machine_id, MAX(reading_date) AS d\n  FROM {CATALOG}.{SCHEMA}.gold_predictive_maintenance GROUP BY machine_id\n)\nSELECT p.machine_id, p.machine_name, p.work_center,\n  ROUND(p.avg_health_score,1) AS health_score,\n  ROUND(p.max_tool_wear_pct,1) AS tool_wear_pct,\n  p.anomaly_count, p.maintenance_urgency, p.days_since_last_pm\nFROM {CATALOG}.{SCHEMA}.gold_predictive_maintenance p\nJOIN latest l ON p.machine_id=l.machine_id AND p.reading_date=l.d\nWHERE p.maintenance_urgency = 'Critical'\nORDER BY p.avg_health_score"]},
    {"id": uid(),
     "question": ["What is on-time delivery % by work center this year?"],
     "sql": [f"SELECT work_center,\n  SUM(total_orders) AS total_orders,\n  SUM(late_count) AS late_orders,\n  ROUND(AVG(on_time_delivery_pct),1) AS avg_otd_pct,\n  ROUND(SUM(total_farm_out_cost),0) AS farm_out_cost\nFROM {CATALOG}.{SCHEMA}.gold_scheduling_performance\nWHERE period_year = YEAR(CURRENT_DATE())\nGROUP BY work_center\nORDER BY avg_otd_pct"]},
    {"id": uid(),
     "question": ["Which work centers are overloaded and need investment?"],
     "sql": [f"SELECT work_center,\n  COUNT(DISTINCT period_month) AS months_overloaded,\n  ROUND(AVG(capacity_utilization_pct),1) AS avg_util_pct,\n  ROUND(SUM(farm_out_cost_usd),0) AS total_farm_out_cost\nFROM {CATALOG}.{SCHEMA}.gold_capacity_planning\nWHERE load_status IN ('Overloaded','High Load')\nGROUP BY work_center\nORDER BY months_overloaded DESC, total_farm_out_cost DESC"]},
    {"id": uid(),
     "question": ["Show farm-out cost vs internal manufacturing cost by vendor"],
     "sql": [f"SELECT farm_out_vendor,\n  SUM(farm_out_orders) AS total_orders,\n  ROUND(SUM(total_farm_out_cost),0) AS farm_out_cost,\n  ROUND(SUM(implied_internal_cost),0) AS internal_equiv_cost,\n  ROUND(AVG(cost_premium_pct),1) AS avg_premium_pct\nFROM {CATALOG}.{SCHEMA}.gold_farmout_analysis\nGROUP BY farm_out_vendor\nORDER BY farm_out_cost DESC"]},
    {"id": uid(),
     "question": ["Show machines overdue for preventive maintenance"],
     "sql": [f"SELECT machine_id, machine_name, work_center,\n  MAX(days_since_last_pm) AS days_since_pm,\n  ROUND(AVG(avg_health_score),1) AS avg_health,\n  SUM(anomaly_count) AS total_anomalies\nFROM {CATALOG}.{SCHEMA}.gold_predictive_maintenance\nGROUP BY 1,2,3\nHAVING MAX(days_since_last_pm) > 180\nORDER BY MAX(days_since_last_pm) DESC"]},
    {"id": uid(),
     "question": ["Show monthly machine utilization trend for all machines"],
     "sql": [f"SELECT DATE_FORMAT(period_month,'yyyy-MM') AS month, machine_id,\n  ROUND(utilization_pct,1) AS utilization_pct,\n  ROUND(efficiency_pct,1) AS efficiency_pct,\n  work_order_count, ROUND(revenue_usd,0) AS revenue_usd\nFROM {CATALOG}.{SCHEMA}.gold_machine_utilization\nORDER BY period_month DESC, machine_id"]},
    {"id": uid(),
     "question": ["Which parts have the highest external machining cost?"],
     "sql": [f"SELECT part_number, part_description,\n  SUM(farm_out_orders) AS total_orders,\n  ROUND(SUM(total_farm_out_cost),0) AS total_cost,\n  ROUND(AVG(cost_premium_pct),1) AS avg_premium_pct\nFROM {CATALOG}.{SCHEMA}.gold_farmout_analysis\nGROUP BY 1,2\nORDER BY total_cost DESC\nLIMIT 10"]},
    {"id": uid(),
     "question": ["Show yearly capacity summary by work center"],
     "sql": [f"SELECT period_year, work_center,\n  ROUND(SUM(demand_hrs),0) AS total_demand_hrs,\n  ROUND(SUM(farm_out_hrs),0) AS farm_out_hrs,\n  ROUND(SUM(farm_out_cost_usd),0) AS farm_out_cost,\n  ROUND(AVG(capacity_utilization_pct),1) AS avg_util_pct\nFROM {CATALOG}.{SCHEMA}.gold_capacity_planning\nGROUP BY 1,2\nORDER BY 1 DESC, farm_out_cost DESC"]},
    {"id": uid(),
     "question": ["Show work order scheduling detail with farm-out flag"],
     "sql": [f"SELECT work_order_id, part_number, part_description,\n  work_center, machine_id,\n  DATE_FORMAT(due_date,'yyyy-MM-dd') AS due_date,\n  status, priority, is_farm_out, is_late,\n  farm_out_vendor, ROUND(farm_out_cost_usd,2) AS farm_out_cost\nFROM {CATALOG}.{SCHEMA}.silver_work_orders\nORDER BY due_date ASC, priority DESC"]},
    {"id": uid(),
     "question": ["Show weekly health score trends to identify machine degradation"],
     "sql": [f"SELECT machine_id, machine_name, reading_year, reading_week,\n  ROUND(avg_health_score,1) AS avg_health_score,\n  ROUND(max_tool_wear_pct,1) AS max_tool_wear,\n  anomaly_count, maintenance_urgency\nFROM {CATALOG}.{SCHEMA}.gold_predictive_maintenance\nORDER BY machine_id, reading_year, reading_week"]},
], key=lambda x: x["id"])

# COMMAND ----------
# MAGIC %md ## Sample Questions

# COMMAND ----------

sample_questions = sorted([
    {"id": uid(), "question": ["Which machines have the highest utilization this year?"]},
    {"id": uid(), "question": ["What is the on-time delivery percentage by work center?"]},
    {"id": uid(), "question": ["Which machines are in Critical health status?"]},
    {"id": uid(), "question": ["Show total farm-out costs by vendor this year"]},
    {"id": uid(), "question": ["Which work centers are overloaded and need more capacity?"]},
    {"id": uid(), "question": ["What machines are overdue for preventive maintenance?"]},
    {"id": uid(), "question": ["Show the monthly OTD trend for the last 12 months"]},
    {"id": uid(), "question": ["Which parts have the highest farm-out dependency?"]},
    {"id": uid(), "question": ["What is our farm-out cost vs implied internal cost by vendor?"]},
    {"id": uid(), "question": ["Show weekly health score trends for all machines"]},
    {"id": uid(), "question": ["Which work orders are late and what is their priority?"]},
    {"id": uid(), "question": ["Compare scheduled vs actual hours for each machine this month"]},
], key=lambda x: x["id"])

# COMMAND ----------
# MAGIC %md ## Create/Update Genie Space

# COMMAND ----------

serialized_space = {
    "version": 2,
    "config": {
        "sample_questions": sample_questions,
    },
    "data_sources": {
        "tables": tables,
    },
    "instructions": {
        "text_instructions": text_instructions,
        "example_question_sqls": example_sqls,
        "sql_snippets": {
            "filters":     sql_filters,
            "expressions": sql_expressions,
            "measures":    sql_measures,
        },
    },
}

space_payload = {
    "warehouse_id":    WAREHOUSE_ID,
    "title":           SPACE_NAME,
    "description":     "AI-powered analytics for SmartMFG machine scheduling optimization. Ask about machine utilization, scheduling OTD, predictive maintenance, capacity planning, and farm-out decisions.",
    "serialized_space": json.dumps(serialized_space),
}

# Update existing space (created via CLI with blank content)
resp = requests.patch(
    f"{host}/api/2.0/genie/spaces/{SPACE_ID}",
    headers=headers,
    json=space_payload,
)

if resp.status_code in (200, 201):
    result = resp.json()
    print("=" * 60)
    print(f"  SmartMFG Genie Space — Ready")
    print("=" * 60)
    print(f"  Name      : {SPACE_NAME}")
    print(f"  Space ID  : {SPACE_ID}")
    print(f"  Tables    : {len(tables)} (4 Silver + 5 Gold)")
    print(f"  Questions : {len(sample_questions)} sample questions")
    print(f"  SQL       : {len(example_sqls)} curated Q→SQL pairs")
    print(f"  Measures  : {len(sql_measures)} | Filters: {len(sql_filters)} | Expressions: {len(sql_expressions)}")
    print(f"  URL       : {host}/genie/spaces/{SPACE_ID}")
    print("=" * 60)
    print("\nTry asking Genie:")
    print("  • Which machines are in Critical health status?")
    print("  • Show me OTD % by work center for the last 6 months")
    print("  • Which work centers should we invest in adding capacity?")
    print("  • What is our total farm-out cost by vendor this year?")
    print("=" * 60)
else:
    print(f"❌ API error {resp.status_code}: {resp.text[:500]}")
