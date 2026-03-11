# Databricks notebook source
# MAGIC %md
# MAGIC # SmartMFG Machine Scheduling Optimization - Column & Table Descriptions
# MAGIC **Notebook:** 05_column_descriptions.py
# MAGIC
# MAGIC Adds rich metadata comments to all Silver and Gold tables in Unity Catalog.
# MAGIC Good column descriptions improve:
# MAGIC - Genie Space query generation accuracy
# MAGIC - Data discoverability in Unity Catalog
# MAGIC - Self-service analytics

# COMMAND ----------

import requests, json, os

CATALOG = "satsen_catalog"
SCHEMA  = "smartmfg_machine_optimization_1"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# Resolve host and token for Unity Catalog REST API
workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "")
host  = f"https://{workspace_url}" if workspace_url else ""
try:
    ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    token = ctx.apiToken().getOrElse(None) or os.environ.get("DATABRICKS_TOKEN", "")
except Exception:
    token = os.environ.get("DATABRICKS_TOKEN", "")

UC_HEADERS = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def set_table_comment(catalog, schema, table, comment):
    """Use COMMENT ON TABLE for all table types (streaming, MV, regular Delta)."""
    try:
        spark.sql(f"COMMENT ON TABLE `{catalog}`.`{schema}`.`{table}` IS '{comment.replace(chr(39), chr(39)+chr(39))}'")
        return True
    except Exception as e:
        print(f"  ⚠️  Table comment failed for {table}: {e}")
        return False

def set_column_comments_via_api(catalog, schema, table, col_comments: dict):
    """
    Update column descriptions via Unity Catalog REST API.
    Works for streaming tables, materialized views, and Delta tables.
    """
    full_name = f"{catalog}.{schema}.{table}"
    # First fetch current column info to preserve all fields
    get_resp = requests.get(
        f"{host}/api/2.0/unity-catalog/tables/{full_name}",
        headers=UC_HEADERS
    )
    if get_resp.status_code != 200:
        print(f"  ❌ Cannot fetch schema for {table}: {get_resp.status_code} {get_resp.text[:200]}")
        return 0

    table_info = get_resp.json()
    columns    = table_info.get("columns", [])

    updated_cols = []
    update_count = 0
    for col in columns:
        col_name = col.get("name", "")
        if col_name in col_comments:
            col["comment"] = col_comments[col_name]
            update_count += 1
        updated_cols.append(col)

    if update_count == 0:
        print(f"  ⚠️  No matching columns found in {table}")
        return 0

    patch_resp = requests.patch(
        f"{host}/api/2.0/unity-catalog/tables/{full_name}",
        headers=UC_HEADERS,
        json={"columns": updated_cols}
    )
    if patch_resp.status_code in (200, 201):
        return update_count
    else:
        print(f"  ❌ Column update failed for {table}: {patch_resp.status_code} {patch_resp.text[:300]}")
        return 0

print(f"✅ Context set: {CATALOG}.{SCHEMA}")
print(f"   Host: {host}")

# COMMAND ----------
# MAGIC %md ## Silver Tables

# COMMAND ----------
# MAGIC %md ### silver_machines

# COMMAND ----------

set_table_comment(CATALOG, SCHEMA, "silver_machines",
    "Cleaned and validated machine master data. One row per machine. Used for capacity planning, scheduling, and predictive maintenance joins.")

col_comments_silver_machines = {
    "machine_id":                    "Unique machine identifier (e.g. MCH-001). Primary key. Used as foreign key in work orders and sensor data tables.",
    "machine_name":                  "Full commercial name of the machine (e.g. Haas VF-4SS). Identifies the specific model and brand.",
    "machine_type":                  "Category of machining operation performed (e.g. CNC Vertical Machining Center, CNC Lathe). Used to group machines by capability.",
    "work_center":                   "Shop-floor work center code where the machine is located (e.g. WC-MILL, WC-TURN, WC-DRILL). Links to work orders and capacity planning.",
    "num_shifts":                    "Number of production shifts the machine operates per day (1, 2, or 3). Used to compute available capacity.",
    "capacity_hrs_per_shift":        "Available productive hours per shift for this machine (typically 8 hours). Multiply by num_shifts to get daily capacity.",
    "vendor":                        "Machine manufacturer / OEM vendor name (e.g. Haas, Mazak, DMG MORI).",
    "installation_year":             "Year the machine was commissioned and placed in service. Used to estimate age and depreciation.",
    "status":                        "Current operational status: Active (producing), Maintenance (down for repair), or Idle (available but not scheduled).",
    "hourly_rate_usd":               "Fully-loaded machine cost per hour in USD, including labor, tooling, and overhead. Used for cost and revenue calculations.",
    "preventive_maint_interval_hrs": "Manufacturer-recommended hours between preventive maintenance events (e.g. 500, 750, 1000 hours).",
    "last_pm_date":                  "Date of the most recent completed preventive maintenance event. Used to calculate days since last PM.",
    "notes":                         "Free-text notes about the machine, including installation history or special configurations.",
    "capacity_hrs_per_day":          "Derived: total available hours per day = num_shifts × capacity_hrs_per_shift. Used as denominator in utilization calculations.",
    "_ingested_at":                  "Timestamp when this record was ingested into the Bronze layer by the DLT pipeline.",
    "_source_file":                  "File path of the source CSV file in the landing volume from which this record was ingested.",
}

cnt = set_column_comments_via_api(CATALOG, SCHEMA, "silver_machines", col_comments_silver_machines)
print(f"✅ silver_machines: {cnt} column comments set")

# COMMAND ----------
# MAGIC %md ### silver_routings

# COMMAND ----------

set_table_comment(CATALOG, SCHEMA, "silver_routings",
    "Cleaned part routing and operation sequence data from the engineering system. Defines which operations and machines are required to manufacture each part.")

col_comments_silver_routings = {
    "routing_id":               "Unique routing step identifier (e.g. RTG-0001). Primary key of the routing table.",
    "part_number":              "Engineering part number (e.g. PN-1001). Foreign key linking to work orders. One part may have multiple routing steps.",
    "part_description":         "Human-readable name of the manufactured part (e.g. Cylinder Head, Drive Shaft).",
    "operation_sequence":       "Sequence number of this operation in the manufacturing process (10, 20, 30...). Operations must be performed in this order.",
    "operation_code":           "Standardized operation code (e.g. OP-010 Rough Turning, OP-030 Milling). Identifies the type of machining step.",
    "operation_description":    "Human-readable description of the machining operation (e.g. Rough Turning, Finish Milling, Drilling & Tapping).",
    "work_center":              "Work center required to perform this operation (e.g. WC-TURN, WC-MILL). Used for capacity planning and scheduling.",
    "preferred_machine_id":     "Preferred or primary machine ID for this routing step. Used in scheduling to assign work orders to specific machines.",
    "setup_time_hrs":           "Time in hours required to set up the machine before running this operation. Included in total work order time calculations.",
    "run_time_hrs_per_unit":    "Standard run time per part in hours for this operation. Multiply by order_qty to get total run time.",
    "scrap_rate_pct":           "Expected scrap / defect rate as a percentage for this routing step. Used in capacity planning to account for rework.",
    "_ingested_at":             "Timestamp when this record was ingested into the Bronze layer by the DLT pipeline.",
    "_source_file":             "File path of the source CSV file in the landing volume from which this record was ingested.",
}

cnt = set_column_comments_via_api(CATALOG, SCHEMA, "silver_routings", col_comments_silver_routings)
print(f"✅ silver_routings: {cnt} column comments set")

# COMMAND ----------
# MAGIC %md ### silver_work_orders

# COMMAND ----------

set_table_comment(CATALOG, SCHEMA, "silver_work_orders",
    "Cleaned and enriched work order data from Oracle ERP. One row per work order operation. Core table for scheduling analysis, OTD, and farm-out cost tracking.")

col_comments_silver_wo = {
    "work_order_id":            "Unique work order identifier from Oracle ERP (e.g. WO-00001). Primary key.",
    "part_number":              "Engineering part number being manufactured (e.g. PN-1001). Links to silver_routings for standard times.",
    "part_description":         "Human-readable part name (e.g. Cylinder Head). Denormalized from routing master for reporting convenience.",
    "order_qty":                "Quantity of parts to be manufactured in this work order.",
    "operation_code":           "Operation code being performed (e.g. OP-030 Milling). Identifies the machining step.",
    "operation_description":    "Human-readable description of the operation being performed.",
    "work_center":              "Work center responsible for this operation (e.g. WC-MILL, WC-TURN). Used for capacity loading.",
    "machine_id":               "Machine assigned to this work order. NULL for farm-out orders. Foreign key to silver_machines.",
    "due_date":                 "Customer or production-required completion date for this work order.",
    "scheduled_start":          "Planned start timestamp from the production schedule.",
    "scheduled_end":            "Planned end timestamp based on standard run and setup times.",
    "actual_start":             "Actual start timestamp recorded at the machine (NULL if not yet started).",
    "actual_end":               "Actual completion timestamp (NULL if not yet complete). Used to calculate lateness.",
    "setup_time_hrs":           "Standard setup time in hours for this work order operation.",
    "run_time_hrs_per_unit":    "Standard run time per part unit in hours.",
    "actual_total_hrs":         "Actual total hours spent on this work order (NULL until complete). Compared to standard_total_hrs for efficiency.",
    "status":                   "Current work order status: Open, In Progress, Complete, or Farm-Out.",
    "priority":                 "Scheduling priority: High, Medium, or Low. High-priority orders are escalated in scheduling.",
    "farm_out_vendor":          "External machining vendor name for Farm-Out orders (e.g. Precision Parts Inc.). NULL for in-house orders.",
    "farm_out_cost_usd":        "Total external machining cost paid to the farm-out vendor in USD. NULL for in-house orders.",
    "source_system":            "Source ERP system that originated this work order (Oracle ERP).",
    "is_farm_out":              "Boolean flag: TRUE if this work order was sent to an external machining vendor rather than run in-house.",
    "is_late":                  "Boolean flag: TRUE if a completed work order finished after its due_date.",
    "standard_total_hrs":       "Derived: total planned hours = setup_time_hrs + (run_time_hrs_per_unit × order_qty). Used as the capacity demand input.",
    "_ingested_at":             "Timestamp when this record was ingested into the Bronze layer by the DLT pipeline.",
    "_source_file":             "File path of the source CSV file in the landing volume from which this record was ingested.",
}

cnt = set_column_comments_via_api(CATALOG, SCHEMA, "silver_work_orders", col_comments_silver_wo)
print(f"✅ silver_work_orders: {cnt} column comments set")

# COMMAND ----------
# MAGIC %md ### silver_sensor_data

# COMMAND ----------

set_table_comment(CATALOG, SCHEMA, "silver_sensor_data",
    "Cleaned hourly IoT sensor readings from shop-floor connected machines. Used for predictive maintenance, anomaly detection, and machine health scoring.")

col_comments_silver_sensor = {
    "sensor_id":                "Unique sensor reading identifier (UUID). Primary key.",
    "machine_id":               "Machine that produced this sensor reading. Foreign key to silver_machines.",
    "reading_timestamp":        "Timestamp of the sensor reading (hourly cadence). Use for time-series trend analysis.",
    "temperature_celsius":      "Machine spindle / cutting zone temperature in degrees Celsius. High values (>80°C) indicate thermal stress.",
    "vibration_mm_s":           "Machine vibration level in millimeters per second (mm/s). Values >4.0 mm/s indicate excessive vibration and potential bearing wear.",
    "spindle_speed_rpm":        "Current spindle rotation speed in revolutions per minute (RPM). 0 when machine is idle.",
    "power_consumption_kw":     "Electrical power drawn by the machine in kilowatts (kW). Elevated power at low spindle speed can indicate mechanical drag.",
    "coolant_flow_lpm":         "Coolant flow rate in liters per minute (L/min). Low values (<3 L/min during production) indicate coolant system issues.",
    "tool_wear_pct":            "Cumulative estimated tool wear as a percentage (0-100%). Values >80% indicate the tool should be replaced to avoid part defects.",
    "machine_status":           "Operational state at time of reading: Running (in production) or Idle (off-shift or between jobs).",
    "anomaly_flag":             "Boolean flag: TRUE if any sensor parameter exceeded a health threshold during this reading.",
    "anomaly_type":             "Type of anomaly detected: High Temperature, Excessive Vibration, High Tool Wear, or Low Coolant Flow. NULL when anomaly_flag is FALSE.",
    "health_score":             "Composite machine health score from 0-100, calculated from all sensor parameters. Scores below 65 indicate Warning; below 40 indicate Critical.",
    "reading_year":             "Calendar year of the sensor reading. Used for yearly aggregation and trend analysis.",
    "reading_month":            "Calendar month (1-12) of the sensor reading. Used for monthly aggregation.",
    "reading_week":             "ISO week number (1-53) of the sensor reading. Used for weekly trend analysis.",
    "_ingested_at":             "Timestamp when this record was ingested into the Bronze layer by the DLT pipeline.",
    "_source_file":             "File path of the source CSV file in the landing volume from which this record was ingested.",
}

cnt = set_column_comments_via_api(CATALOG, SCHEMA, "silver_sensor_data", col_comments_silver_sensor)
print(f"✅ silver_sensor_data: {cnt} column comments set")

# COMMAND ----------
# MAGIC %md ## Gold Tables

# COMMAND ----------
# MAGIC %md ### gold_machine_utilization

# COMMAND ----------

set_table_comment(CATALOG, SCHEMA, "gold_machine_utilization",
    "Machine utilization KPIs aggregated by day, week, month, and year. One row per machine per period. Primary table for scheduling efficiency and revenue tracking dashboards.")

col_comments_gold_util = {
    "machine_id":           "Machine identifier. Foreign key to silver_machines. Group or filter by this to compare machine performance.",
    "capacity_hrs_per_day": "Total available machine hours per day based on num_shifts × capacity_hrs_per_shift from the machine master.",
    "hourly_rate_usd":      "Fully-loaded machine cost rate in USD per hour. Used to compute revenue_usd.",
    "period_day":           "Start of the calendar day (DATE_TRUNC DAY) for daily-level aggregation.",
    "period_week":          "Start of the ISO week (DATE_TRUNC WEEK) for weekly trend analysis.",
    "period_month":         "Start of the calendar month (DATE_TRUNC MONTH) for monthly reporting.",
    "period_year":          "Calendar year (integer) for annual summaries and year-over-year comparison.",
    "scheduled_hrs":        "Total planned machine hours from work orders scheduled during this period.",
    "actual_hrs":           "Total actual machine hours recorded for completed work orders in this period. Zero for future periods.",
    "work_order_count":     "Number of work orders scheduled for this machine during the period.",
    "utilization_pct":      "Utilization percentage: scheduled_hrs ÷ capacity_hrs_per_day × 100. Values >90% indicate potential overload; <50% indicates underutilization.",
    "efficiency_pct":       "Efficiency percentage: scheduled_hrs ÷ actual_hrs × 100. Values >100% mean the job finished faster than planned.",
    "revenue_usd":          "Revenue contribution: actual (or scheduled) hours × hourly_rate_usd. Represents the value-add attributed to this machine.",
}

cnt = set_column_comments_via_api(CATALOG, SCHEMA, "gold_machine_utilization", col_comments_gold_util)
print(f"✅ gold_machine_utilization: {cnt} column comments set")

# COMMAND ----------
# MAGIC %md ### gold_scheduling_performance

# COMMAND ----------

set_table_comment(CATALOG, SCHEMA, "gold_scheduling_performance",
    "Weekly, monthly, and yearly scheduling KPIs by work center and priority. Tracks on-time delivery (OTD), farm-out costs, and late order counts to measure scheduling effectiveness.")

col_comments_gold_sched = {
    "period_week":          "Start of the ISO week (DATE_TRUNC WEEK) for weekly scheduling analysis.",
    "period_month":         "Start of the calendar month (DATE_TRUNC MONTH) for monthly OTD reporting.",
    "period_year":          "Calendar year for annual scheduling performance summaries.",
    "work_center":          "Shop-floor work center (e.g. WC-MILL, WC-TURN). Filter to analyze specific production areas.",
    "priority":             "Work order priority: High, Medium, or Low. High-priority OTD is the most critical business metric.",
    "total_orders":         "Total number of work orders due in this period for this work center and priority.",
    "farm_out_count":       "Number of work orders sent to external vendors (Farm-Out status) in this period.",
    "late_count":           "Number of completed work orders that finished after their due date in this period.",
    "completed_count":      "Number of work orders completed in this period.",
    "total_farm_out_cost":  "Total USD cost paid to external machining vendors for Farm-Out orders in this period.",
    "avg_actual_hrs":       "Average actual machine hours for completed work orders in this period. Compare to standard hours for efficiency insight.",
    "total_standard_hrs":   "Sum of planned (standard) hours for all work orders in this period. Represents total capacity demand.",
    "on_time_delivery_pct": "On-Time Delivery percentage: (completed on-time ÷ total completed) × 100. Target is typically >95%.",
}

cnt = set_column_comments_via_api(CATALOG, SCHEMA, "gold_scheduling_performance", col_comments_gold_sched)
print(f"✅ gold_scheduling_performance: {cnt} column comments set")

# COMMAND ----------
# MAGIC %md ### gold_predictive_maintenance

# COMMAND ----------

set_table_comment(CATALOG, SCHEMA, "gold_predictive_maintenance",
    "Daily aggregated sensor health metrics by machine. Summarizes temperature, vibration, tool wear, and health scores to identify machines at risk and schedule preventive maintenance.")

col_comments_gold_pm = {
    "machine_id":           "Machine identifier. Foreign key to silver_machines. One row per machine per reading_date.",
    "machine_name":         "Commercial name of the machine. Denormalized from silver_machines for reporting.",
    "machine_type":         "Machine category (e.g. CNC Lathe). Denormalized from silver_machines.",
    "work_center":          "Work center where the machine is located. Used to aggregate health metrics by production area.",
    "reading_year":         "Calendar year of the sensor readings. Used for yearly trend analysis.",
    "reading_month":        "Calendar month (1-12) of the sensor readings.",
    "reading_week":         "ISO week number of the sensor readings. Used to identify weekly degradation trends.",
    "reading_date":         "Calendar date (truncated to day) for daily health snapshots.",
    "sensor_readings":      "Total number of hourly sensor readings aggregated into this daily record.",
    "avg_temp_c":           "Average machine temperature in Celsius for the day. Trend increases indicate thermal degradation.",
    "max_temp_c":           "Peak temperature recorded during the day in Celsius. Single-hour spikes may indicate cooling issues.",
    "avg_vibration":        "Average vibration level in mm/s for the day. Sustained increases indicate bearing or spindle wear.",
    "max_vibration":        "Peak vibration level in mm/s for the day. Values >4.0 mm/s indicate excessive vibration.",
    "avg_spindle_rpm":      "Average spindle speed in RPM during production hours.",
    "avg_power_kw":         "Average power consumption in kilowatts during production hours.",
    "avg_coolant_lpm":      "Average coolant flow rate in L/min during production. Low values indicate potential coolant system issues.",
    "max_tool_wear_pct":    "Highest tool wear percentage recorded during the day (0-100%). Values >80% require immediate tool change.",
    "avg_health_score":     "Average composite health score (0-100) for the day. <65 = Warning; <40 = Critical.",
    "min_health_score":     "Lowest health score recorded during the day. Used to identify momentary critical events.",
    "anomaly_count":        "Number of hourly sensor readings that triggered an anomaly flag during the day.",
    "maintenance_urgency":  "Derived maintenance priority: Normal, Warning, or Critical. Based on min_health_score and max_tool_wear_pct thresholds.",
    "days_since_last_pm":   "Number of days elapsed since the most recent preventive maintenance event. Compare to preventive_maint_interval_hrs threshold.",
}

cnt = set_column_comments_via_api(CATALOG, SCHEMA, "gold_predictive_maintenance", col_comments_gold_pm)
print(f"✅ gold_predictive_maintenance: {cnt} column comments set")

# COMMAND ----------
# MAGIC %md ### gold_capacity_planning

# COMMAND ----------

set_table_comment(CATALOG, SCHEMA, "gold_capacity_planning",
    "Monthly capacity supply vs demand analysis by work center. Used to identify overloaded areas, justify equipment investments, and evaluate farm-out reduction strategies.")

col_comments_gold_cap = {
    "work_center":              "Shop-floor work center (e.g. WC-MILL, WC-TURN, WC-DRILL). Primary grouping dimension for capacity analysis.",
    "period_month":             "Start of the calendar month for this capacity snapshot.",
    "period_year":              "Calendar year for annual capacity summaries.",
    "demand_hrs":               "Total planned machine hours required from work orders scheduled for this work center and month.",
    "available_hrs":            "Total available machine hours for the month based on active machine count × capacity_hrs_per_day × 22 working days.",
    "farm_out_hrs":             "Hours of work that were sent to external vendors (Farm-Out) instead of run in-house during this period.",
    "farm_out_cost_usd":        "USD cost paid to external vendors for Farm-Out orders in this period. Key metric for make-vs-buy analysis.",
    "machine_count":            "Number of Active machines in this work center available during the period.",
    "machines_used":            "Number of distinct machines that had work orders scheduled in this period.",
    "capacity_utilization_pct": "Capacity utilization: demand_hrs ÷ available_hrs × 100. >90% = Overloaded; >75% = High Load; >50% = Normal; <50% = Underutilized.",
    "farm_out_rate_pct":        "Percentage of total demand hours that were farmed out externally: farm_out_hrs ÷ demand_hrs × 100. High rates suggest capacity shortage.",
    "load_status":              "Derived load category: Overloaded (>90%), High Load (>75%), Normal (>50%), or Underutilized (<50%). Used for investment prioritization.",
}

cnt = set_column_comments_via_api(CATALOG, SCHEMA, "gold_capacity_planning", col_comments_gold_cap)
print(f"✅ gold_capacity_planning: {cnt} column comments set")

# COMMAND ----------
# MAGIC %md ### gold_farmout_analysis

# COMMAND ----------

set_table_comment(CATALOG, SCHEMA, "gold_farmout_analysis",
    "Farm-out vendor performance and cost analysis by part and vendor. Supports make-vs-buy decisions by comparing external machining costs against implied internal production costs.")

col_comments_gold_fo = {
    "farm_out_vendor":          "External machining vendor name (e.g. Precision Parts Inc., MetalCraft LLC). Primary grouping key for vendor analysis.",
    "part_number":              "Engineering part number farmed out to this vendor. Used to identify parts with high external dependency.",
    "part_description":         "Human-readable part name. Denormalized for reporting.",
    "period_month":             "Calendar month when these farm-out orders were due.",
    "period_year":              "Calendar year for annual farm-out cost summaries.",
    "farm_out_orders":          "Number of work orders sent to this vendor for this part and period.",
    "total_qty_farmed":         "Total quantity of parts farmed out to this vendor in this period.",
    "total_farm_out_cost":      "Total USD paid to this vendor for farm-out orders in this period. Sum across all orders.",
    "avg_cost_per_order":       "Average USD cost per farm-out work order for this vendor and part.",
    "equivalent_internal_hrs":  "Total standard machine hours that would have been required if these parts were made in-house.",
    "implied_internal_cost":    "Estimated internal manufacturing cost if run in-house, at a blended rate of $150/hr × equivalent_internal_hrs.",
    "cost_premium_pct":         "Premium paid for external machining: (total_farm_out_cost − implied_internal_cost) ÷ total_farm_out_cost × 100. Positive = more expensive externally.",
}

cnt = set_column_comments_via_api(CATALOG, SCHEMA, "gold_farmout_analysis", col_comments_gold_fo)
print(f"✅ gold_farmout_analysis: {cnt} column comments set")

# COMMAND ----------
# MAGIC %md ## Summary

# COMMAND ----------

total = (
    len(col_comments_silver_machines) + len(col_comments_silver_routings) +
    len(col_comments_silver_wo)        + len(col_comments_silver_sensor)   +
    len(col_comments_gold_util)        + len(col_comments_gold_sched)       +
    len(col_comments_gold_pm)          + len(col_comments_gold_cap)         +
    len(col_comments_gold_fo)
)

print("=" * 60)
print("  Column Descriptions Complete")
print("=" * 60)
print(f"  silver_machines          : {len(col_comments_silver_machines):>3} columns")
print(f"  silver_routings          : {len(col_comments_silver_routings):>3} columns")
print(f"  silver_work_orders       : {len(col_comments_silver_wo):>3} columns")
print(f"  silver_sensor_data       : {len(col_comments_silver_sensor):>3} columns")
print(f"  gold_machine_utilization : {len(col_comments_gold_util):>3} columns")
print(f"  gold_scheduling_performance:{len(col_comments_gold_sched):>2} columns")
print(f"  gold_predictive_maintenance:{len(col_comments_gold_pm):>2} columns")
print(f"  gold_capacity_planning   : {len(col_comments_gold_cap):>3} columns")
print(f"  gold_farmout_analysis    : {len(col_comments_gold_fo):>3} columns")
print(f"  {'─'*40}")
print(f"  TOTAL                    : {total:>3} column descriptions")
print("=" * 60)
print("  Next: Run 06_genie_space.py to create the Genie Space")
print("=" * 60)
