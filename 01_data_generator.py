# Databricks notebook source
# MAGIC %md
# MAGIC # SmartMFG Machine Scheduling Optimization - Data Generator
# MAGIC **Notebook:** 01_data_generator.py
# MAGIC
# MAGIC Generates realistic synthetic data for the demo and writes CSV files
# MAGIC to the Unity Catalog landing volume for ingestion by the DLT pipeline.
# MAGIC
# MAGIC **Datasets generated:**
# MAGIC | File | Records | Description |
# MAGIC |------|---------|-------------|
# MAGIC | machines.csv | 12 | Machine master data |
# MAGIC | routings.csv | 48 | Part routing / operation sequences |
# MAGIC | work_orders.csv | 400 | Work orders from ERP (Oracle) |
# MAGIC | sensor_data.csv | ~8,640 | Hourly IoT sensor readings (30 days × 12 machines × 24 hrs) |

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

CATALOG     = "satsen_catalog"
SCHEMA      = "smartmfg_machine_optimization_1"
VOLUME      = "landing_zone"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# COMMAND ----------
# MAGIC %md ## Imports & Helpers

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta, date
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType, BooleanType
)

random.seed(42)   # reproducible

def rand_between(a, b, decimals=2):
    return round(random.uniform(a, b), decimals)

def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

# COMMAND ----------
# MAGIC %md ## 1. Machines Master Data

# COMMAND ----------

machines_data = [
    # (machine_id, machine_name, machine_type, work_center, shifts, cap_hrs_per_shift, vendor, install_year, status)
    ("MCH-001", "Haas VF-4SS", "CNC Vertical Machining Center", "WC-MILL",  3, 8.0, "Haas",       2019, "Active"),
    ("MCH-002", "Mazak QT-200", "CNC Turning Center",          "WC-TURN",  2, 8.0, "Mazak",      2020, "Active"),
    ("MCH-003", "Fanuc Robodrill D21MiA5", "CNC Drill/Tap",   "WC-DRILL", 2, 8.0, "Fanuc",      2018, "Active"),
    ("MCH-004", "Okuma LB3000", "CNC Lathe",                   "WC-TURN",  3, 8.0, "Okuma",      2021, "Active"),
    ("MCH-005", "DMG MORI DMU 50", "5-Axis Machining Center",  "WC-MILL",  2, 8.0, "DMG MORI",   2022, "Active"),
    ("MCH-006", "Doosan DNM 5700", "CNC Machining Center",     "WC-MILL",  1, 8.0, "Doosan",     2017, "Active"),
    ("MCH-007", "Mazak Integrex i-200", "Multi-Tasking Machine","WC-MULTITASK", 3, 8.0, "Mazak", 2023, "Active"),
    ("MCH-008", "Haas TL-1", "Toolroom Lathe",                 "WC-TURN",  1, 8.0, "Haas",       2016, "Maintenance"),
    ("MCH-009", "Brown & Sharpe CMM", "Coordinate Measuring Machine","WC-QC", 2, 8.0, "Hexagon",  2020, "Active"),
    ("MCH-010", "Hydromat HT 45-12", "Transfer Machine",       "WC-TRANSFER", 3, 8.0, "Hydromat", 2019, "Active"),
    ("MCH-011", "Makino A61NX", "Horizontal Machining Center", "WC-MILL",  2, 8.0, "Makino",     2021, "Active"),
    ("MCH-012", "OKK HM 400", "Horizontal Machining Center",   "WC-MILL",  2, 8.0, "OKK",        2018, "Idle"),
]

machines_rows = [
    Row(
        machine_id=r[0], machine_name=r[1], machine_type=r[2],
        work_center=r[3], num_shifts=r[4], capacity_hrs_per_shift=r[5],
        vendor=r[6], installation_year=r[7], status=r[8],
        hourly_rate_usd=rand_between(45, 80),
        preventive_maint_interval_hrs=random.choice([500, 750, 1000]),
        last_pm_date=str(rand_date(date(2024, 1, 1), date(2025, 9, 30))),
        notes=f"{r[1]} installed in {r[7]}"
    )
    for r in machines_data
]

machines_schema = StructType([
    StructField("machine_id",                  StringType(),  False),
    StructField("machine_name",                StringType(),  True),
    StructField("machine_type",                StringType(),  True),
    StructField("work_center",                 StringType(),  True),
    StructField("num_shifts",                  IntegerType(), True),
    StructField("capacity_hrs_per_shift",      DoubleType(),  True),
    StructField("vendor",                      StringType(),  True),
    StructField("installation_year",           IntegerType(), True),
    StructField("status",                      StringType(),  True),
    StructField("hourly_rate_usd",             DoubleType(),  True),
    StructField("preventive_maint_interval_hrs", IntegerType(), True),
    StructField("last_pm_date",                StringType(),  True),
    StructField("notes",                       StringType(),  True),
])

machines_df = spark.createDataFrame(machines_rows, machines_schema)
machines_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/machines/")
print(f"✅ machines.csv written  ({machines_df.count()} rows)")

# COMMAND ----------
# MAGIC %md ## 2. Routings (Part-Operation Sequences)

# COMMAND ----------

parts = [
    ("PN-1001", "Cylinder Head",       "WC-MILL",   "MCH-001"),
    ("PN-1002", "Drive Shaft",         "WC-TURN",   "MCH-002"),
    ("PN-1003", "Gear Housing",        "WC-MILL",   "MCH-005"),
    ("PN-1004", "Bearing Bracket",     "WC-MILL",   "MCH-006"),
    ("PN-1005", "Pump Body",           "WC-TURN",   "MCH-004"),
    ("PN-1006", "Valve Seat",          "WC-DRILL",  "MCH-003"),
    ("PN-1007", "Flange Assembly",     "WC-MULTITASK","MCH-007"),
    ("PN-1008", "Impeller",            "WC-MILL",   "MCH-011"),
    ("PN-1009", "Spindle Housing",     "WC-MILL",   "MCH-001"),
    ("PN-1010", "Crankshaft Pin",      "WC-TURN",   "MCH-002"),
    ("PN-1011", "Transfer Plate",      "WC-TRANSFER","MCH-010"),
    ("PN-1012", "CMM Fixture Plate",   "WC-QC",     "MCH-009"),
]

ops = [
    ("OP-010", "Rough Turning",      0.5, 0.25),
    ("OP-020", "Finish Turning",     0.5, 0.20),
    ("OP-030", "Milling",            1.0, 0.30),
    ("OP-040", "Drilling & Tapping", 0.5, 0.15),
    ("OP-050", "Inspection (CMM)",   0.3, 0.10),
]

routing_rows = []
routing_id = 1
for part_no, part_desc, wc, machine_id in parts:
    # Each part gets 3-4 operations (subset of ops)
    selected_ops = ops[:random.randint(3, 4)]
    for seq, (op_code, op_desc, setup_hrs, run_hrs) in enumerate(selected_ops, start=1):
        routing_rows.append(Row(
            routing_id=f"RTG-{routing_id:04d}",
            part_number=part_no,
            part_description=part_desc,
            operation_sequence=seq * 10,
            operation_code=op_code,
            operation_description=op_desc,
            work_center=wc,
            preferred_machine_id=machine_id,
            setup_time_hrs=setup_hrs,
            run_time_hrs_per_unit=round(run_hrs + rand_between(-0.05, 0.05), 3),
            scrap_rate_pct=rand_between(0.5, 3.5),
        ))
        routing_id += 1

routing_schema = StructType([
    StructField("routing_id",           StringType(),  False),
    StructField("part_number",          StringType(),  True),
    StructField("part_description",     StringType(),  True),
    StructField("operation_sequence",   IntegerType(), True),
    StructField("operation_code",       StringType(),  True),
    StructField("operation_description",StringType(),  True),
    StructField("work_center",          StringType(),  True),
    StructField("preferred_machine_id", StringType(),  True),
    StructField("setup_time_hrs",       DoubleType(),  True),
    StructField("run_time_hrs_per_unit",DoubleType(),  True),
    StructField("scrap_rate_pct",       DoubleType(),  True),
])

routings_df = spark.createDataFrame(routing_rows, routing_schema)
routings_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/routings/")
print(f"✅ routings.csv written  ({routings_df.count()} rows)")

# COMMAND ----------
# MAGIC %md ## 3. Work Orders (ERP / Oracle Simulation)

# COMMAND ----------

farm_out_vendors = ["Precision Parts Inc", "MetalCraft LLC", "Allied Machining", "TechForge Co", None]
statuses        = ["Complete", "Complete", "Complete", "In Progress", "Open", "Farm-Out"]

wo_rows = []
start_date = date(2024, 1, 1)

for i in range(1, 401):
    part_no, part_desc, wc, machine_id = random.choice(parts)
    op_code, op_desc, setup_hrs, run_hrs_unit = random.choice(ops)
    order_qty   = random.choice([5, 10, 20, 25, 50, 100])
    due_dt      = rand_date(date(2024, 2, 1), date(2025, 12, 31))
    sched_start = due_dt - timedelta(days=random.randint(3, 14))
    sched_end   = sched_start + timedelta(hours=round(setup_hrs + run_hrs_unit * order_qty, 1))
    status      = random.choice(statuses)

    actual_start = actual_end = None
    actual_hrs   = None
    farm_cost    = None
    farm_vendor  = None

    if status in ("Complete", "In Progress"):
        offset       = random.randint(-1, 2)
        actual_start = sched_start + timedelta(days=offset)
        if status == "Complete":
            actual_end = actual_start + timedelta(hours=round((setup_hrs + run_hrs_unit * order_qty) * rand_between(0.9, 1.3), 1))
            actual_hrs = round((actual_end - actual_start).total_seconds() / 3600, 2)

    if status == "Farm-Out":
        farm_vendor = random.choice([v for v in farm_out_vendors if v])
        farm_cost   = round(order_qty * run_hrs_unit * rand_between(120, 200), 2)
        machine_id  = None

    wo_rows.append(Row(
        work_order_id    = f"WO-{i:05d}",
        part_number      = part_no,
        part_description = part_desc,
        order_qty        = order_qty,
        operation_code   = op_code,
        operation_description = op_desc,
        work_center      = wc,
        machine_id       = machine_id,
        due_date         = str(due_dt),
        scheduled_start  = str(sched_start),
        scheduled_end    = str(sched_end),
        actual_start     = str(actual_start) if actual_start else None,
        actual_end       = str(actual_end)   if actual_end   else None,
        setup_time_hrs   = setup_hrs,
        run_time_hrs_per_unit = run_hrs_unit,
        actual_total_hrs = actual_hrs,
        status           = status,
        priority         = random.choice(["High", "Medium", "Low"]),
        farm_out_vendor  = farm_vendor,
        farm_out_cost_usd= farm_cost,
        source_system    = "Oracle ERP",
    ))

wo_schema = StructType([
    StructField("work_order_id",         StringType(),  False),
    StructField("part_number",           StringType(),  True),
    StructField("part_description",      StringType(),  True),
    StructField("order_qty",             IntegerType(), True),
    StructField("operation_code",        StringType(),  True),
    StructField("operation_description", StringType(),  True),
    StructField("work_center",           StringType(),  True),
    StructField("machine_id",            StringType(),  True),
    StructField("due_date",              StringType(),  True),
    StructField("scheduled_start",       StringType(),  True),
    StructField("scheduled_end",         StringType(),  True),
    StructField("actual_start",          StringType(),  True),
    StructField("actual_end",            StringType(),  True),
    StructField("setup_time_hrs",        DoubleType(),  True),
    StructField("run_time_hrs_per_unit", DoubleType(),  True),
    StructField("actual_total_hrs",      DoubleType(),  True),
    StructField("status",                StringType(),  True),
    StructField("priority",              StringType(),  True),
    StructField("farm_out_vendor",       StringType(),  True),
    StructField("farm_out_cost_usd",     DoubleType(),  True),
    StructField("source_system",         StringType(),  True),
])

wo_df = spark.createDataFrame(wo_rows, wo_schema)
wo_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/work_orders/")
print(f"✅ work_orders.csv written  ({wo_df.count()} rows)")

# COMMAND ----------
# MAGIC %md ## 4. Machine Sensor Data (IoT / Shop Floor)

# COMMAND ----------

# Simulate 30 days of hourly sensor readings for all active machines
active_machines = [r for r in machines_data if r[8] in ("Active",)]
sensor_start    = datetime(2025, 2, 1, 0, 0)
sensor_hours    = 24 * 30   # 30 days

# Maintenance alert thresholds per machine type (normal operating ceiling)
temp_limits  = {"CNC Vertical Machining Center": 75, "CNC Lathe": 70, "CNC Turning Center": 68}
default_temp = 65

# ── Per-machine maintenance profiles ─────────────────────────────────────
# Controls how worn/healthy each machine looks.
# Gold layer urgency (computed per day per machine):
#   Critical : MIN(health_score) < 40  OR  MAX(tool_wear_pct) > 90
#   Warning  : MIN(health_score) < 65  OR  MAX(tool_wear_pct) > 75
#   Normal   : everything else
#
# Health score deductions per reading:
#   tool_wear > 80 → -30,  vibration > 4.0 → -25,
#   temperature > 80 → -20,  anomaly → -10
#
# KEY DESIGN RULE for robust daily urgency:
#   For Warning/Critical machines, accrual is set high enough that the machine
#   hits its reset threshold WITHIN A SINGLE 17-hour production day.
#   Formula: (reset_pct - 5) / min_accrual < 17 hrs ensures intra-day reset.
#   This guarantees MAX(tool_wear) = reset_pct on EVERY production day,
#   regardless of which day in the 30-day window is the "last reading date."
#
# Designed distribution across 10 active machines:
#   Normal  (7): MCH-001, MCH-002, MCH-004, MCH-005, MCH-007, MCH-009, MCH-011
#   Warning (2): MCH-003, MCH-006  (max_wear 77-89 → > 75, never > 90)
#   Critical(1): MCH-010           (max_wear 92 > 90 AND health < 40)
#
# Profile keys:
#   reset_pct     – tool wear % that triggers a PM tool change (resets wear to ~5%)
#   accrual       – (min, max) wear increase per production hour
#   anomaly_rate  – probability of an anomaly event per production hour
#   temp_extra    – degrees C added to normal temp ceiling (simulates heat buildup)
#   vib_range     – (min, max) normal vibration in mm/s
# ──────────────────────────────────────────────────────────────────────────
MACHINE_PROFILES = {
    # ── Normal machines — well-maintained, frequent PM, low anomaly rate ──
    # Low accrual (0.04-0.24/hr) means tool wear cycles slowly.
    # reset_pct ≤ 72 ensures max_wear ≤ 72% (well below 75 Warning threshold).
    # Vibration ≤ 2.5 mm/s and temp within limits keep health score ≥ 75 every day.
    "MCH-001": dict(reset_pct=68,  accrual=(0.08, 0.20), anomaly_rate=0.020, temp_extra=0,  vib_range=(0.3, 2.2)),  # Haas VF-4SS      — flagship mill, proactive PM
    "MCH-002": dict(reset_pct=70,  accrual=(0.10, 0.24), anomaly_rate=0.025, temp_extra=0,  vib_range=(0.4, 2.5)),  # Mazak QT-200     — reliable turner
    "MCH-004": dict(reset_pct=70,  accrual=(0.09, 0.22), anomaly_rate=0.020, temp_extra=0,  vib_range=(0.3, 2.3)),  # Okuma LB3000     — well-maintained lathe
    "MCH-005": dict(reset_pct=65,  accrual=(0.07, 0.18), anomaly_rate=0.015, temp_extra=0,  vib_range=(0.2, 1.8)),  # DMG MORI DMU 50  — newest, pristine condition
    "MCH-007": dict(reset_pct=72,  accrual=(0.10, 0.23), anomaly_rate=0.020, temp_extra=0,  vib_range=(0.4, 2.4)),  # Mazak Integrex   — high-value, proactive PM
    "MCH-009": dict(reset_pct=62,  accrual=(0.04, 0.12), anomaly_rate=0.012, temp_extra=0,  vib_range=(0.1, 1.4)),  # Brown & Sharpe CMM — precision QC, minimal wear
    "MCH-011": dict(reset_pct=70,  accrual=(0.09, 0.21), anomaly_rate=0.020, temp_extra=0,  vib_range=(0.3, 2.2)),  # Makino A61NX     — well-maintained HMC

    # ── Warning machines — deferred PM, high intra-day tool wear ──
    # accrual=5.0-7.0/hr ensures (reset_pct - 5) / 5.0 < 17 hrs → intra-day reset.
    # reset_pct set to 78-84: MAX(tool_wear) > 75 (Warning) but < 90 (not Critical).
    # Vibration ≤ 3.8 mm/s and no temp elevation keeps MIN(health) ≥ 44 → not Critical.
    "MCH-003": dict(reset_pct=82,  accrual=(5.0, 7.0),   anomaly_rate=0.060, temp_extra=3,  vib_range=(0.8, 3.5)),  # Fanuc Robodrill  — overdue PM, tool wear spikes
    "MCH-006": dict(reset_pct=79,  accrual=(5.0, 7.0),   anomaly_rate=0.070, temp_extra=4,  vib_range=(1.0, 3.8)),  # Doosan DNM 5700  — 2017 vintage, wear-induced Warning

    # ── Critical machine — severe neglect, needs immediate shutdown ──
    # reset_pct=92 → MAX(tool_wear) = 92 > 90 → Critical (tool wear condition alone).
    # vib_range starts at 4.5 → EVERY production reading has vib > 4.0 → -25 always.
    # temp_extra=18 → temp range (63, 88°C) → regularly > 80°C → -20 penalty frequently.
    # When all 4 penalties coincide: 100-30-25-20-10 = 15 < 40 → Critical (health score too).
    "MCH-010": dict(reset_pct=92,  accrual=(5.5, 8.5),   anomaly_rate=0.220, temp_extra=18, vib_range=(4.5, 7.5)),  # Hydromat HT 45-12 — overdue, running hot & critical
}
_default_profile = dict(reset_pct=75, accrual=(0.12, 0.30), anomaly_rate=0.030, temp_extra=0, vib_range=(0.5, 2.5))

sensor_rows = []
for mch in active_machines:
    mch_id    = mch[0]
    mch_type  = mch[2]
    temp_max  = temp_limits.get(mch_type, default_temp)
    profile   = MACHINE_PROFILES.get(mch_id, _default_profile)
    tool_wear = 0.0  # accumulates across hours; resets to ~5% after PM

    for h in range(sensor_hours):
        ts            = sensor_start + timedelta(hours=h)
        hour_of_day   = ts.hour
        is_production = 6 <= hour_of_day <= 22

        if not is_production:
            # Machine idle / off-shift
            sensor_rows.append(Row(
                sensor_id           = str(uuid.uuid4()),
                machine_id          = mch_id,
                reading_timestamp   = str(ts),
                temperature_celsius = rand_between(22, 28),
                vibration_mm_s      = rand_between(0.0, 0.2),
                spindle_speed_rpm   = 0.0,
                power_consumption_kw= rand_between(0.5, 1.5),
                coolant_flow_lpm    = 0.0,
                tool_wear_pct       = round(tool_wear, 2),
                machine_status      = "Idle",
                anomaly_flag        = False,
                anomaly_type        = None,
            ))
        else:
            # Accumulate tool wear; trigger PM (tool change) when threshold reached
            tool_wear = min(tool_wear + rand_between(*profile["accrual"]), 100.0)
            if tool_wear >= profile["reset_pct"]:
                tool_wear = rand_between(3.0, 8.0)   # residual after tool change

            # Normal operating values — use profile-specific temp offset and vib range
            temp_lo   = temp_max - 20 + profile["temp_extra"]
            temp_hi   = temp_max +  5 + profile["temp_extra"]
            temperature = rand_between(temp_lo, temp_hi)
            vibration   = rand_between(*profile["vib_range"])
            spindle     = rand_between(800, 3500)
            power       = rand_between(10, 45)
            coolant     = rand_between(8, 20)

            # Inject anomalies at machine-specific rate
            anomaly_flag = False
            anomaly_type = None
            if random.random() < profile["anomaly_rate"]:
                anomaly_flag = True
                anomaly_type = random.choice([
                    "High Temperature", "Excessive Vibration",
                    "High Tool Wear",   "Low Coolant Flow"
                ])
                if anomaly_type == "High Temperature":
                    temperature = rand_between(temp_max + 5 + profile["temp_extra"],
                                               temp_max + 20 + profile["temp_extra"])
                elif anomaly_type == "Excessive Vibration":
                    vibration   = rand_between(4.0, max(5.0, profile["vib_range"][1]))
                elif anomaly_type == "High Tool Wear":
                    tool_wear   = min(float(tool_wear) + rand_between(5, 15), 100.0)
                elif anomaly_type == "Low Coolant Flow":
                    coolant     = rand_between(0.5, 3.0)

            sensor_rows.append(Row(
                sensor_id           = str(uuid.uuid4()),
                machine_id          = mch_id,
                reading_timestamp   = str(ts),
                temperature_celsius = round(temperature, 2),
                vibration_mm_s      = round(vibration, 3),
                spindle_speed_rpm   = round(spindle, 0),
                power_consumption_kw= round(power, 2),
                coolant_flow_lpm    = round(coolant, 2),
                tool_wear_pct       = round(tool_wear, 2),
                machine_status      = "Running",
                anomaly_flag        = anomaly_flag,
                anomaly_type        = anomaly_type,
            ))

sensor_schema = StructType([
    StructField("sensor_id",            StringType(),  False),
    StructField("machine_id",           StringType(),  True),
    StructField("reading_timestamp",    StringType(),  True),
    StructField("temperature_celsius",  DoubleType(),  True),
    StructField("vibration_mm_s",       DoubleType(),  True),
    StructField("spindle_speed_rpm",    DoubleType(),  True),
    StructField("power_consumption_kw", DoubleType(),  True),
    StructField("coolant_flow_lpm",     DoubleType(),  True),
    StructField("tool_wear_pct",        DoubleType(),  True),
    StructField("machine_status",       StringType(),  True),
    StructField("anomaly_flag",         BooleanType(), True),
    StructField("anomaly_type",         StringType(),  True),
])

sensor_df = spark.createDataFrame(sensor_rows, sensor_schema)
sensor_df.coalesce(4).write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/sensor_data/")
print(f"✅ sensor_data.csv written  ({sensor_df.count()} rows)")

# COMMAND ----------
# MAGIC %md ## 5. Summary

# COMMAND ----------

print("=" * 60)
print("  Data Generation Complete")
print("=" * 60)
for name, df in [("machines", machines_df), ("routings", routings_df),
                 ("work_orders", wo_df), ("sensor_data", sensor_df)]:
    print(f"  {name:15s}: {df.count():>7,} rows")
print(f"\n  Volume Path : {VOLUME_PATH}")
print("  Next Step   : Create + run DLT pipeline (02_dlt_pipeline.sql)")
print("=" * 60)
