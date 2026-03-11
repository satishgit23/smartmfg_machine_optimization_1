# SmartMFG — Machine Scheduling Optimization Demo

> **AI-powered machine scheduling, farm-out decision support, and long-term capacity planning on Databricks**

---

## Business Context

Manufacturing organizations face constant pressure to:
- Reduce fluctuations in **external machining (farm-out) costs**
- Make smarter **make-vs-buy decisions** using data
- Plan **long-term capacity** (new shifts, equipment, supplier adjustments)
- Reduce unplanned downtime with **predictive maintenance**
- Centralize data from **Oracle ERP, shop-floor systems, and connected machines**

This demo delivers all of the above on the Databricks Lakehouse using a full Medallion architecture.

---

## Architecture

```
Oracle ERP          Shop-Floor Systems    IoT Sensors
   │                       │                  │
   └───────────────────────┴──────────────────┘
                           │
                    Unity Catalog Volume
               (Landing Zone — CSV / JSON)
                           │
              ┌────────────▼────────────┐
              │      DLT Pipeline       │
              │  (Spark SQL / SDP)      │
              └─────────────────────────┘
                     │         │
             ┌───────┘         └───────┐
             ▼                         ▼
      Bronze Tables             (streaming ingest)
   (raw, append-only)
             │
             ▼
      Silver Tables
   (typed, validated,
    quality-checked)
             │
             ▼
       Gold Tables
   (aggregated MVs,
    business-ready)
             │
     ┌───────┴────────┐
     │                │
     ▼                ▼
 AI/BI Dashboard   SQL Queries
 (Lakeview)        (Analytics)
```

### Medallion Layers

| Layer | Tables | Description |
|-------|--------|-------------|
| **Bronze** | `bronze_machines`, `bronze_work_orders`, `bronze_sensor_data`, `bronze_routings` | Raw ingestion via `read_files()` — append-only streaming tables |
| **Silver** | `silver_machines`, `silver_work_orders`, `silver_sensor_data`, `silver_routings` | Cleaned, typed, quality-constrained with DLT expectations |
| **Gold** | `gold_machine_utilization`, `gold_scheduling_performance`, `gold_predictive_maintenance`, `gold_capacity_planning`, `gold_farmout_analysis` | Aggregated materialized views optimized for BI and analytics |

---

## Artifacts

| File | Description |
|------|-------------|
| `00_setup.py` | Creates Unity Catalog, schema, and landing volume |
| `01_data_generator.py` | Generates synthetic CSV data (machines, work orders, sensor readings, routings) |
| `02_dlt_pipeline.sql` | Full Bronze → Silver → Gold DLT pipeline (Spark SQL) |
| `03_analytics_queries.sql` | Curated Gold-layer SQL queries for all use case areas |
| `04_dashboard.py` | Deploys the Lakeview AI/BI dashboard (5 pages) |
| `README.md` | This file |

---

## Data Model

### Source Systems Simulated

| System | Tables | Notes |
|--------|--------|-------|
| Oracle ERP | `work_orders` | 400 work orders, Jan 2024–Dec 2025 |
| Engineering BOM System | `routings` | 48 routing records for 12 parts |
| Machine Master | `machines` | 12 machines across 6 work centers |
| IoT / Shop-Floor | `sensor_data` | ~8,600 hourly sensor readings (30 days) |

### Gold Tables & Use Cases

| Gold Table | Use Case |
|------------|----------|
| `gold_machine_utilization` | Which machines are under/overloaded? What is revenue per machine? |
| `gold_scheduling_performance` | On-time delivery, farm-out cost trends, late order root cause |
| `gold_predictive_maintenance` | Health scores, anomaly detection, PM scheduling |
| `gold_capacity_planning` | Demand vs capacity, investment ROI analysis |
| `gold_farmout_analysis` | Vendor benchmarking, make-vs-buy cost comparison |

---

## Dashboard Pages

The Lakeview dashboard `smartmfg_machine_optimization_dashboard` contains 5 pages:

| Page | KPIs | Charts | Table |
|------|------|--------|-------|
| **Machine Utilization** | Total Machines, Avg Util %, Total Revenue | Revenue by Machine (bar), Util Trend (line) | Monthly Utilization Detail |
| **Scheduling Performance** | Total Orders, Farm-Out Cost, Avg OTD % | OTD Trend (line), Farm-Out by WC (bar) | Scheduling Detail |
| **Predictive Maintenance** | Fleet Health Score, Anomaly Count, Avg Tool Wear | Health by Machine (bar), Urgency (pie) | Machine Health Snapshot |
| **Capacity Planning** | Avg Cap Util %, Farm-Out Cost, Demand Hrs | Demand vs Capacity (grouped bar), Load Status (pie) | Capacity Detail |
| **Farm-Out Analysis** | Total Farm-Out $, Internal Equiv $, Avg Premium % | Cost Comparison (bar), Orders by Vendor (bar) | Vendor Detail |

---

## Setup & Execution

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- `satsen_catalog` catalog (or admin rights to create it)
- A running SQL warehouse
- Serverless compute enabled

### Step-by-Step

1. **Import notebooks** into your Databricks workspace:
   ```bash
   databricks sync ./smartmfg_machine_optimization_1 /Workspace/Users/<your-user>/smartmfg_machine_optimization_1
   ```

2. **Run `00_setup.py`** — creates catalog, schema, and landing volume

3. **Run `01_data_generator.py`** — writes synthetic CSV data to the landing volume

4. **Create DLT Pipeline** in Databricks UI:
   - Name: `smartmfg_machine_opt_pipeline`
   - Source file: `/Workspace/Users/<user>/smartmfg_machine_optimization_1/02_dlt_pipeline.sql`
   - Target catalog: `satsen_catalog`
   - Target schema: `smartmfg_machine_optimization_1`
   - Compute: Serverless
   - Click **Start**

5. **Run `03_analytics_queries.sql`** in the Databricks SQL Editor to validate Gold tables

6. **Run `04_dashboard.py`** — deploys and publishes the Lakeview dashboard

---

## Key SQL Analytics Queries

### Machine Utilization — Top Machines
```sql
SELECT machine_id,
       ROUND(AVG(utilization_pct), 1) AS avg_util_pct,
       ROUND(SUM(revenue_usd), 0)     AS total_revenue_usd
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_machine_utilization
GROUP BY machine_id
ORDER BY avg_util_pct DESC;
```

### Predictive Maintenance — Machines Needing PM
```sql
SELECT machine_id, machine_name, MAX(days_since_last_pm) AS days_since_pm,
       MAX(max_tool_wear_pct) AS peak_tool_wear
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_predictive_maintenance
GROUP BY 1, 2
HAVING MAX(days_since_last_pm) > 180
ORDER BY days_since_pm DESC;
```

### Farm-Out Cost by Vendor
```sql
SELECT farm_out_vendor,
       ROUND(SUM(total_farm_out_cost), 0)   AS total_cost,
       ROUND(AVG(cost_premium_pct), 1)      AS avg_premium_pct
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_farmout_analysis
GROUP BY farm_out_vendor
ORDER BY total_cost DESC;
```

### Overloaded Work Centers (Capacity Investment Candidates)
```sql
SELECT work_center,
       COUNT(DISTINCT period_month)          AS months_overloaded,
       ROUND(SUM(farm_out_cost_usd), 0)      AS total_farm_out_cost
FROM satsen_catalog.smartmfg_machine_optimization_1.gold_capacity_planning
WHERE load_status IN ('Overloaded', 'High Load')
GROUP BY work_center
ORDER BY months_overloaded DESC;
```

---

## Catalog & Naming Conventions

| Component | Value |
|-----------|-------|
| Catalog | `satsen_catalog` |
| Schema | `smartmfg_machine_optimization_1` |
| Bronze prefix | `bronze_*` |
| Silver prefix | `silver_*` |
| Gold prefix | `gold_*` |
| Pipeline name | `smartmfg_machine_opt_pipeline` |
| Dashboard name | `smartmfg_machine_optimization_dashboard` |
| Volume | `landing_zone` |

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Platform | Databricks Lakehouse |
| Data catalog | Unity Catalog |
| ETL pipelines | Spark Declarative Pipelines (DLT) — Spark SQL |
| Compute | Serverless |
| Storage | Delta Lake (Unity Catalog managed tables) |
| BI / Dashboards | Databricks AI/BI (Lakeview) |
| Data format | CSV (landing) → Delta (Bronze/Silver/Gold) |
| Source integration | Oracle ERP (simulated), IoT sensors (simulated) |

---

## Repo Structure

```
smartmfg_machine_optimization_1/
├── 00_setup.py               # Environment setup (catalog, schema, volumes)
├── 01_data_generator.py      # Synthetic data generation
├── 02_dlt_pipeline.sql       # DLT pipeline — Bronze, Silver, Gold
├── 03_analytics_queries.sql  # Gold-layer analytics SQL queries
├── 04_dashboard.py           # Lakeview dashboard deployment
└── README.md                 # This file
```

---

*Built on Databricks | SmartMFG Machine Scheduling Optimization Demo*
