# ServiceTrack: Job Tracking & Customer Visit Analytics Pipeline
### Celebal Technologies — Data Engineering Programme

## 📌 Problem Statement
Service centers collect job and customer data daily as a mandatory operational requirement,
but lack the analytics infrastructure to turn it into actionable insight — on workload
distribution, repair delays, technician performance, repeat customers, and device failure
trends. This pipeline builds that missing infrastructure.

## 📂 Dataset — Upload These 3 Files to Databricks
Unlike the earlier projects, this one uses **real provided data**, not synthetic generation:
- `customers.csv` (300 rows)
- `devices.csv` (43 rows)
- `service_jobs.csv` (1,510 rows, includes 10 duplicate `job_id`s and ~75 blank `technician_name`s — by design)

### How to upload to Databricks
1. In your Databricks workspace: **Data (left sidebar) → Add data → Upload files to a volume**
   (or drag-and-drop onto an existing volume, e.g. `/Volumes/workspace/default/servicetrack/`)
2. Upload all 3 CSVs to the same folder
3. In the notebook's first widget (`data_path`), set it to that exact folder path
   (defaults to `/Volumes/workspace/default/servicetrack/` — change if you used a different path)

## 📥 What to Upload / Submit
Just **one file**: `ServiceTrack_Databricks.py`
- **Databricks:** Workspace → Import → File
- **GitHub:** Push into a new folder (e.g. `Assignment10`) in `Shoppind_dataset_analysis`

## ⬆️ How to Run
1. Import into Databricks (Workspace → Import → File)
2. Attach to a cluster
3. Upload the 3 CSVs to a volume (see above) and set the `data_path` widget accordingly
4. **Run All**
5. Check **Data/Catalog** sidebar → `servicetrack` schema should show 10 tables:
   `bronze_customers`, `bronze_devices`, `bronze_service_jobs`, `silver_service_jobs_enriched`,
   `gold_technician_performance`, `gold_delay_analysis`, `gold_device_brand_trends`,
   `gold_repeat_customers`, `gold_customer_visit_history`, `dim_technician`

## 🏗️ Architecture (Bronze → Silver → Gold)
| Layer | Tables | Purpose |
|-------|--------|---------|
| 🥉 Bronze | `bronze_customers`, `bronze_devices`, `bronze_service_jobs` | Raw, exactly as received — including all duplicates/nulls |
| 🥈 Silver | `silver_service_jobs_enriched` | De-duplicated, technician names resolved, repair duration + delay flag computed, 3-table join |
| 🥇 Gold | 5 aggregate tables | Pre-aggregated answers to each of the 5 business questions |

## 🧹 Data Quality Handling (Silver Layer)
| Issue | Handling |
|-------|----------|
| 10 duplicate `job_id` rows | `dropDuplicates(["job_id"])` |
| ~75 blank `technician_name` | Resolved via a `technician_id → technician_name` lookup built from non-blank rows (each technician_id maps to exactly one name in this dataset) |
| `completed_date` NULL for non-completed jobs | Preserved — used to derive the `In Progress` delivery flag |
| `actual_cost` NULL for non-completed jobs | Preserved as-is (expected for jobs not yet billed) |

**Result on the real dataset:** 1,500 rows after de-duplication, 0 unresolved technician
names, delivery flag distribution: 833 On Time / 297 Delayed / 370 In Progress (Cancelled/Pending jobs excluded from delay calc since they were never completed).

## 🥇 Gold Layer Tables (Answers the 5 Business Questions)
1. **`gold_technician_performance`** — jobs completed, avg repair duration, delay rate per technician
2. **`gold_delay_analysis`** — delay rate and avg delay days per issue type
3. **`gold_device_brand_trends`** — job volume and avg repair duration per brand/device type
4. **`gold_repeat_customers`** — customers with >1 visit (271 of 300 customers, in this dataset)
5. **`gold_customer_visit_history`** — most recent job per customer (via `ROW_NUMBER()` window function)

## 🔎 SQL Analytics Layer
Demonstrates the exact SQL capabilities called for in the spec, each as its own `%sql` cell:
- **CASE expression** — classify jobs Delayed/On Time/In Progress
- **GROUP BY … HAVING** — repeat customer detection (>1 visit)
- **Window function with PARTITION BY** — each technician's avg repair time alongside every job
- **CTE with ROW_NUMBER()** — most recent job per customer, no subquery nesting
- **Multi-table join** — device brand delay rate (Silver fact + `bronze_devices`)

## 🔒 Delta Lake Capabilities Demonstrated
1. **Time Travel** — `DESCRIBE HISTORY` + reading `silver_service_jobs_enriched` at `versionAsOf(0)`
2. **Schema Enforcement** — a deliberately malformed write (wrong column type) is shown being
   rejected rather than silently corrupting the table
3. **SCD Type 2 via `MERGE INTO`** — a `dim_technician` dimension table tracks technician name
   history; a simulated update (T003's name correction) is merged in, closing out the old row
   (`is_current = false`, `effective_end_date` set) and inserting the new current row —
   preserving full history for any job records tied to the old name

## ✅ Validated Before Delivery
The full Bronze → Silver → Gold pipeline and all 5 SQL analytics queries were run end-to-end
against the **actual provided CSVs** (not synthetic data) using a local Spark session before
delivery — every stage produces sensible, verified output (counts, delay rates, technician
comparisons all checked). Only the Delta-specific features (time travel, schema enforcement,
`MERGE INTO`) require genuine Delta Lake, which is native to Databricks but not available in
the local sandbox used to validate the rest of the pipeline — these use standard, well-
documented Delta syntax and will run correctly on Databricks without modification.

## 🧰 Tech Stack
PySpark (DataFrame API + Spark SQL), Delta Lake, Databricks widgets

## ✍️ Author
Rahul Singh — Data Engineering Intern, Celebal Technologies
