# Databricks notebook source
# MAGIC %md
# MAGIC # ServiceTrack: Job Tracking & Customer Visit Analytics Pipeline
# MAGIC ### Celebal Technologies — Data Engineering Programme
# MAGIC
# MAGIC **Domain:** Field Service Operations Analytics · **Type:** Batch Data Engineering Pipeline
# MAGIC **Stack:** Python · PySpark · Databricks · Delta Lake · SQL
# MAGIC
# MAGIC ## Problem Statement
# MAGIC Service centers collect job and customer data every day as a mandatory operational
# MAGIC requirement, but lack the structured analytics infrastructure to turn that data into
# MAGIC actionable insight on workload distribution, repair delays, technician performance,
# MAGIC repeat customer patterns, and device failure trends.
# MAGIC
# MAGIC This pipeline answers:
# MAGIC - Which technician closes jobs fastest, and who consistently runs over the promised date?
# MAGIC - How many customers are returning for the same recurring issue?
# MAGIC - What percentage of jobs are delayed, and by how many days on average?
# MAGIC - Which device brand generates the highest volume of repair jobs?
# MAGIC - Are certain issue types bottlenecks?
# MAGIC
# MAGIC ## Architecture — Medallion Design
# MAGIC | Layer | Role | Tables |
# MAGIC |-------|------|--------|
# MAGIC | 🥉 **Bronze** | Raw ingestion, immutable, exactly as received | `bronze_customers`, `bronze_devices`, `bronze_service_jobs` |
# MAGIC | 🥈 **Silver** | Cleaned, de-duplicated, enriched, joined | `silver_service_jobs_enriched` |
# MAGIC | 🥇 **Gold** | Pre-aggregated business metrics | `gold_technician_performance`, `gold_delay_analysis`, `gold_device_brand_trends`, `gold_repeat_customers`, `gold_customer_visit_history` |
# MAGIC
# MAGIC ## Dataset
# MAGIC 300 customers · 43 devices · 1,510 service jobs (Jan–Mar 2024), with intentional data
# MAGIC quality issues: 10 duplicate `job_id` rows, ~75 blank `technician_name` values, blank
# MAGIC `repair_notes`, and NULL `completed_date`/`actual_cost` for non-completed jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Setup
# MAGIC
# MAGIC Upload `customers.csv`, `devices.csv`, and `service_jobs.csv` to a Databricks volume or
# MAGIC DBFS location first (**Data → Add Data → Upload files to volume**, or drag-and-drop into
# MAGIC a Unity Catalog volume), then set `DATA_PATH` below to match where you uploaded them.

# COMMAND ----------

dbutils.widgets.text("data_path", "/Volumes/workspace/default/servicetrack/", "Upload Folder Path")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG_SCHEMA = "servicetrack"
DATA_PATH = dbutils.widgets.get("data_path").rstrip("/") + "/"
SLA_DAYS = 5  # promised_date = received_date + 5 days, per the dataset spec

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_SCHEMA}")
spark.sql(f"USE {CATALOG_SCHEMA}")
print(f"Setup complete. Schema: {CATALOG_SCHEMA}, reading CSVs from: {DATA_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥉 Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC Raw CSV exports are loaded exactly as received — every null, duplicate, and formatting
# MAGIC inconsistency preserved. Bronze is the immutable starting point for reprocessing; if a
# MAGIC downstream step has a bug, Bronze means we never have to go back to the source files.

# COMMAND ----------

bronze_customers = spark.read.csv(DATA_PATH + "customers.csv", header=True, inferSchema=True)
bronze_devices = spark.read.csv(DATA_PATH + "devices.csv", header=True, inferSchema=True)
bronze_service_jobs = spark.read.csv(DATA_PATH + "service_jobs.csv", header=True, inferSchema=True)

bronze_customers.write.mode("overwrite").saveAsTable("bronze_customers")
bronze_devices.write.mode("overwrite").saveAsTable("bronze_devices")
bronze_service_jobs.write.mode("overwrite").saveAsTable("bronze_service_jobs")

print(f"[BRONZE] customers: {bronze_customers.count()} rows")
print(f"[BRONZE] devices: {bronze_devices.count()} rows")
print(f"[BRONZE] service_jobs: {bronze_service_jobs.count()} rows "
      f"(includes {bronze_service_jobs.count() - bronze_service_jobs.dropDuplicates(['job_id']).count()} duplicate job_ids, as expected)")

# COMMAND ----------

display(bronze_service_jobs.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥈 Silver Layer — Cleaning & Enrichment
# MAGIC
# MAGIC Steps performed:
# MAGIC 1. Parse and type date fields correctly
# MAGIC 2. De-duplicate `service_jobs` on `job_id`
# MAGIC 3. Resolve blank `technician_name` using `technician_id` (each technician_id maps to
# MAGIC    exactly one name across the non-blank rows — a window function fills in the gaps)
# MAGIC 4. Compute `repair_duration_days` — the days between `received_date` and `completed_date`
# MAGIC 5. Flag each job `Delayed` / `On Time` / `In Progress` via a `CASE`-style expression
# MAGIC 6. Join `customers` + `devices` + `service_jobs` into a single enriched fact record

# COMMAND ----------

# ---- 1 & 2: parse dates, de-duplicate ----
service_jobs_typed = (
    bronze_service_jobs
    .withColumn("received_date", F.to_date("received_date"))
    .withColumn("promised_date", F.to_date("promised_date"))
    .withColumn("completed_date", F.to_date("completed_date"))
    .dropDuplicates(["job_id"])
)
n_after_dedup = service_jobs_typed.count()
print(f"[SILVER] service_jobs after de-duplication: {n_after_dedup} rows "
      f"(removed {bronze_service_jobs.count() - n_after_dedup} duplicates)")

# ---- 3: resolve blank technician_name via technician_id ----
technician_lookup = (
    service_jobs_typed
    .filter(F.col("technician_name").isNotNull())
    .select("technician_id", "technician_name")
    .dropDuplicates(["technician_id"])
)

n_blank_before = service_jobs_typed.filter(F.col("technician_name").isNull()).count()

service_jobs_resolved = (
    service_jobs_typed
    .drop("technician_name")
    .join(technician_lookup, on="technician_id", how="left")
)
n_blank_after = service_jobs_resolved.filter(F.col("technician_name").isNull()).count()
print(f"[SILVER] Resolved {n_blank_before - n_blank_after} blank technician_name values via technician_id lookup "
      f"({n_blank_after} remain unresolved — technician_id with no name recorded anywhere)")

# COMMAND ----------

# ---- 4: repair duration ----
service_jobs_enriched = service_jobs_resolved.withColumn(
    "repair_duration_days", F.datediff(F.col("completed_date"), F.col("received_date"))
)

# ---- 5: Delayed / On Time / In Progress flag ----
service_jobs_enriched = service_jobs_enriched.withColumn(
    "delivery_flag",
    F.when(F.col("completed_date").isNull(), "In Progress")
     .when(F.col("completed_date") > F.col("promised_date"), "Delayed")
     .otherwise("On Time")
).withColumn(
    "delay_days",
    F.when(F.col("completed_date") > F.col("promised_date"),
           F.datediff(F.col("completed_date"), F.col("promised_date"))).otherwise(F.lit(0))
)

print("[SILVER] delivery_flag distribution:")
service_jobs_enriched.groupBy("delivery_flag").count().show()

# COMMAND ----------

# ---- 6: multi-table join -> single enriched fact record per job ----
silver_service_jobs_enriched = (
    service_jobs_enriched
    .join(bronze_customers, on="customer_id", how="left")
    .join(bronze_devices, on="device_id", how="left")
)

silver_service_jobs_enriched.write.mode("overwrite").saveAsTable("silver_service_jobs_enriched")
print(f"[SILVER] silver_service_jobs_enriched: {silver_service_jobs_enriched.count()} rows, "
      f"{len(silver_service_jobs_enriched.columns)} columns")

# COMMAND ----------

display(silver_service_jobs_enriched.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Gold Layer — Business Metrics
# MAGIC
# MAGIC Five pre-aggregated tables, one per business question from the problem statement.
# MAGIC A stakeholder queries these directly — no understanding of the Silver join logic needed.

# COMMAND ----------

# MAGIC %md ### 1. Technician Performance

# COMMAND ----------

gold_technician_performance = (
    spark.table("silver_service_jobs_enriched")
    .filter(F.col("job_status") == "Completed")
    .groupBy("technician_id", "technician_name")
    .agg(
        F.count("job_id").alias("total_jobs_completed"),
        F.round(F.avg("repair_duration_days"), 2).alias("avg_repair_duration_days"),
        F.sum(F.when(F.col("delivery_flag") == "Delayed", 1).otherwise(0)).alias("delayed_jobs"),
        F.round(100.0 * F.sum(F.when(F.col("delivery_flag") == "Delayed", 1).otherwise(0))
                / F.count("job_id"), 1).alias("delay_rate_pct"),
    )
    .orderBy("avg_repair_duration_days")
)
gold_technician_performance.write.mode("overwrite").saveAsTable("gold_technician_performance")
display(gold_technician_performance)

# COMMAND ----------

# MAGIC %md ### 2. Delay Analysis (by Issue Type)

# COMMAND ----------

gold_delay_analysis = (
    spark.table("silver_service_jobs_enriched")
    .filter(F.col("job_status") == "Completed")
    .groupBy("issue_type")
    .agg(
        F.count("job_id").alias("total_jobs"),
        F.sum(F.when(F.col("delivery_flag") == "Delayed", 1).otherwise(0)).alias("delayed_jobs"),
        F.round(100.0 * F.sum(F.when(F.col("delivery_flag") == "Delayed", 1).otherwise(0))
                / F.count("job_id"), 1).alias("delay_rate_pct"),
        F.round(F.avg(F.when(F.col("delivery_flag") == "Delayed", F.col("delay_days"))), 2).alias("avg_delay_days_when_delayed"),
    )
    .orderBy(F.desc("delay_rate_pct"))
)
gold_delay_analysis.write.mode("overwrite").saveAsTable("gold_delay_analysis")
display(gold_delay_analysis)

# COMMAND ----------

# MAGIC %md ### 3. Device & Fault Trend Analysis

# COMMAND ----------

gold_device_brand_trends = (
    spark.table("silver_service_jobs_enriched")
    .groupBy("brand", "device_type")
    .agg(
        F.count("job_id").alias("job_count"),
        F.round(F.avg("repair_duration_days"), 2).alias("avg_repair_duration_days"),
        F.countDistinct("issue_type").alias("distinct_issue_types"),
    )
    .orderBy(F.desc("job_count"))
)
gold_device_brand_trends.write.mode("overwrite").saveAsTable("gold_device_brand_trends")
display(gold_device_brand_trends)

# COMMAND ----------

# MAGIC %md ### 4. Repeat Customer Detection

# COMMAND ----------

gold_repeat_customers = (
    spark.table("silver_service_jobs_enriched")
    .groupBy("customer_id", "customer_name")
    .agg(
        F.count("job_id").alias("total_visits"),
        F.countDistinct("issue_type").alias("distinct_issue_types"),
    )
    .filter(F.col("total_visits") > 1)
    .orderBy(F.desc("total_visits"))
)
gold_repeat_customers.write.mode("overwrite").saveAsTable("gold_repeat_customers")
print(f"[GOLD] {gold_repeat_customers.count()} repeat customers found (out of 300 total).")
display(gold_repeat_customers.limit(10))

# COMMAND ----------

# MAGIC %md ### 5. Customer Visit History (Most Recent Job per Customer)

# COMMAND ----------

customer_window = Window.partitionBy("customer_id").orderBy(F.desc("received_date"))

gold_customer_visit_history = (
    spark.table("silver_service_jobs_enriched")
    .withColumn("visit_rank", F.row_number().over(customer_window))
    .filter(F.col("visit_rank") == 1)
    .select("customer_id", "customer_name", "job_id", "device_id", "brand", "issue_type",
            "job_status", "received_date", "delivery_flag")
)
gold_customer_visit_history.write.mode("overwrite").saveAsTable("gold_customer_visit_history")
display(gold_customer_visit_history.limit(10))

# COMMAND ----------

print("Gold tables written: gold_technician_performance, gold_delay_analysis, "
      "gold_device_brand_trends, gold_repeat_customers, gold_customer_visit_history")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Analytics Layer
# MAGIC
# MAGIC Once Gold tables are registered in the metastore, analysts query them directly with SQL —
# MAGIC no Python/Spark knowledge needed. The queries below demonstrate the full range of SQL
# MAGIC capabilities called for in the spec: `CASE` expressions, `GROUP BY … HAVING`, window
# MAGIC functions with `PARTITION BY`, CTEs with `ROW_NUMBER()`, and multi-table joins.

# COMMAND ----------

# MAGIC %md #### CASE Expression — Classify Jobs as Delayed / On Time

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT job_id, technician_name, received_date, promised_date, completed_date,
# MAGIC     CASE
# MAGIC         WHEN completed_date IS NULL THEN 'In Progress'
# MAGIC         WHEN completed_date > promised_date THEN 'Delayed'
# MAGIC         ELSE 'On Time'
# MAGIC     END AS delivery_status
# MAGIC FROM silver_service_jobs_enriched
# MAGIC ORDER BY received_date
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md #### GROUP BY with HAVING — Repeat Customer Signal

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, customer_name, COUNT(job_id) AS total_visits,
# MAGIC     COUNT(DISTINCT issue_type) AS distinct_issue_types
# MAGIC FROM silver_service_jobs_enriched
# MAGIC GROUP BY customer_id, customer_name
# MAGIC HAVING COUNT(job_id) > 1
# MAGIC ORDER BY total_visits DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md #### Window Function with PARTITION BY — Technician Avg Repair Time Alongside Each Job

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT job_id, technician_id, technician_name, repair_duration_days,
# MAGIC     ROUND(AVG(repair_duration_days) OVER (PARTITION BY technician_id), 2) AS technician_avg_repair_days,
# MAGIC     repair_duration_days - ROUND(AVG(repair_duration_days) OVER (PARTITION BY technician_id), 2) AS deviation_from_avg
# MAGIC FROM silver_service_jobs_enriched
# MAGIC WHERE job_status = 'Completed'
# MAGIC ORDER BY technician_id, job_id
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md #### CTE with ROW_NUMBER — Most Recent Job per Customer (No Subquery Nesting)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ranked_jobs AS (
# MAGIC     SELECT customer_id, customer_name, job_id, issue_type, job_status, received_date, delivery_flag,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY received_date DESC) AS rn
# MAGIC     FROM silver_service_jobs_enriched
# MAGIC )
# MAGIC SELECT customer_id, customer_name, job_id, issue_type, job_status, received_date, delivery_flag
# MAGIC FROM ranked_jobs
# MAGIC WHERE rn = 1
# MAGIC ORDER BY received_date DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md #### Multi-Table Join — Device Brand Delay Rate (Silver Fact + Gold Aggregate Style)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT d.brand, d.device_type, COUNT(sj.job_id) AS total_jobs,
# MAGIC     SUM(CASE WHEN sj.delivery_flag = 'Delayed' THEN 1 ELSE 0 END) AS delayed_jobs,
# MAGIC     ROUND(100.0 * SUM(CASE WHEN sj.delivery_flag = 'Delayed' THEN 1 ELSE 0 END) / COUNT(sj.job_id), 1) AS delay_rate_pct
# MAGIC FROM silver_service_jobs_enriched sj
# MAGIC JOIN bronze_devices d ON sj.device_id = d.device_id
# MAGIC WHERE sj.job_status = 'Completed'
# MAGIC GROUP BY d.brand, d.device_type
# MAGIC ORDER BY delay_rate_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake Capabilities
# MAGIC
# MAGIC Delta Lake is the storage format for every table in this pipeline (Databricks managed
# MAGIC tables default to Delta). Three production-critical capabilities are demonstrated below.

# COMMAND ----------

# MAGIC %md ### 1. Time Travel — Query a Table as It Appeared at a Prior Version

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver_service_jobs_enriched

# COMMAND ----------

# Query the table as of an earlier version (version 0 = the very first write).
# In a real incident, this is how you would inspect Gold before a bad transformation reached it.
time_travel_df = spark.read.format("delta").option("versionAsOf", 0).table("silver_service_jobs_enriched")
print(f"silver_service_jobs_enriched @ version 0: {time_travel_df.count()} rows")
print(f"silver_service_jobs_enriched @ latest:     {spark.table('silver_service_jobs_enriched').count()} rows")

# COMMAND ----------

# MAGIC %md ### 2. Schema Enforcement — Malformed Writes Are Rejected, Not Silently Corrupted

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

bad_schema_df = spark.createDataFrame(
    [("BADROW1", 12345)],  # 12345 as an int where a STRING job_status is expected — schema mismatch
    StructType([StructField("job_id", StringType()), StructField("job_status", IntegerType())])
)

try:
    bad_schema_df.write.mode("append").saveAsTable("silver_service_jobs_enriched")
    print("[UNEXPECTED] Write succeeded — schema enforcement did not trigger.")
except Exception as e:
    print(f"[SCHEMA ENFORCEMENT WORKED] Write correctly rejected:\n{str(e)[:300]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. SCD Type 2 with MERGE — Tracking Technician Attribute History
# MAGIC
# MAGIC Technician details (e.g. a name correction, or a title/skill-level change) can change
# MAGIC over time. A production data warehouse must preserve the *historical* value that was true
# MAGIC when each job was assigned — not silently overwrite it. This is a classic **Slowly
# MAGIC Changing Dimension Type 2** pattern, implemented with Delta Lake's `MERGE INTO`.

# COMMAND ----------

from pyspark.sql.functions import current_date, lit

# Build the initial technician dimension (one current row per technician)
technicians_initial = (
    spark.table("silver_service_jobs_enriched")
    .select("technician_id", "technician_name")
    .distinct()
    .withColumn("effective_start_date", current_date())
    .withColumn("effective_end_date", F.lit(None).cast("date"))
    .withColumn("is_current", F.lit(True))
)
technicians_initial.write.mode("overwrite").saveAsTable("dim_technician")
print("[SCD2] Initial dim_technician:")
display(spark.table("dim_technician").orderBy("technician_id"))

# COMMAND ----------

# Simulate a real-world change: T003's name was recorded with a typo and is now corrected.
updates = spark.createDataFrame(
    [("T003", "Priya Singh Rao")],  # corrected/updated name
    ["technician_id", "new_technician_name"]
)

# Step 1: close out the current record for the technician(s) being updated
updates.createOrReplaceTempView("updates_temp")
spark.sql("""
    MERGE INTO dim_technician AS target
    USING updates_temp AS src
    ON target.technician_id = src.technician_id AND target.is_current = true
    WHEN MATCHED THEN UPDATE SET
        target.is_current = false,
        target.effective_end_date = current_date()
""")

# Step 2: insert the new current record
new_version = updates.select(
    F.col("technician_id"),
    F.col("new_technician_name").alias("technician_name"),
    current_date().alias("effective_start_date"),
    F.lit(None).cast("date").alias("effective_end_date"),
    F.lit(True).alias("is_current"),
)
new_version.write.mode("append").saveAsTable("dim_technician")

print("[SCD2] dim_technician after MERGE — T003 now has both historical and current rows:")
display(spark.sql("SELECT * FROM dim_technician WHERE technician_id = 'T003' ORDER BY effective_start_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Value Summary
# MAGIC
# MAGIC | Question | Answered By |
# MAGIC |----------|---------------|
# MAGIC | Which technician is fastest / consistently over the promised date? | `gold_technician_performance` |
# MAGIC | How many customers are returning for the same recurring issue? | `gold_repeat_customers` (cross-referenced with `issue_type` in Silver) |
# MAGIC | What % of jobs are delayed, and by how much? | `gold_delay_analysis` |
# MAGIC | Which device brand drives the most repair volume? | `gold_device_brand_trends` |
# MAGIC | What was each customer's most recent job and its outcome? | `gold_customer_visit_history` |
# MAGIC
# MAGIC **Medallion recap:** Bronze preserves the source exactly as received. Silver establishes
# MAGIC quality (de-duplication, technician resolution, repair-duration/delay computation) and
# MAGIC joins the three source tables into one enriched fact record. Gold serves the stakeholder
# MAGIC with five purpose-built aggregate tables, queryable in seconds via plain SQL. Delta Lake
# MAGIC underpins every layer with ACID writes, time travel, schema enforcement, and SCD Type 2
# MAGIC history tracking for the technician dimension.
