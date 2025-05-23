
---

## ✅ 2. `PROJECT_JOURNAL.md`

A personal or technical journal of your build process — great for interviews, documentation, or sharing your journey.

---

### 📓 `PROJECT_JOURNAL.md`

```markdown
# 📓 Project Journal: Real-Time Telecom CDR Pipeline

---

## 📅 Day-by-Day Progress

### ✅ Day 1: Setup

- ✅ Initialized ADLS container: `hourlycdr`
- ✅ Created Unity Catalog schemas: `bronze`, `silver`, `gold`
- ✅ Developed Python CDR generator to simulate hourly telecom data

---

### ✅ Day 2: Bronze Layer (Ingestion)

- ✅ Used Auto Loader with `cloudFiles.format = csv`
- ✅ Defined schema explicitly due to malformed data
- ✅ Created streaming read and write to `bronze.cdr_raw`
- 🐞 Fixed `SparkNumberFormatException` (caller_number was wrongly cast to BIGINT)

---

### ✅ Day 3: Silver Layer (Transformation)

- ✅ Applied data cleansing (trim, null handling)
- ✅ Added derived columns: `weekend_flag`, `call_duration_mins`
- ✅ Filtered out bad records
- ✅ Wrote to `silver.cdr_cleaned`

---

### ✅ Day 4: Gold Layer (Aggregation)

- ✅ Summarized metrics by `call_hour`, `call_date`
- ✅ Aggregated columns:
  - total_calls
  - avg_duration
  - total_data_usage
- ✅ Output to `gold.cdr_summary`

---

### ✅ Day 5: Orchestration

- ✅ Created Databricks Job with 3 dependent tasks
- ✅ Configured trigger: file arrival in `cdr_data`
- ✅ Added success & failure notifications via email

---

### ✅ Day 6: Dashboarding

- ✅ Built real-time dashboard using Databricks SQL
- ✅ Visuals:
  - Line chart of hourly call volume
  - Bar chart: weekend vs weekday calls
  - Heatmap: calls by hour and date
- ✅ Published and shared dashboard

---

## 📌 Learnings & Highlights

- Auto Loader + Delta Lake = smooth real-time ingestion
- Proper schema enforcement is crucial for malformed CSVs
- Unity Catalog simplifies table access + security
- Job Triggers are powerful for real-time workflows
- Real-time dashboards are surprisingly simple with SQL

---

## 🔥 Future Improvements

- Add user location analysis
- Push real-time alerts for fraud detection
- Implement CI/CD with Repos + Jobs API
- Integrate with Power BI via JDBC

---

## 🙌 Credits

Built by Ayub Khan | Inspired by modern data engineering patterns | Powered by Databricks
