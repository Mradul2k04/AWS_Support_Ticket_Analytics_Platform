## Customer Support Ticket Analytics Platform 

## 1) Project overview 

- - - The Customer Support Ticket Analytics Platform is an end to end, cloud based data engineering pipeline built for the AWS track of the Data Engineering Fellowship. The project addresses a real operational gap: an international software company's helpdesk team has years of ticket history sitting in raw CSV exports, with no consolidated way to see ticket volume, resolution performance, or agent workload. Support leadership has been relying on manual spreadsheet work, ' which doesn t scale and offers no trend visibility. 

The pipeline ingests two real, linked source files — `issues.csv` and 

`issues_change_history.csv` , sourced from the Mendeley "Help Desk Tickets" dataset LJan 2016^Mar 2023, �60,000+ tickets) — and processes them through a fully automated AWS-native architecture: Amazon S3 (raw and curated zones), AWS - Lambda (event driven ETL trigger), PySpark (transformation and metric - engineering), Amazon RDS PostgreSQL (analytics ready storage), and Matplotlib/Seaborn/Streamlit (visualization layer). 

' Architecturally, the pipeline reconstructs each ticket s current state — assignee and status — from the change-history log rather than assuming it from the static issues file, which mirrors how real production support systems behave. From - there, PySpark computes resolution time metrics ( `resolution_duration_hours` , 

`resolution_target_met` ) and loads a star schema ( `fact_tickets` plus `dim_agents` , - `dim_customers` , `dim_projects` ) into PostgreSQL, alongside three pre aggregated analytics tables. 

The platform was designed around three core business use cases: resolution - performance analysis LSLA style compliance by priority), agent workload and performance (ticket distribution and efficiency by agent), and ticket category - trends (recurring or fast growing issue categories). Each use case follows a consistent pattern — ETL logic, an output table in RDS, and a dashboard view — so the technical pipeline maps directly back to a business question support leadership actually needs answered. 

Customer Support Ticket Analytics Platform 

1 

## 2) Architecture design : 

```
issues.csv / issues_change_history.csv
  |
  v
Amazon S3 (raw zone)
  |
  v
PySpark ETL
  |
  v
Amazon S3 (curated zone)
  |
  v
AWS RDS (PostgreSQL)
  |
  v
Matplotlib / Seaborn / Streamlit
  |
  v
Support Operations Dashboard
```

## 3) Dataset details 

## 3.1 Source & volume 

|Feature|Description|
|---|---|
|Dataset name|AWS Support Ticket Data|
|Source|Internal Support System|
|Description|Historical AWS support tickets tracking lifecycle from creation to<br>resolution|
|Record count|�60,000+tickets|
|Raw files|`issues.csv` ,<br>`issues_change_history.csv`|



## 3.2 What the data represents 

Tickets include information such as: 

Ticket creation and resolution timestamps 

Priority / severity 

Customer Support Ticket Analytics Platform 

2 

   - Issue type / category 

   - Assigned agent (or owner) 

   - Change history events (status changes, reassignments, etc.) 

- Primary analytics goals: 

   - 

   - SLA / resolution target compliance 

   - Workload distribution and efficiency by agent 

   - Trend analysis by issue category over time 

## 4) Cloud storage: AWS S3 Data Lake 

Amazon S3 is used as the central Data Lake to store data throughout its lifecycle. 

## 4.1 Bucket setup (high level) 

- Create a dedicated S3 bucket for this project (naming convention based on your org/project standards). 

- Enable appropriate IAM access (least privilege). 

- ] ] 

- LOptional) Enable versioning and server side encryption LSSE S3 or SSE KMSM. 

Customer Support Ticket Analytics Platform 

3 

## 4.2 Folder layout (zone separation) 

## Recommended layout: 

## Bronze LRaw Zone) 

Stores unmodified raw CSVs from the source system. 

- Example: `s3://<bucket>/bronze/issues.csv` 

- Example: `s3://<bucket>/bronze/issues_change_history.csv` 

Customer Support Ticket Analytics Platform 

4 

## Gold LCurated Zone) 

- Stores cleaned, modeled, analytics ready data produced by PySpark, typically as Parquet LSnappyM. 

- Example: `s3://<bucket>/gold/fact_tickets/` 

- Example: `s3://<bucket>/gold/dim_agent/` 

## 5) Architecture & ETL flow 

## - - 5.1 End to end flow 

## ��> Extract 

Read raw CSV files from S3 LBronze zone). 

- ��> Transform LPySpark) 

Core transformation responsibilities: 

- Schema validation & type casting 

- Data quality checks (null handling, anomaly filtering) 

- Surrogate key generation for dimensions (as needed) 

- Metric engineering, e.g.: 

`resolution_duration_hours` 

Customer Support Ticket Analytics Platform 

5 

- 

- `resolution_target_met` (priority based threshold) 

## Build a star schema: 

- `fact_tickets` 

- multiple dimension tables (e.g., agent, category, priority, date) 

## ��> Load 

- Write curated outputs to S3 LGold zone) as Parquet LSnappyM for storage and query efficiency. 

- Load curated tables into AWS RDS PostgreSQL using JDBC for 

- downstream SQL analytics and dashboard consumption. 

## 5.2 Data model (star schema) 

Typical modeled entities: 

## Fact 

   - `fact_tickets` : one row per ticket (or per ticket event depending on design) 

- Dimensions (examples; adjust to your implementation) 

   - `dim_agent` 

   - `dim_category` 

   - `dim_priority` 

Customer Support Ticket Analytics Platform 

6 

- 

- `dim_date` (optional but common for time series reporting) 

## 5.3 Key engineered fields 

- `resolution_duration_hours` 

Computed time between created and resolved timestamps. 

- `resolution_target_met` 

Boolean derived by comparing resolution duration against thresholds by priority (e.g., P1 � X hours). 

## 6) SQL analysis (RDS PostgreSQL) 

After loading curated tables into PostgreSQL, run analytical queries to validate data and produce reporting datasets. 

## 6.1 Validation & business metrics (placeholders) 

Replace the following with your actual SQL tasks/queries and outputs: 

- Task 1o SLA compliance by priority (met target vs total + compliance %M 

Customer Support Ticket Analytics Platform 

7 

## Task 2o Top agents by tickets resolved (most recent month window) 

## Task 3o View — average resolution time by project 

## Task 4o Agent speed ranking (avg resolution hours + rank) 

Customer Support Ticket Analytics Platform 

8 

## Task 5o Top reporters by ticket volume (by month) 

## Task 6o Monthly resolved tickets + running total 

Customer Support Ticket Analytics Platform 

9 

## Task 7o Category lookup (example: `Project` ) 

## - Task 8o Resolution time stats by priority (avg / min / max / ticket count) 

Customer Support Ticket Analytics Platform 

10 

## Task 9o View — ticket status distribution (count + %M 

- Task 10o Detailed results (project + agent + avg resolution hours) 

Customer Support Ticket Analytics Platform 

11 

## 7) Business use cases (Streamlit visualizations) 

The Streamlit dashboard connects to the RDS PostgreSQL database using SQLAlchemy and presents interactive reporting across three core use cases. 

## Use case 1: Resolution performance analysis 

## Business goal 

Understand what share of tickets are resolved within target timelines, broken down by priority and over time. 

## ETL / analytics flow 

- 

- ��> Compute `resolution_target_met` per ticket using priority specific thresholds. 

- ��> Aggregate compliance rate by priority and month. 

- ��> Write aggregated results to RDS (example output: 

   - `analytics.resolution_performance` ). 

Customer Support Ticket Analytics Platform 

12 

## Dashboard outputs 

- Bar chart: compliance rate by priority 

- Line chart: monthly compliance trend 

- KPI: overall compliance percentage 

## Expected insight 

Identifies which priority levels frequently miss targets, enabling targeted staffing and process improvements. 

## Use case 2: Agent workload & performance 

## Business goal 

Understand distribution of ticket load and resolution efficiency across agents. 

## ETL / analytics flow 

- ��> Aggregate `fact_tickets` by `agent_id` : 

   - ticket count 

   - average `resolution_duration_hours` 

- ��> Rank agents by tickets resolved (window function). 

- ��> Write ranked metrics to RDS (example output: `analytics.agent_performance` ). 

Customer Support Ticket Analytics Platform 

13 

## Dashboard outputs 

- Bar chart: tickets resolved per agent 

- Trend chart: average resolution time per agent (over time) 

- Distribution chart: workload spread (identify overloaded vs underutilized) 

## Expected insight 

Highlights top performers and workload imbalance to guide coaching, staffing, and process improvements. 

## Use case 3: Ticket category trends 

## Business goal 

Identify recurring or fast-growing issue categories that may indicate product gaps or documentation needs. 

## ETL / analytics flow 

- ��> Aggregate tickets by category and month: 

   - ticket volume 

   - average `resolution_duration_hours` 

- - 

- ��> Compute month over month LMoMM % change in volume per category. 

- ��> Write category trends to RDS (example output: `analytics.category_trends` ). 

## Dashboard outputs 

Customer Support Ticket Analytics Platform 

14 

- Heatmap: category vs month ticket volume 

- Line chart: fastest-growing categories 

- KPI: top recurring category 

## Expected insight 

- Surfaces categories that warrant product fixes or improved self service documentation. 

## 8) Conclusion 

This project delivers a working, automated analytics pipeline for AWS support ticket data, taking raw, unstructured helpdesk exports and turning them into a governed, queryable, and visualized data product. By combining PySpark for - - scalable transformation, a Lambda triggered event driven architecture for - automation, S3 for durable zone separated storage, and RDS PostgreSQL as the - analytics layer, the pipeline removes the need for manual pipeline kick off or spreadsheet-based reporting. 

The resulting dashboards give support leadership direct visibility into three previously invisible dimensions of their operation: whether tickets are meeting - resolution time targets by priority, how workload and efficiency are distributed across agents, and which issue categories are trending upward in volume — a signal for potential product or documentation gaps. Each of these outputs is backed by validated, deduplicated, and reconciled data, with 15 logged test cases 

Customer Support Ticket Analytics Platform 

15 

- - covering edge cases like duplicate ticket IDs, out of order timestamps, unassigned tickets, and idempotent re-runs. - Beyond the technical deliverable, the project reinforced practical, production relevant skills: reconstructing current-state records from an event/change-history - log, designing a star schema for time series and workload reporting, building idempotent load logic keyed on a natural identifier, and wiring an event-driven trigger LS3 → Lambda → PySpark) rather than relying on manual orchestration. The architecture is intentionally scoped to a small, defensible set of AWS services, - demonstrating that a production style analytics platform can be built without unnecessary infrastructure complexity 

Customer Support Ticket Analytics Platform 

16 

