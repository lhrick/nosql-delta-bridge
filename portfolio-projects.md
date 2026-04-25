# Data Engineering & Analytics Portfolio — Project Map

A structured roadmap of portfolio projects designed to demonstrate real-world data engineering and analytics skills. Projects are ordered by priority, starting with the most impactful.

---

## 🥇 Priority 1 — Anchor Project (Start Here)

### `nosql-delta-bridge` — NoSQL to Delta Lake Ingestion Abstraction

**The Problem**
Dynamic document schemas from NoSQL sources (MongoDB, Firestore, DynamoDB) constantly break naive ETL pipelines. Fields appear and disappear, types conflict across documents, nested objects vary in depth, and arrays hold mixed types. This is a real production pain point that most tutorials never address.

**What This Project Solves**
A Python library/framework that provides a resilient, configurable abstraction layer for ingesting schema-free documents into Delta Lake with predictable, auditable results.

**Core Features to Build**
- Schema inference engine from document samples, with conflict resolution strategies (widest type wins, nullable by default)
- Type coercion rules with configurable behavior per field (cast, reject, or flag)
- Nested document and array flattening with configurable depth
- Schema evolution support — new fields are merged, not breaking
- Dead letter queue — malformed documents are routed and logged, never silently dropped
- Audit metadata columns (`_ingested_at`, `_source_collection`, `_schema_version`)

**Suggested Stack**
| Layer | Tool |
|---|---|
| Source | MongoDB Atlas (free tier) or local JSON fixtures |
| Processing | PySpark or Pandas + PyArrow |
| Sink | Delta Lake via `delta-rs` (pure Python, no Spark cluster needed) |
| Orchestration | CLI first, then optionally Prefect |
| Testing | `pytest` with synthetic messy document fixtures |
| Docs | MkDocs or plain README with architecture diagram |

**Why It Stands Out**
- Solves a problem you experienced in production — the README can be written with real credibility
- `delta-rs` makes it fully reproducible without infrastructure
- Library-style design means others might actually use it (stars, forks)
- Dead letter queue and audit columns show production maturity

**Suggested Repo Structure**
```
nosql-delta-bridge/
├── nosql_delta_bridge/
│   ├── infer.py          # Schema inference
│   ├── coerce.py         # Type coercion rules
│   ├── flatten.py        # Nested/array flattening
│   ├── writer.py         # Delta Lake writer
│   └── dlq.py            # Dead letter queue
├── tests/
│   └── fixtures/         # Synthetic messy JSON docs
├── examples/
│   └── mongo_to_delta.py
├── docs/
│   └── architecture.md
└── README.md
```

---

## 🥈 Priority 2 — End-to-End Pipeline

### `open-pipeline` — Production-Style ETL Pipeline with Orchestration

**The Problem**
Most portfolio pipelines are single scripts. This one mimics how a real data team ships a pipeline: modular, scheduled, monitored, and documented.

**What to Build**
A full pipeline from a public API (weather, finance, or Reddit) through a raw → cleaned → aggregated layer, orchestrated with Airflow or Prefect, with data quality checks at each layer.

**Core Features**
- API ingestion with rate limit handling and retry logic
- Raw landing zone → dbt transformation layers (staging, intermediate, mart)
- Data quality checks with Great Expectations or dbt tests
- Alerting on failures (email or Slack webhook)
- Docker Compose to spin up the full stack locally

**Suggested Stack:** Python · Airflow or Prefect · dbt · DuckDB or BigQuery · Great Expectations · Docker

---

## 🥉 Priority 3 — Analytics Case Study

### `olist-churn-analysis` — Business Case Analysis on Brazilian E-Commerce

**The Problem**
Analysts are hired to drive decisions, not just make charts. This project frames an analysis like a real work deliverable.

**What to Build**
Using the public Olist dataset (Brazilian e-commerce), investigate seller churn or customer retention. Deliver: problem statement → SQL/Python analysis → statistical validation → business recommendation.

**Core Features**
- Cohort analysis and retention curves
- Funnel drop-off identification
- Statistical significance testing on key findings
- Final output: a polished notebook or Streamlit report with an executive summary section

**Suggested Stack:** Python · Pandas · SQL · Matplotlib/Plotly · Streamlit (optional)

---

## Priority 4 — dbt Showcase

### `dbt-modeling-showcase` — Proper Layered Data Modeling

**The Problem**
Most people list dbt on their resume but never show a real model. This project demonstrates you actually understand medallion/layered architecture.

**What to Build**
Take any dataset and model it correctly: `raw` → `staging` → `intermediate` → `mart` layers, with full dbt tests (not null, unique, relationships, accepted values), documentation, and the generated docs site hosted on GitHub Pages.

**Core Features**
- At least one mart with a clear business use case (e.g. daily revenue, user activity)
- Custom generic tests beyond the defaults
- Docs site deployed and linked from README
- Lineage graph screenshot in README

**Suggested Stack:** dbt Core · DuckDB or BigQuery · GitHub Pages for docs

---

## Priority 5 — Engineering Craft

### `data-stack-iac` — Reproducible Local Data Stack with IaC

**The Problem**
Setting up a local data stack is painful and undocumented. This project makes it one command.

**What to Build**
A Docker Compose + Makefile setup that spins up a complete local data stack: Postgres + Airflow + dbt + Metabase (or Superset). Document every architectural decision.

**Core Features**
- `make up` starts everything, `make down` tears it down cleanly
- Pre-loaded with sample data and example dashboards
- Architecture decision record (ADR) docs explaining tool choices
- Optional: Terraform config for cloud deployment (GCP or AWS free tier)

**Suggested Stack:** Docker · Docker Compose · Makefile · Terraform (optional) · Airflow · Metabase

---

## Priority 6 — Tooling / Open Source Contribution Style

### `csv-profiler-cli` — Command-Line Data Profiling Tool

**The Problem**
Before loading any dataset, you need to understand it. This CLI tool profiles any CSV or JSON file and surfaces schema issues, type inconsistencies, null rates, and outliers.

**What to Build**
A small, well-packaged Python CLI (`pip install csv-profiler`) that outputs a human-readable or JSON profile report.

**Core Features**
- Type inference per column with confidence score
- Null rate, cardinality, and distribution summary
- Outlier flagging (IQR or z-score)
- Output formats: terminal table, JSON, HTML report
- Publishable to PyPI

**Suggested Stack:** Python · Typer or Click · Rich · PyArrow · PyPI packaging

---

## Execution Order

```
1. nosql-delta-bridge     ← Start now. Real problem, high signal.
2. open-pipeline          ← Build while nosql project is "resting"
3. olist-churn-analysis   ← Balances engineering with analytics storytelling
4. dbt-modeling-showcase  ← Can be a sub-project of open-pipeline
5. data-stack-iac         ← Ties everything together
6. csv-profiler-cli       ← Build when you want something shippable/fun
```

---

## Cross-Project README Principles

Every project README should answer:
1. **What problem does this solve?** (1 paragraph, written like a PM brief)
2. **Architecture diagram** (even a simple ASCII one)
3. **How to run it** (must work on a clean machine)
4. **What you'd do differently** or next steps (shows reflection)
5. **Why you made key tech decisions** (Airflow vs Prefect, DuckDB vs Postgres, etc.)

This replaces the context you'd normally give in a job interview.
