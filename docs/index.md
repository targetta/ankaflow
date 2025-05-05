
# AnkaFlow - Run Data Pipelines Anywhere

**From REST APIs to SQL, from Local Python to Browser Execution**

---

## What is AnkaFlow?

AnkaFlow is a YAML-driven, SQL-powered data pipeline framework designed for both local Python and in-browser (Pyodide) execution. It enables seamless extraction, transformation, and joining of data across REST APIs, cloud storage, and databases, all without writing custom Python code.

**Write your pipeline once, run it anywhere.**

---

## Key Features

- **Dual Execution Modes**: Run pipelines locally or fully in-browser with Pyodide.
- **DuckDB In-Memory SQL Engine**: Fast, scalable analytics with SQL.
- **Dynamic Templating**: Full support for variable injection, header and query templating.
- **REST & GraphQL Support**: Production-ready REST and GraphQL connectors with error handling and polling.
- **Joins Across REST and SQL**: Native support for combining API responses with SQL datasets.
- **Python Transform Stage**: Execute custom Python logic inline within your pipeline.
- **DeltaLake, BigQuery, S3, MSSQL, Oracle**: Seamlessly connect to enterprise data sources.
- **YAML Anchors & References**: DRY pipeline definitions with reusable components.
- **Async Ready and Future-Proof**: Designed for scalable and parallel execution.

---

## Example Use Cases

- **Data Enrichment Pipelines**: Join Shopify orders (REST), DeltaLake financials, BigQuery users, and real-time weather data.
- **Browser-Based Data Apps**: Execute pipelines directly in the browser, preserving data privacy.
- **ML Feature Engineering**: Combine SQL and Python transform steps for complex feature generation.
- **SaaS Product Integrations**: Embed pipelines into dashboards, trigger REST calls, and process responses.
- **Ad-hoc Analysis and Reporting**: Dynamic pipelines for analysts and consultants, no Python code required.

---

## Why Choose AnkaFlow?

# AnkaFlow vs Other Pipeline Frameworks

| **Feature**                        | **AnkaFlow**              | **Airflow**              | **Dagster**              | **Bonobo**               | **Luigi**                | **DLT**                    |
|------------------------------------|----------------------------|---------------------------|---------------------------|---------------------------|---------------------------|-----------------------------|
| **In-Browser Execution (Pyodide)** | ✅ Yes                    | ❌                        | ❌                        | ❌                        | ❌                        | ❌                          |
| **Dynamic Templating**             | ✅ Yes                    | 🔶 Partial (Jinja)        | 🔶 Partial                | 🔶 Basic                  | 🔶 Basic                  | 🔶 via Python               |
| **REST + SQL Join**                | ✅ Native                 | 🔶 Plugin-based           | 🔶 Possible               | 🔶 Indirect               | 🔶 Indirect               | 🔶 via SQLMesh              |
| **Python Transform**               | ✅ Yes                    | 🔶 Plugin-based           | ✅ Yes                    | ✅ Yes                    | ✅ Yes                    | ✅ Yes                      |
| **Pure SQL Transforms**            | ✅ Native (DuckDB SQL)    | 🔶 via Plugins            | 🔶 Limited SQL Nodes      | ❌                        | ❌                        | ✅ via Destinations         |
| **BigQuery / Delta / S3 **  | ✅ Native Support         | 🔶 via Plugins            | ✅ Integrations           | 🔶 User-managed           | 🔶 User-managed           | ✅ Native                   |
| **Recursive YAML / Anchors**       | ✅ Yes                    | 🔶 via Jinja              | 🔶 Partial                | ❌                        | ❌                        | ❌                          |
| **External System Requirements**   | ✅ None — self-contained  | ❌ Requires DB & Scheduler | 🔶 Optional Metadata DB   | ✅ Lightweight — no deps  | ❌ Requires Scheduler     | ✅ No built-in orchestration |
| **Configuration-First Design**     | ✅ Declarative — code optional | 🔶 Code-first with DAGs    | 🔶 Hybrid — config & code | 🔶 Mostly code-based      | 🔶 Code-centric           | ❌ Code is required (Python) |

---

## Roadmap Highlights

- ✅ Fully battle-tested REST and GraphQL support
- ✅ Python transform stage shipped
- ✅ IndexedDB caching
- 🟠 Built-in data lineage tracking
- 🟠 Parallel execution in local runtime

---

## Get Started Today

Write once, run anywhere — from your laptop to the browser. AnkaFlow pipelines adapt to your workflow, combining flexibility, power, and portability.

[Learn more](#) or [View Examples](#)


---

## 📖 Documentation

- [Technical Summary](overview/)
- [Pipeline Specification](api/ankaflow.models/)
- [AnkaFlow API](api/ankaflow/)

---

#### Built with

[DuckDB](https://duckdb.org) | [YAML](https://yaml.org/) | [Jinja](https://jinja.palletsprojects.com/en/stable/)