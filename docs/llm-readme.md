# Leveraging LLMs in Data Pipelines with AnkaFlow

AnkaFlow allows you to integrate powerful language models like OpenAI's GPT into a SQL-based ETL workflow — not for generating text, but for generating *structured SQL queries* dynamically from user questions. This turns natural language into executable logic as part of a data pipeline. Here's how to structure such a pipeline, what the components do, and how to generalize it to your own use case.

---

## 🧠 What Are We Building?

A pipeline that:
1. Loads structured data (e.g., Parquet files).
2. Extracts schema information.
3. Sends metadata + user question to an LLM (e.g., GPT).
4. Receives a SQL query as output.
5. Executes the query and optionally returns results.

---

## 🧱 Pipeline Structure Overview

AnkaFlow pipelines are defined in YAML and executed via an in-memory DuckDB engine. A pipeline consists of:

- **`source`** steps to load structured data or call external services (like an LLM) and generate queries
- **`transform`** steps to apply logic
- **`sink`** steps to persist results or make them available for later reuse

---

## 🧩 LLM Integration: Step-by-Step

### 1. Load Your Data

Use `source` steps to ingest data from Parquet, SQL, or REST APIs:

```yaml
- name: LoadSales
  kind: source
  connection:
    kind: Parquet
    locator: data/sales_data*.parquet
```

### 2. Discover Schema Dynamically

Use a SQL query to inspect the tables you've loaded:

```yaml
- name: Describe
  kind: self
  query: >
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'main'
```

### 3. Save Schema as a Variable

Store the schema result for reuse in the LLM call:

```yaml
- name: SetSchema
  kind: sink
  connection:
    kind: Variable
    locator: DiscoveredSchema
```

### 4. Prompt Template (Header Block)

Define a reusable LLM prompt using Jinja2 templating:

```yaml
- name: Header
  kind: header
  prompt: &Prompt |
    You are a SQL query generator.
    Schema: {{ schema_json }}
    {% if relations_json %}Relations: {{ relations_json }}{% endif %}
    Question: {{ user_prompt }}
```

### 5. Generate SQL via LLM

Use a `tap` step with an LLM backend:

```yaml
- name: GenerateSQL
  kind: tap
  connection:
    kind: SQLGen
    config:
      kind: openai
    variables:
    schema_json: <<API.look('DiscoveredSchema', variables) | tojson>>
    user_prompt: <<API.look('UserPrompt', variables)>>
  query: *Prompt
```

Here, the LLM is used to translate a natural language prompt into executable SQL using the live schema as context. The generated query will be injected to the pipeline generating new output.

The output can be examined by setting `show: 1`.

---

## 🔄 Design Considerations

- **Idempotent Execution**: Each step is deterministic. Even LLM steps are cacheable and inspectable.
- **Traceability**: LLM input and output are transparent; prompt + schema + response are all visible.
- **Declarative Logic**: SQL output is injected back into the pipeline like any other transform.
- **Custom Connectors**: Can integrate other backends besides OpenAI (e.g., local models or proxies).

---

## 🔧 Use Cases

- **Ad Hoc Analytics**: Analysts can phrase questions in plain language.
- **Self-Service Dashboards**: Business users ask "How many orders were delayed last month?" and get results.
- **Semantic SQL Layer**: Front-end apps dynamically generate SQL from user queries without manual query writing.

---

## 🛠 Generalization Tips

- Template your LLM prompts. Store them under `- kind: header` and inject metadata via `variables`.
- Always pass schema as JSON — ideally normalized via an inspection query.
- Store LLM results in `Variable` sinks to reuse, log, or audit them.
- Use environment-specific `backend` config for OpenAI, local, or proxy usage.

---

## 🚀 Running the Pipeline

Once defined, pipelines can be run locally:

```bash
ankaflow pipeline.yaml
```

Or embedded into browser apps via Pyodide.

---

## 📌 Summary

| Feature                  | Benefit                             |
|--------------------------|-------------------------------------|
| Schema-aware prompts     | More accurate SQL generation        |
| Variable injection       | Reuse outputs across steps          |
| YAML-first architecture  | Version-controlled, auditable flow  |
| OpenAI backend (or other)| Plug in any LLM provider            |
