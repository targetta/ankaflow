# AnkaFlow

**Write Once, Run Anywhere** — SQL-powered, YAML-defined data pipelines that work in **Python or the Browser**.

Run data workflows that combine Parquet, REST APIs, and SQL transforms with no infrastructure, using [DuckDB](https://duckdb.org) under the hood. Supports local Python, Pyodide (browser), and Chrome/Firefox Extensions.

## 🚀 Features

- ✅ Run pipelines **in Python or in-browser** (Pyodide/WebAssembly)
- ✅ Supports Parquet, REST, GraphQL, DeltaLake, BigQuery, ClickHouse, and more
- ✅ **No-code pipelines** via YAML: source → transform → sink
- ✅ **Modular**: browser/server backends, cloud optional
- ✅ Built on DuckDB with SQL and optional Python transforms
- ✅ Compatible with [Pyodide](https://pyodide.org), Chrome Extensions, and GitHub Pages

## 🛠 Quickstart

### 🐍 Python (server)

```bash
uv pip install -e .[server]
ankaflow pipeline.yaml
```

### 🌐 Browser (Pyodide)

1. Open [demo](https://your-demo-url/)
2. Upload a `pipeline.yaml` file
3. View SQL output live in your browser

### 🧪 Dev Environment

```bash
uv pip install -e .[dev,server]
```

## 📦 Installation

Install only the core (minimal setup with Parquet, JSON S3 support for Pyodide or remote embedding; does not include databases):

```bash
uv pip install ankaflow
```

Install with full server capabilities (BigQuery, ClickHouse, Delta write):

```bash
uv pip install ankaflow[server]
```

Install for development:

```bash
uv pip install -e .[dev,server]
```

## 📂 Project Layout

- `ankaflow/` – Python engine and core logic
- `docs/` – Markdown and API docs (generated via pdoc)


Upload a `pipeline.yaml` like:

```yaml
- name: Load
  kind: source
  connection: { kind: Parquet, locator: data$*.parquet }

- name: View
  kind: transform
  query: select * from Load
```

## 🧠 Roadmap

- [x] Pyodide-compatible core
- [x] Remote file support (S3, GCS)
- [x] Chrome/Firefox extension support
- [ ] IndexedDB caching
- [ ] REST-to-SQL join templating
- [ ] JupyterLite integration

## 🙌 Contributing

PRs welcome. See [CONTRIBUTING.md](CONTRIBUTING.md)

## 📄 License

MIT License
