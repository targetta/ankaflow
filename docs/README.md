# DuctFlow

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
python -m ductflow run pipeline.yaml
```

### 🌐 Browser (Pyodide)

1. Open [demo](https://your-demo-url/)
2. Upload a `pipeline.yaml` file
3. View SQL output live in your browser

### 🧪 Dev Environment

```bash
uv pip install -e .[dev,server]
pytest
```

## 📦 Installation

Install only the core (for Pyodide or remote embedding):

```bash
uv pip install ductflow
```

Install with full server capabilities (DuckDB, boto3, ClickHouse):

```bash
uv pip install ductflow[server]
```

Install for development:

```bash
uv pip install -e .[dev,server]
```

## 📂 Project Layout

- `ductflow/` – Python engine and core logic
- `docs/` – Markdown and API docs (generated via pdoc)
- `spa/` – Angular demo (compiled to single-page Pyodide app)
- `tests/` – Unit tests for SQL interceptors, paths, and pipeline runners

## 📖 Documentation

- [Technical Summary](docs/DUCT%20Technical_Summary.md)
- [Pipeline Specification](docs/DUCT%20GPT%20Pipeline_Specification.md)
- [Object Downloader](docs/downloader_localfs_documentation.md)
- [API Docs (pdoc)](https://yourdomain.github.io/DuctFlow/)

## 🌍 Live Demo

Try DuctFlow fully in-browser:

👉 [Launch browser demo](https://yourdomain.github.io/DuctFlow/demo.html)

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
