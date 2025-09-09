# AnkaFlow

**Run your data pipelines in Python or the browser.**  
AnkaFlow is a YAML + SQL-powered data pipeline engine that works in local Python, JupyterLite, or fully in-browser via Pyodide.

## 🚀 Features

- Run pipelines using DuckDB with SQL and optional Python
- Supports Parquet, REST APIs, BigQuery, ClickHouse (server only)
- Browser-compatible: works in JupyterLite, GitHub Pages, VS Code Web and more

## 📦 Install

```bash
# Server
pip install ankaflow[server]

# Dev
pip install -e .[dev,server]
```

## 🛠 Usage

```bash

> ankaflow /path/to/stages.yaml
```

```python
from ankaflow import (
    ConnectionConfiguration,
    Stages,
    Flow,
)

connections = ConnectionConfiguration()

stages = Stages.load("path/to/stages.yaml")
flow = Flow(stages, connections)
flow.run()
```

## 🔁 What is `Stages`?

`Stages` is the object that holds your pipeline definition parsed from a YAML file.  
Each stage is one of: `tap`, `transform`, or `sink`.

### Example

```yaml
- name: Extract Data
  kind: tap
  connection:
    kind: Parquet
    locator: input.parquet

- name: Transform Data
  kind: transform
  query: SELECT * FROM "Extract Data" WHERE "amount" > 100

- name: Load Data
  kind: sink
  connection:
    kind: Parquet
    locator: output.parquet
```

## 📖 Documentation

- [All docs](https://targetta.github.io/ankaflow/)
- [Pipeline specification](https://targetta.github.io/ankaflow/api/ankaflow.models/)
- [Live demo](https://targetta.github.io/ankaflow/demo/)

---