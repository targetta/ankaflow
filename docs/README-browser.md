# 🧪 AnkaFlow in the Browser (Pyodide)

This guide shows how to run **AnkaFlow pipelines fully in-browser**, using Pyodide (Python in WebAssembly).  
No server, no install — pipelines run client-side using the same YAML-based definitions.

---

## 🚀 Demo Options

| Method                  | Link                                              |
|-------------------------|---------------------------------------------------|
| 🧪 JupyterLite Notebook | [Launch Demo Notebook](#) |
| 🌐 HTML SPA Demo        | [Try YAML Upload Demo](#) |

---

## 📦 How It Works

- AnkaFlow is compatible with Pyodide (via `micropip`)
- Remote files (e.g., S3, GCS) are fetched using `pyodide.http.pyfetch` or custom implementation (e.g. `axios`)
- The SQL engine is DuckDB (running in WASM)
- Everything is local to your browser

---

## 🧰 Example (JupyterLite or Pyodide)

```python
import micropip
await micropip.install("ankaflow")

from ankaflow import Flow
yaml = '''
- name: Load
  kind: source
  connection:
    kind: Parquet
    locator: data.parquet

- name: View
  kind: transform
  query: select * from Load
'''

Flow().run(yaml)
```

---

## 🧠 Notes

- Only packages available in the Pyodide index can be used
- Some connectors (e.g., BigQuery, ClickHouse) are not available in the browser
- All pipelines must avoid server-only dependencies when targeting Pyodide

---

## 🛠 For Developers

If you're embedding AnkaFlow in a browser app:

1. Load Pyodide:
```html
<script src="https://cdn.jsdelivr.net/pyodide/v0.24.1/full/pyodide.js"></script>
```

2. Bootstrap and run:
```js
const pyodide = await loadPyodide();
await pyodide.loadPackage(["micropip"]);
await pyodide.runPythonAsync(`
    import micropip
    await micropip.install("ankaflow")
    from ankaflow import Flow
    Flow().run(...)
`);
```

---

## 📁 Browser-Specific Modules

The following modules help with Pyodide execution:

- `connections/rest/browser.py`: Fetches data via `pyfetch`
- `LocalFileSystem`: Writes to Pyodide's `/tmp`
- `ObjectDownloader`: Manages cloud-to-browser downloads

---

## ✅ Works Great In

- [x] Chrome
- [x] Firefox
- [x] JupyterLite
- [x] VS Code WebView
- [x] GitHub Pages
