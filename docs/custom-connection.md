# CustomConnection User Guide
Welcome to the CustomConnection user guide. This document will walk you through:

- What `CustomConnection` is and when to use it
- The available configuration fields
- How to reference a `CustomConnection` in your pipeline YAML
- Example usage and best practices

---

## 1. Introduction

`CustomConnection` allows you to plug in your own connection logic into the Ductflow pipeline. Your custom connection class must implement the base `Connection` interface (or derive from it), and provide the following methods:

- `tap()`: Extract data from the source
- `sink()`: Write data to the destination
- `sql()`: Execute or generate SQL statements
- `show_schema()`: Display or return the table/schema structure

Even if your class does not need all of these for its logic, it must expose them (they can be no-ops).

---

## 2. CustomConnection Fields

Below is a description of each field in the `CustomConnection` model.

| Field      | Type                             | Description                                                                                  |
|------------|----------------------------------|----------------------------------------------------------------------------------------------|
| `kind`     | `Literal["CustomConnection"]`   | Must always be set to `CustomConnection` to select this provider.                            |
| `module`   | `str`                            | Python module path containing your custom connection class (e.g. `myapp.connectors.database`).    |
| `classname`| `str`                            | The name of the class to load from the specified module.                                     |
| `params`   | `dict`<br><small>(default `{}`)</small> | Arbitrary parameters passed into your connection’s constructor.                              |
| `config`   | `ConnectionConfiguration \| None`| (Optional) Pre-built configuration object injected by BaseConnection super-class.            |
| `fields`   | `List[Field] \| None`           | (Optional) Schema fields, auto-populated or used by the base implementation if needed.      |
| `locator`  | `str \| None`                   | (Optional) Name or identifier used by `Connection.locate()` for dynamic discovery.         |

---

## 3. Defining a CustomConnection in Your Pipeline

In your pipeline YAML, under a `sink` or `tap` stage, you reference a custom connection like this:

```yaml
- name: MySqlWriter
  kind: sink
  connection:
    kind: CustomConnection
    module: myapp.connectors.database
    classname: MySQL
    params:
      port: 5555
```

Explanation:

1. **`name:`** Logical name for the pipeline stage (`MySqlWriter`).
2. **`kind: sink`** Specifies that this stage writes data (a "sink" stage).
3. **`connection:`** Block configures how to connect to the target system:
    - `kind`: Must be `CustomConnection`.
    - `module`: Python import path where your class lives.
    - `classname`: Actual class name inside that module.
    - `params`: Any keyword arguments your class expects (e.g. table name, credentials key).
4. **`show: 1`** Enables schema introspection after connecting (Debug mode).

---

## 4. Implementing Your Custom Class

In `myapp/connectors/database.py`:

```python
from duckflow.connection import Connection

class MySQL(Connection):
    def init(self):
        # This method is provided by base class as convenience to
        # avoid mucking with super().__init__()
        # It is called in the end of base class __init__ hence
        # self.config and other attributes are already populated.
        pass

    async def tap(self, query: str|None = None):
        # Extract data from external storage and cache to pipeline
        df = get_dataframe_from_mysql(query)
        await self.c.register("tmp_{self.name}", df)
        await self.c.sql(f'CREATE TABLE "{self.name}" AS SELECT * FROM data')
        # You can use CREATE OR REPLACE or CREATE IF NOT EXISTS
        # CREATE OR REPLACE Will overwrite existing data (does not enforce stage name uniqueness)
        # CREATE IF NOT EXISTS will append data if table already exists.
        await self.c.unregister("tmp_{self.name}")

    async def sink(self, from_name: str):
        # Write data to external storage
        # from_name is always previous stage name (presumably tap or transform)
        relation = await self.c.sql(f'SELECT * FROM "{from_name}"')
        df = await relation.df() # of fetchall()
        send_df_to_mysql(df)

    async def sql(self, query: str):
        # Execute SQL logic on target
        execute_sql_on_mysql(query)

    async def show_schema(self):
        # Return table schema, e.g. list of Field objects
        return []
```

> **Note:** All methods must be async.

> **Note:** Your class must derive from the base `Connection` to inherit common logic and have access to `self.conn.cfg`, `self.conn.locator()`, etc.

---

## 5. Best Practices & Troubleshooting

- **Validation**: Always validate your `params` against what your class expects. Mismatches will not raise errors at `Stages.load()` time but at runtime.
- **Module Path**: Ensure your project’s root is on `PYTHONPATH` so that `import module` succeeds at runtime.
- **Debugging**: Use `show: 1` in your YAML to print the output of tap().
- **Error Handling**: Wrap your I/O in try/except blocks and surface meaningful messages; Ductflow will propagate exceptions upstream.

---

## 6. Download & Next Steps

You can download this guide as a standalone `downloadable.md`, or embed it in your project docs. For advanced scenarios (e.g. dynamic locator lookup, multi-tenant configs), consult the developer reference.

Happy piping!
