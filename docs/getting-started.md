
# Getting Started with Duckflow

Duckflow is a powerful data pipeline framework that allows you to define, manage, and execute complex workflows involving data sourcing, transformation, and storage. This guide walks you through the basic steps to get started with Duckflow, including setting up the pipeline, installing dependencies, and running your first pipeline.

### Prerequisites

Before you get started, make sure you have the following installed on your machine:

1. Python 3.12 or later
2. `pip` (Python's package installer)

### A. Install Duckflow

To begin using Duckflow, install the required packages by running:

```bash
pip install duckflow
```

In server environment you may want to include additional connectors

```bash
pip install duckflow[server]
```

This will install Duckflow and its necessary dependencies, including `duckdb`, `yaml`, and `pandas`.


### B. Environment variables

There are few environment variables that can be used to confgure Duckflow behaviour (usage is optional):

- Load extensions from local disk (disable network load)
```bash
export DUCKDB_EXTENSION_DIR=/my/dir
```

- Disable local filesystem access
```bash
export DUCKDB_DISABLE_LOCALFS=1
```

- Lock DuckDB configuration and prevent changing it in pipeline
```bash
export DUCKDB_LOCK_CONFIG=1
```

In Pyodide environment these settings are not available.


### 1. Imports

To begin using Duckflow, you'll first need to import the necessary libraries:

```python
import duckdb
import yaml
import pandas as pd
from duckflow import Duct, DuctContext, ConnectionConfiguration
import logging
```

### 2. Create a Logger

It's always good practice to create a logger for your pipeline to track its execution. The logger will help capture events, errors, and other relevant information during the pipeline's execution.

```python
# Create a logger to log pipeline events
my_logger = logging.getLogger('mylogger')
my_logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
my_logger.addHandler(console_handler)
```

### 3. Load YAML from File

The configuration for your pipeline is often defined in a YAML file. This file will contain the stages of the pipeline, their configurations, and other necessary details.

```python
# Load pipeline configuration from YAML file
with open('pipeline_config.yaml', 'r') as file:
    pipeline_config = yaml.safe_load(file)

# Or via utility
from duckflow import Stages

pipeline_config = Stages.load('pipeline_config.yaml')
```

This assumes you have a `pipeline_config.yaml` file in the same directory. Here's an example of what the YAML file might look like:

```yaml
stages:
  - kind: tap
    name: source_data
    connection:
      kind: BigQuery
      project_id: "my_project"
      dataset: "my_dataset"
  - kind: transform
    name: process_data
    query: "SELECT * FROM source_data WHERE condition"
  - kind: sink
    name: output_data
    connection:
      kind: File
      file_path: "output/data.csv"
```

### 4. Create `ConnectionConfiguration` and `DuctContext`

The `ConnectionConfiguration` and `DuctContext` are essential for configuring the pipeline and providing context for variables and connections.

```python
# Create a ConnectionConfiguration with necessary details
conn_config = ConnectionConfiguration(
    kind='BigQuery', 
    project_id='my_project',
    dataset='my_dataset'
)

# Create a DuctContext, passing any relevant configuration parameters
duct_context = DuctContext(
    context_variable='some_value',  # Example variable
    connection=conn_config
)
```

### 5. Parse Loaded YAML to Stages

After loading the YAML file, you'll need to parse it into `Stage` objects that define each step of your pipeline. This includes connecting to data sources, transforming data, and writing the output to a target system.

```python
# Parse the loaded YAML configuration into stages
stages = []
for stage_config in pipeline_config['stages']:
    stage = {
        'kind': stage_config['kind'],
        'name': stage_config['name'],
        'connection': stage_config.get('connection', {}),
        'query': stage_config.get('query', '')
    }
    stages.append(stage)
```

### 6. Create the Pipeline (`Duct`)

Now that you have everything set up (logger, configuration, stages), you can create the `Duct` object and start running your pipeline.

```python
# Create the Duct instance with the pipeline stages, context, and configuration
duct = Duct(
    defs=stages, 
    context=duct_context, 
    default_connection=conn_config, 
    logger=my_logger
)

# Run the pipeline
duct.run()
```

### Wrapping Up

- **Duckflow** allows you to create flexible and modular data pipelines by defining stages for sourcing, transforming, and storing data.
- You can manage your pipeline configuration using YAML files and easily set up connections to different data sources (BigQuery, databases, files).
- The logger provides essential insight into your pipeline’s execution, helping you debug and track issues.
  
Now that you've set up your first pipeline, you can customize it further by adding more stages, adjusting configurations, and experimenting with different data sources.

---

### Additional Resources

- **Duckflow Documentation**: For more advanced usage and API references, check out the Duckflow documentation.
- **Community Support**: If you encounter any issues, join the Duckflow community for support and troubleshooting.
