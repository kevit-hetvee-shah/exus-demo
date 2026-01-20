# Declarative Function-Based ETL Layer - Documentation

## Overview

This document describes the **declarative function-based ETL layer** - a configuration-driven abstraction that hides Airflow implementation details from business users.

### What Problem Does This Solve?

**Before** (Current): Business users need to understand:
- Airflow operators (`PythonOperator`, `FileSensor`)
- DAG structure and task dependencies
- Python callables and file paths
- Airflow-specific parameters

**After** (With Function Layer): Business users only define:
- **What** to do (wait for file, read data, copy data)
- **Where** data comes from and goes to
- **How** to process data (format, filters, transformations)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    BUSINESS USER INTERFACE                       │
│  (Declarative YAML Configs - No Airflow Knowledge Required)     │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                       OUTER LAYER (YAML)                         │
│  • Function Definitions (wait, read, transform, copy, delete)   │
│  • Workflow Definitions (chain functions together)              │
│  • Storage Profiles (local, S3, SFTP, shared)                   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                       INNER LAYER (Python)                       │
│  • StorageBackend (abstract file operations)                    │
│  • FunctionExecutor (executes function configs)                 │
│  • DuckDBManager (Parquet processing)                           │
│  • DAGBuilder / K8s Job Generator (generates workloads)          │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                 EXECUTION (Airflow + Kubernetes)                 │
│  Airflow orchestrates K8s pods, each pod runs DuckDB           │
└─────────────────────────────────────────────────────────────────┘
```

---

## Function Types

### 1. wait_for_source_file

**Purpose**: Monitor for new file arrival (local, S3, SFTP)

**Business Use Case**: "Wait for a vendor to drop a CSV file in the incoming folder"

**Config Parameters**:
| Parameter | Description | Example |
|-----------|-------------|---------|
| `storage.profile` | Storage profile reference | `local_system_a` |
| `storage.local.directory` | Directory to monitor | `/data/incoming` |
| `storage.local.filepath_pattern` | File pattern to match | `source_file_*.csv` |
| `monitoring.poke_interval` | Check interval (seconds) | `60` |
| `monitoring.timeout` | Max wait time (seconds) | `3600` |

**Inner Layer Implementation**:
- Uses `StorageBackend` interface to check for files (supports local, S3, SFTP)
- Polls directory at specified interval until file found or timeout
- Returns file metadata (path, size, modified time) to downstream functions
- Airflow: Uses `FileSensor` under the hood

---

### 2. read_data

**Purpose**: Read delimited files with parsing options and row filters

**Business Use Case**: "Read a CSV file with specific format, filter rows, output to Parquet"

**Config Parameters**:
| Parameter | Description | Example |
|-----------|-------------|---------|
| `source.format.delimiter` | Field delimiter | `,`, `\|`, `;`, `\t` |
| `source.format.encoding` | File encoding (with/without BOM) | `utf-8`, `utf-16`, `ansi` |
| `source.format.has_header` | First row is header | `true` |
| `source.format.skip_rows` | Skip N rows | `0` |
| `source.format.comment_char` | Comment character | `#` |
| `source.format.line_ending` | Line ending type | `unix`, `windows`, `mac` |
| `source.format.quote_char` | Enclosing character | `"` |
| `source.filename_pattern.regex` | Date pattern in filename | `file_(\d{8})\.csv` |
| `source.error_handling.bad_records_threshold` | Max bad records | `100` |
| `source.error_handling.empty_file_behavior` | Empty file action | `error`, `continue` |
| `row_filters[]` | Row-level filters | See below |
| `output.format` | Output format | `parquet` |
| `output.paths[]` | Output file paths | `/data/staging/raw/products.parquet` |

**Row Filters**:
| Operator | Description | Example |
|----------|-------------|---------|
| `not_null` | Column not empty | `operator: not_null` |
| `equals` | Exact match | `operator: equals, value: "active"` |
| `not_equals` | Not equal | `operator: not_equals, value: 0` |
| `greater_than` | Greater than | `operator: greater_than, value: 100` |
| `less_than` | Less than | `operator: less_than, value: 1000` |
| `in` | In list | `operator: in, value: ["A", "B"]` |

**Inner Layer Implementation**:
- Uses DuckDB to read files with specified format options
- Applies row filters only (NO column transformations)
- Writes filtered data to Parquet
- Returns Parquet file path and row count
- Airflow: Generates K8s pod that runs DuckDB read + filters

**What it DOESN'T do**: Column transformations (use `transform_data` function)

---

### 3. transform_data

**Purpose**: Apply column transformations to Parquet data

**Business Use Case**: "Read products from Parquet, cast types, trim strings, apply business logic"

**Config Parameters**:
| Parameter | Description | Example |
|-----------|-------------|---------|
| `source.type` | Source type | `parquet` |
| `source.input_from` | Previous function output | `read_products` |
| `transformations[]` | Column transformations | See below |
| `filters[]` | SQL filters (optional) | `"ProductId > 0"` |
| `output.paths[]` | Output Parquet paths | `/staging/transformed/*.parquet` |

**Transformation Types**:
| Type | Description | Example |
|------|-------------|---------|
| `trim` | Remove whitespace | `type: trim` |
| `cast` | Convert data type | `type: cast, target_type: integer` |
| `upper_case` | Convert to uppercase | `type: upper_case` |
| `lower_case` | Convert to lowercase | `type: lower_case` |
| `substring` | Extract substring | `type: substring, start: 0, length: 10` |
| `regex` | Apply regex | `type: regex, pattern: "[0-9]+"` |
| `remove_non_printable` | Remove non-printable chars | `type: remove_non_printable` |
| `default_value` | Replace nulls | `type: default_value, value: "Unknown"` |
| `sql` | Custom SQL expression | `sql: "CAST({{value}} AS DATE)"` |

**Inner Layer Implementation**:
- Uses DuckDB to read Parquet from previous step
- Applies transformations via SQL or transformation functions
- Writes transformed data back to Parquet
- Returns output Parquet path and row count
- Airflow: Generates K8s pod that runs DuckDB transformations

---

### 4. copy_data

**Purpose**: Copy data from Parquet to destination database tables

**Business Use Case**: "Load products from staging Parquet into Product table"

**Config Parameters**:
| Parameter | Description | Example |
|-----------|-------------|---------|
| `storage.profile` | Storage profile reference | `local_system_a` |
| `storage.local.directory` | Directory to monitor | `/data/incoming` |
| `storage.local.filepath_pattern` | File pattern to match | `source_file_*.csv` |
| `monitoring.poke_interval` | Check interval (seconds) | `60` |
| `monitoring.timeout` | Max wait time (seconds) | `3600` |

**Inner Layer Implementation**:
- Uses `StorageBackend` interface to check for files
- Supports `LocalStorage`, `S3Storage`, `SFTPStorage`
- Returns file metadata (path, size, modified time) to downstream functions
- Airflow: Uses `FileSensor` under the hood

---

### 2. read_data

**Purpose**: Read delimited files with advanced parsing options

**Business Use Case**: "Read a CSV file with specific format, apply filters, output to Parquet"

**Config Parameters**:
| Parameter | Description | Example |
|-----------|-------------|---------|
| `source.format.delimiter` | Field delimiter | `,`, `\|`, `;`, `\t` |
| `source.format.encoding` | File encoding | `utf-8`, `utf-16`, `ansi` |
| `source.format.has_header` | First row is header | `true` |
| `source.format.skip_rows` | Skip N rows | `0` |
| `source.format.comment_char` | Comment character | `#` |
| `source.format.line_ending` | Line ending type | `unix`, `windows`, `mac` |
| `source.format.quote_char` | Enclosing character | `"` |
| `source.filename_pattern.regex` | Date pattern in filename | `file_(\d{8})\.csv` |
| `source.error_handling.bad_records_threshold` | Max bad records | `100` |
| `source.error_handling.empty_file_behavior` | Empty file action | `error`, `continue` |
| `columns[]` | Column definitions with transformations | See below |
| `row_filters[]` | Row-level filters | See below |
| `output.format` | Output format | `parquet` |
| `output.paths[]` | Output file paths | `/data/staging/products.parquet` |

**Column Transformations**:
| Transformation | Description | Example |
|----------------|-------------|---------|
| `trim` | Remove whitespace | `type: trim` |
| `cast` | Convert data type | `type: cast, target_type: integer` |
| `upper_case` | Convert to uppercase | `type: upper_case` |
| `lower_case` | Convert to lowercase | `type: lower_case` |
| `substring` | Extract substring | `type: substring, start: 0, length: 10` |
| `regex` | Apply regex | `type: regex, pattern: "[0-9]+"` |
| `default_value` | Replace nulls | `type: default_value, value: "Unknown"` |

**Row Filters**:
| Operator | Description | Example |
|----------|-------------|---------|
| `not_null` | Column not empty | `operator: not_null` |
| `equals` | Exact match | `operator: equals, value: "active"` |
| `not_equals` | Not equal | `operator: not_equals, value: 0` |
| `greater_than` | Greater than | `operator: greater_than, value: 100` |
| `less_than` | Less than | `operator: less_than, value: 1000` |
| `in` | In list | `operator: in, value: ["A", "B"]` |

**Inner Layer Implementation**:
- Uses pandas to read files with specified format options
- Applies transformations via `TransformationEngine`
- Applies row filters
- Uses `DuckDBManager` to write output to Parquet
- Returns Parquet file path and row count to downstream functions
- Airflow: Uses `PythonOperator` that calls `ReadDataFunction.execute()`

---

### 3. copy_data

**Purpose**: Copy data from Parquet to database tables

**Business Use Case**: "Load products from staging Parquet into Product table"

**Config Parameters**:
| Parameter | Description | Example |
|-----------|-------------|---------|
| `source.type` | Source type | `parquet` |
| `source.input_from` | Previous function output | `read_products` |
| `source.paths[]` | Direct Parquet paths (alternative) | `/staging/products.parquet` |
| `source.duckdb.query` | Optional SQL transformation | `SELECT * FROM ...` |
| `destination.table` | Target table | `Product` |
| `destination.database` | Target database | `systemA.db` |
| `copy_mode` | Copy mode | `insert_update`, `insert_only` |
| `column_mappings[]` | Source to dest column mapping | See below |
| `record_join.source_columns` | Join columns for deduplication | `["ProductId"]` |
| `batch.batch_size` | Records per batch | `1000` |

**Column Mappings**:
| Parameter | Description | Example |
|-----------|-------------|---------|
| `source` | Source column name | `ProductId` |
| `destination` | Destination column name | `ProductId` |
| `empty_source_behavior` | What to do if source is empty | `update`, `keep`, `skip` |

**Copy Modes**:
| Mode | Description |
|------|-------------|
| `insert_update` | Check if record exists: update if yes, insert if no |
| `insert_only` | Insert only, skip if record exists |
| `update_only` | Update existing records only |

**Inner Layer Implementation**:
- Uses `DuckDBManager` to read Parquet files
- Applies SQL query if specified
- Maps columns according to `column_mappings`
- Connects to destination database via existing `database.client_db`
- Performs insert/update operations based on `copy_mode`
- Uses batching for large datasets
- Airflow: Uses `PythonOperator` that calls `CopyDataFunction.execute()`

---

### 5. delete_data

**Purpose**: Delete data from tables based on criteria

**Business Use Case**: "Delete records older than 1 year from Product table"

**Config Parameters**:
| Parameter | Description | Example |
|-----------|-------------|---------|
| `target.table` | Target table | `Product` |
| `target.database` | Target database | `systemA.db` |
| `delete_criteria.conditions[]` | Delete conditions | See below |
| `delete_criteria.custom_sql` | Custom WHERE clause | `WHERE date < '2025-01-01'` |
| `delete_mode` | Delete type | `hard`, `soft` |
| `safety.max_delete_count` | Safety limit | `10000` |
| `safety.dry_run` | Preview only | `true`, `false` |
| `backup.enabled` | Backup before delete | `true`, `false` |
| `backup.storage.path` | Backup path | `/data/backups` |

**Delete Conditions**:
| Parameter | Description | Example |
|-----------|-------------|---------|
| `column` | Column name | `CreatedDate` |
| `operator` | Comparison operator | `less_than`, `equals`, `in` |
| `value` | Comparison value | `"2025-01-01"`, `100` |

**Delete Modes**:
| Mode | Description |
|------|-------------|
| `hard` | Actually remove rows from table |
| `soft` | Set `deleted_at` timestamp (rows remain) |

**Inner Layer Implementation**:
- Builds WHERE clause from conditions or uses custom SQL
- Counts affected records before deleting
- Enforces `max_delete_count` safety limit
- Optionally backs up data to Parquet before delete
- Supports soft delete by setting `deleted_at` column
- Airflow: Uses `PythonOperator` that calls `DeleteDataFunction.execute()`

---

## Storage Profiles

Storage profiles abstract the details of different storage backends. Once defined, functions reference them by name.

### Local Storage Profile

```yaml
# configs_v2/storage/local/system_a_incoming.yaml
storage:
  type: local
  name: local_system_a
  version: "1.0"

configuration:
  base_path: "/data/system_A"
  directories:
    incoming: "incoming"
    staging: "staging"
    processed: "processed"
```

### S3 Storage Profile

```yaml
# configs_v2/storage/s3/production_bucket.yaml
storage:
  type: s3
  name: s3_production
  version: "1.0"

configuration:
  bucket: "my-etl-bucket"
  region: "us-east-1"
  prefix: "etl/"
  credentials:
    access_key_id: "{{ env.AWS_ACCESS_KEY_ID }}"
    secret_access_key: "{{ env.AWS_SECRET_ACCESS_KEY }}"
```

### SFTP Storage Profile

```yaml
# configs_v2/storage/sftp/vendor_sftp.yaml
storage:
  type: sftp
  name: vendor_sftp
  version: "1.0"

configuration:
  host: "sftp.vendor.com"
  port: 22
  username: "etl_user"
  auth:
    type: "key"
    private_key_path: "/home/airflow/.ssh/id_rsa"
```

---

## Kubernetes Architecture

### Pod-per-Function Model

Each function runs in a separate Kubernetes pod. Data is passed between pods via shared storage.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AIRFLOW ORCHESTRATOR                        │
│  (DAG scheduler that spawns K8s pods based on workflow config)    │
└─────────────────────────────────────────────────────────────────────┘
                                │
         ┌──────────────────────┼──────────────────────┐
         │                      │                      │
         ▼                      ▼                      ▼
┌────────────────┐     ┌────────────────┐     ┌────────────────┐
│     POD 1      │     │     POD 2      │     │     POD 3      │
│                │     │                │     │                │
│ wait_source   │     │ transform_     │     │ copy_          │
│ + read_products│     │ products       │     │ products       │
│                │     │                │     │                │
│ DuckDB:        │     │ DuckDB:        │     │ DuckDB:        │
│ • Read CSV     │     │ • Read Parquet │     │ • Read Parquet │
│ • Filter rows  │     │ • Transform    │     │ • Insert/Update│
│ • Write Parquet│     │ • Write Parquet│     │   to DB        │
└────────────────┘     └────────────────┘     └────────────────┘
         │                      │                      │
         └──────────────────────┼──────────────────────┘
                                ▼
                    ┌─────────────────────┐
                    │  SHARED STORAGE    │
                    │  (PVC / S3 / NFS)   │
                    │                     │
                    │ /shared/staging/    │
                    │ ├── raw/            │
                    │ ├── transformed/    │
                    │ └── archive/        │
                    └─────────────────────┘
```

### Data Flow Between Pods

```
STEP 1: POD 1 (wait_source + read_products)
┌───────────────────────────────────────────────────────────────┐
│ Input:  source_file_A.csv (from incoming storage)            │
│ Process: DuckDB reads CSV + applies row filters              │
│ Output: /shared/staging/raw/products_raw.parquet             │
└───────────────────────────────────────────────────────────────┘
                          │
                          ▼ (Parquet file in shared storage)

STEP 2: POD 2 (transform_products)
┌───────────────────────────────────────────────────────────────┐
│ Input:  /shared/staging/raw/products_raw.parquet             │
│ Process: DuckDB reads Parquet + applies transformations        │
│ Output: /shared/staging/transformed/products_transformed.parquet│
└───────────────────────────────────────────────────────────────┘
                          │
                          ▼ (Parquet file in shared storage)

STEP 3: POD 3 (copy_products)
┌───────────────────────────────────────────────────────────────┐
│ Input:  /shared/staging/transformed/products_transformed.parquet│
│ Process: DuckDB reads Parquet + loads into database           │
│ Output: Product table in systemA.db                           │
└───────────────────────────────────────────────────────────────┘
```

### Shared Storage Options

| Storage Type | K8s Implementation | Path Format | Use Case |
|---------------|-------------------|-------------|----------|
| **PVC** | PersistentVolumeClaim mounted in pods | `/shared/staging/` | Ephemeral staging data |
| **S3** | S3 accessor (env vars or IRSA) | `s3://bucket/path/` | Durable, cross-region |
| **NFS** | NFS volume mounted in pods | `/shared/staging/` | Shared file system |

### Pod Configuration

Each pod receives:
- **Function config** from `configs_v2/functions/`
- **Storage profile** for accessing shared storage
- **DuckDB** for processing
- **Resource limits** (CPU, memory)

Pods run sequentially based on workflow dependencies:
1. Airflow spawns POD 1
2. POD 1 completes, writes to shared storage
3. Airflow spawns POD 2
4. POD 2 reads from shared storage, completes
5. Airflow spawns POD 3
6. POD 3 reads from shared storage, completes

---

## Workflow Definitions

Workflows chain functions together into complete ETL pipelines.

### Workflow Structure

```yaml
# configs_v2/functions/workflows/system_a_etl.yaml
workflow:
  name: system_a_etl
  schedule: "*/2 * * * *"  # Every 2 minutes
  description: "System A ETL workflow"

functions:
  - id: wait_source
    function_ref: wait_for_source_file_a
    enabled: true

  - id: read_products
    function_ref: read_products_a
    enabled: true
    depends_on: [wait_source]

  - id: copy_products
    function_ref: copy_products_a
    enabled: true
    depends_on: [read_products]
```

### Workflow Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `workflow.name` | Workflow/DAG name | `system_a_etl` |
| `workflow.schedule` | Cron schedule | `*/2 * * * *` |
| `workflow.description` | DAG description | `"System A ETL"` |
| `functions[].id` | Task ID | `wait_source` |
| `functions[].function_ref` | Reference to function definition | `wait_for_source_file_a` |
| `functions[].enabled` | Enable/disable task | `true`, `false` |
| `functions[].depends_on` | List of task dependencies | `[wait_source]` |

---

## Data Flow Example

```
1. wait_for_source_file
   ├─> POD 1: Monitors /data/incoming/ for source_file_A.csv
   └─> Outputs: {filepath: "/data/incoming/source_file_A.csv"}

2. read_data (depends_on: wait_for_source_file)
   ├─> POD 1: Reads source_file_A.csv with DuckDB
   ├─> POD 1: Applies row filters (NO transformations)
   ├─> POD 1: Writes to /shared/staging/raw/products_raw.parquet
   └─> Outputs: {parquet_path: "/shared/staging/raw/products_raw.parquet", row_count: 1000}

3. transform_data (depends_on: read_data)
   ├─> POD 2: Reads /shared/staging/raw/products_raw.parquet
   ├─> POD 2: Applies column transformations with DuckDB
   ├─> POD 2: Writes to /shared/staging/transformed/products_transformed.parquet
   └─> Outputs: {parquet_path: "/shared/staging/transformed/products_transformed.parquet", row_count: 1000}

4. copy_data (depends_on: transform_data)
   ├─> POD 3: Reads /shared/staging/transformed/products_transformed.parquet
   ├─> POD 3: Loads into Product table in database
   └─> Outputs: {records_inserted: 100, records_updated: 50}
```

---

## Inner Layer: What Happens Under the Hood

### When a Function Executes

1. **Config Loading**: `ConfigLoader` reads the function YAML from `configs_v2/`
2. **Storage Creation**: `StorageFactory` creates storage backend from profile
3. **Function Execution**: Function class executes with config and storage
4. **Output Passing**: Results passed via Airflow XCom to next function
5. **DAG Generation**: All tasks combined into Airflow DAG by `FunctionDAGBuilder`

### Key Inner Layer Classes

| Class | File | Responsibility |
|-------|------|-----------------|
| `StorageBackend` | `etl_v2/storage/base.py` | Abstract interface for file operations |
| `LocalStorage` | `etl_v2/storage/local.py` | Local filesystem implementation |
| `S3Storage` | `etl_v2/storage/s3.py` | AWS S3 implementation |
| `SFTPStorage` | `etl_v2/storage/sftp.py` | SFTP implementation |
| `StorageFactory` | `etl_v2/storage/factory.py` | Creates storage backends |
| `DuckDBManager` | `etl_v2/duckdb/manager.py` | Parquet read/write operations |
| `BaseFunction` | `etl_v2/functions/base.py` | Abstract function class |
| `WaitForSourceFileFunction` | `etl_v2/functions/wait_for_source_file.py` | Wait implementation |
| `ReadDataFunction` | `etl_v2/functions/read_data.py` | Read implementation |
| `CopyDataFunction` | `etl_v2/functions/copy_data.py` | Copy implementation |
| `DeleteDataFunction` | `etl_v2/functions/delete_data.py` | Delete implementation |
| `FunctionExecutor` | `etl_v2/functions/executor.py` | Executes functions |
| `FunctionDAGBuilder` | `etl_v2/dag_generator/function_dag_builder.py` | Builds Airflow DAGs |

---

## Quick Reference: Function vs Airflow

| What You Want | Function Layer (Declarative) | Airflow (Imperative) |
|---------------|------------------------------|---------------------|
| Wait for file | `function: wait_for_source_file` | `operator: FileSensor(filepath=...)` |
| Read CSV | `function: read_data` with format params | Write Python code with pandas |
| Copy to table | `function: copy_data` with mappings | Write SQL or use SQLAlchemy |
| Delete records | `function: delete_data` with criteria | Write DELETE SQL |
| Chain tasks | `depends_on: [previous_task]` | `task1 >> task2` |

---

## Benefits

1. **No Python Required**: Business users define ETL in YAML only
2. **Source Agnostic**: Switch between local, S3, SFTP by changing profile name
3. **Reusable Functions**: Define once, reference in multiple workflows
4. **Validated Configs**: Schema validation catches errors before execution
5. **Self-Documenting**: YAML configs are readable and explain the pipeline
6. **Version Control Friendly**: YAML changes are clear in git diffs
