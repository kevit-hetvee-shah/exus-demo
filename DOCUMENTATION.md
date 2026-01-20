# ETL Tool - Technical Documentation

## System Overview

A configuration-driven ETL platform with a **scalable, multi-system architecture**:
- **32 YAML configuration files** organized per system
- **4 database tables**: Customer, Product, Address, Phone
- **2 systems**: System A (5 source rows), System B (4 source rows)

## Architecture

```
configs/
├── schema/
│   ├── _index.yaml
│   └── tables/
│       ├── customer.yaml
│       ├── product.yaml
│       ├── address.yaml
│       └── phone.yaml
│
├── sources/
│   ├── system_a/
│   │   ├── _index.yaml
│   │   └── source_file_a.yaml
│   └── system_b/
│       ├── _index.yaml
│       └── source_file_b.yaml
│
├── aggregations/
│   ├── system_a/
│   │   ├── _index.yaml
│   │   ├── customer.yaml
│   │   ├── address.yaml
│   │   └── phone.yaml
│   └── system_b/
│       └── (same structure)
│
├── copydata/
│   ├── system_a/
│   │   ├── _index.yaml
│   │   ├── products.yaml
│   │   ├── customers.yaml
│   │   ├── addresses.yaml
│   │   └── phones.yaml
│   └── system_b/
│       └── (same structure)
│
└── jobs/
    ├── system_a/job.yaml
    └── system_b/job.yaml
```

## Data Flow

```
1. Source File Arrives (CSV)
   ↓
2. File Sensor Detects
   ↓
3. Execute CopyData Tasks (parallel if no dependencies)
   ↓
   a. Load YAML Configuration
   ↓
   b. Read CSV → Apply Transformations → Apply Aggregations
   ↓
   c. Map to Destination Columns
   ↓
   d. Insert/Update to Database
```

## Configuration Reference

### Source File
```yaml
source:
  name: source_file_a
  file_config:
    path: "/path/to/file.csv"
    format: delimited
    delimiter: ","
    encoding: utf-8
    has_header: true
  columns:
    - name: ColumnName
      transformations:
      - type: trim
      - type: cast
        target_type: integer
  row_filters:
    - column: ColumnName
      operator: not_null
```

### Aggregation
```yaml
aggregation:
  name: customer
  source:
    name: source_file_a
    type: source_file
  group_by: [CustomerId]
  aggregated_columns:
    - name: CustomerName
      function: first  # first, last, min, max, count, sum, avg
```

### CopyData
```yaml
task:
  name: customers
  source:
    name: customer
    type: aggregated_dataset
  destination:
    table: Customer
    database: systemA.db
  record_join:
    source_columns: [CustomerId]
    destination_columns: [CustomerId]
  copy_mode: insert_update  # insert_update, insert_only
  column_mappings:
    - source: SourceColumn
      destination: DestColumn
      empty_source_behavior: update  # update, keep
```

### Job
```yaml
schedule_interval: "0 1 * * *"
default_args:
  owner: "data_engineering_team"
  start_date: "2026-01-19"
tasks:
  wait_for_source_file:
    task_type: file_sensor
    filepath_pattern: "source_file_A.csv"
    directory: "/path/to/incoming"
    poke_interval: 60
  copydata_products:
    task_type: copy_data
    python_callable: "etl.workflows.execute_copy_data_task"
    task_kwargs:
      system: "a"
      source: "source_file_a"
      source_type: "source_file"
      destination_table: "Product"
      copydata_task: "products"
    dependencies: ["wait_for_source_file"]
```

## API Reference

### ConfigLoader
```python
from etl.config_loader import get_config_loader

loader = get_config_loader()

# Systems
systems = loader.get_all_systems()

# Sources
sources = loader.get_all_sources('a')
source_config = loader.load_source_file('a', 'source_file_a')

# Aggregations
aggs = loader.get_all_aggregations('a')
agg_config = loader.load_aggregation('a', 'customer')

# CopyData
tasks = loader.get_all_copydata_tasks('a')
task_config = loader.load_copydata_task('a', 'products')

# Validation
errors = loader.validate_system('a')
```

### Workflow Execution
```python
from etl.workflows import execute_copy_data_task

# From source file
execute_copy_data_task(
    system='a',
    source='source_file_a',
    source_type='source_file',
    destination_table='Product',
    copydata_task='products'
)

# From aggregation
execute_copy_data_task(
    system='a',
    source='customer',
    source_type='aggregated_dataset',
    destination_table='Customer',
    copydata_task='customers'
)
```

## Transformations

- `trim`, `upper_case`, `lower_case`
- `cast` (integer, decimal, boolean, date)
- `substring`, `regex`, `remove_non_printable`
- `default_value`

## Aggregation Functions

- `first`, `last`, `min`, `max`, `count`, `sum`, `avg`
