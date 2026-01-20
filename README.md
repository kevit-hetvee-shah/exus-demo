# Configuration-Driven ETL Tool

A production-grade, configuration-driven ETL platform for automating data ingestion, transformation, and loading from text files into normalized database schemas.

## Quick Start

### Prerequisites
- Python 3.8+
- pip

### Installation
```bash
pip install -r requirements.txt
python database.py  # Creates systemA.db and systemB.db
```

### Running ETL Tasks

**Direct Python (Testing):**
```python
from etl.workflows import execute_copy_data_task

# System A
execute_copy_data_task('a', 'source_file_a', 'source_file', 'Product', 'products')
execute_copy_data_task('a', 'customer', 'aggregated_dataset', 'Customer', 'customers')
execute_copy_data_task('a', 'address', 'aggregated_dataset', 'Address', 'addresses')
execute_copy_data_task('a', 'phone', 'aggregated_dataset', 'Phone', 'phones')
```

## Architecture

```
configs/
├── schema/tables/        # Individual table definitions
├── sources/{system}/     # Source file configs per system
├── aggregations/{system}/ # Aggregation configs per system
├── copydata/{system}/    # CopyData tasks per system
└── jobs/{system}/        # One job.yaml per system with all tasks
```

**Key Principle:** Each system has its own folder with all related configurations. Each aggregation/copydata task is in its own file.

## Configuration Structure

### Source File (`configs/sources/system_a/source_file_a.yaml`)
```yaml
source:
  name: source_file_a
  file_config:
    path: "/path/to/file.csv"
    delimiter: ","
  columns:
    - name: Column1
      transformations:
        - type: trim
```

### Aggregation (`configs/aggregations/system_a/customer.yaml`)
```yaml
aggregation:
  name: customer
  source:
    name: source_file_a
    type: source_file
  group_by: [CustomerId]
  aggregated_columns:
    - name: CustomerName
      function: first
```

### CopyData (`configs/copydata/system_a/customers.yaml`)
```yaml
task:
  name: customers
  source:
    name: customer
    type: aggregated_dataset
  destination:
    table: Customer
    database: systemA.db
  copy_mode: insert_update
  column_mappings:
    - source: CustomerId
      destination: CustomerId
```

### Job (`configs/jobs/system_a/job.yaml`)
```yaml
schedule_interval: "0 1 * * *"
tasks:
  wait_for_source_file:
    task_type: file_sensor
    # ...
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

## Adding a New System

1. Create `configs/sources/system_c/source_file_c.yaml`
2. Create `configs/aggregations/system_c/*.yaml`
3. Create `configs/copydata/system_c/*.yaml`
4. Create `configs/jobs/system_c/job.yaml`

## API Reference

```python
from etl.config_loader import get_config_loader

loader = get_config_loader()

# Get all systems
systems = loader.get_all_systems()  # ['system_a', 'system_b']

# Load configurations
source = loader.load_source_file('a', 'source_file_a')
agg = loader.load_aggregation('a', 'customer')
task = loader.load_copydata_task('a', 'customers')

# Validate
errors = loader.validate_system('a')
```

## Requirements
```
apache-airflow>=2.0.0
dag-factory>=0.19.0
pandas>=1.3.0
PyYAML>=5.4.0
```

## Documentation
See [DOCUMENTATION.md](DOCUMENTATION.md) for detailed technical documentation.
