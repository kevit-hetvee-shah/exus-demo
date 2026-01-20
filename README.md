# Configuration-Driven ETL Tool

A production-grade, configuration-driven ETL (Extract, Transform, Load) platform for automating data ingestion, transformation, and loading from text files into normalized database schemas. Built with Python, Apache Airflow, and YAML-based configuration.

## Overview

This ETL tool implements the Technical Approach from the Kevit Proposal (pages 17-19), providing:

- **Configuration-Driven Architecture**: All ETL logic defined through YAML files - no code customization required per system
- **Modular Configuration Structure**: Separate YAML files for sources, aggregations, copy data operations, and job orchestration
- **Flexible Transformations**: Support for type casting, string operations, date functions, and more
- **Aggregation Support**: Group-by operations with aggregate functions (MIN, MAX, SUM, AVG, COUNT, FIRST, LAST)
- **Database Integration**: Support for insert/update (upsert) operations with configurable join keys
- **Airflow Orchestration**: GitOps-style DAG generation from YAML configurations using dag-factory

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          YAML Configuration Layer                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │   Source     │  │ Aggregation  │  │  CopyData    │  │    Jobs      │   │
│  │   Files      │  │ Datasets     │  │  Mappings    │  │ Orchestration│   │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       ETL Processing Engine (Python)                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ Config       │  │ Transformation│  │  Aggregation │  │ Database     │   │
│  │ Loader       │  │   Engine     │  │  Functions   │  │ Operations   │   │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Airflow DAG Generation                             │
│  ┌──────────────┐  ┌──────────────┐                                        │
│  │ DAG Factory  │  │  Kubernetes  │                                        │
│  │ (dag-factory)│  │   Executor   │                                        │
│  └──────────────┘  └──────────────┘                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Target Database Systems                             │
│  ┌──────────────┐  ┌──────────────┐                                        │
│  │  System A    │  │  System B    │   (Add more systems as needed)         │
│  │  Database    │  │  Database    │                                        │
│  └──────────────┘  └──────────────┘                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
test-airflow-dag-factoory/
├── configs/
│   ├── schema/
│   │   └── database_schema.yaml      # Database table definitions
│   ├── sources/
│   │   ├── system_A_source.yaml      # System A source file config
│   │   └── system_B_source.yaml      # System B source file config
│   ├── aggregations/
│   │   ├── system_A_aggregations.yaml # System A aggregation rules
│   │   └── system_B_aggregations.yaml # System B aggregation rules
│   ├── copydata/
│   │   ├── system_A_copydata.yaml    # System A data copy mappings
│   │   └── system_B_copydata.yaml    # System B data copy mappings
│   └── jobs/
│       ├── system_A_job.yaml         # System A job orchestration
│       └── system_B_job.yaml         # System B job orchestration
├── data/
│   ├── docs/                         # RFP and proposal documents
│   ├── sample_files/                 # Sample configuration files
│   ├── system_A/
│   │   └── incoming/
│   │       └── source_file_A.csv     # System A source data
│   └── system_B/
│       └── incoming/
│           └── source_file_B.csv     # System B source data
├── etl/
│   ├── __init__.py
│   ├── config_loader.py              # YAML configuration loader
│   ├── transformations.py            # Data transformation engine
│   └── workflows.py                  # ETL workflow execution functions
├── generators/
│   ├── __init__.py
│   └── dag_loader.py                 # Airflow DAG generator
├── database.py                        # Database initialization
├── requirements.txt                   # Python dependencies
├── README.md                          # This file
└── DOCUMENTATION.md                   # Detailed technical documentation
```

## Quick Start

### Prerequisites

- Python 3.8 or higher
- pip (Python package installer)
- SQLite (comes with Python) or other supported database

### Installation

1. **Clone or navigate to the project directory:**
   ```bash
   cd /path/to/test-airflow-dag-factoory
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Initialize the database:**
   ```bash
   python database.py
   ```
   This creates `systemA.db` and `systemB.db` with the required tables.

### Running ETL Tasks

#### Option 1: Direct Python Execution (Testing)

Run ETL tasks directly without Airflow for testing:

```bash
python -c "
from etl.workflows import execute_copy_data_task

# Run System A CopyData tasks
execute_copy_data_task(system='A', source='SourceFileA', destination_table='Product')
execute_copy_data_task(system='A', source='Customer_Aggregated', destination_table='Customer')
execute_copy_data_task(system='A', source='Address_Aggregated', destination_table='Address')
execute_copy_data_task(system='A', source='Phone_Aggregated', destination_table='Phone')

print('System A ETL completed!')
"
```

#### Option 2: With Apache Airflow (Production)

1. **Install and configure Apache Airflow:**
   ```bash
   pip install apache-airflow
   pip install dag-factory
   export AIRFLOW_HOME=~/airflow
   airflow db init
   ```

2. **Set up DAGs directory:**
   ```bash
   mkdir -p $AIRFLOW_HOME/dags
   ln -s $(pwd)/generators $AIRFLOW_HOME/dags/generators
   ln -s $(pwd)/configs $AIRFLOW_HOME/dags/configs
   ln -s $(pwd)/etl $AIRFLOW_HOME/dags/etl
   ln -s $(pwd)/database.py $AIRFLOW_HOME/dags/database.py
   ```

3. **Start Airflow scheduler and webserver:**
   ```bash
   airflow scheduler &
   airflow webserver &
   ```

4. **Access Airflow UI:**
   Open http://localhost:8080 in your browser

5. **Trigger DAGs manually or wait for scheduled execution:**
   - System A DAG: Runs daily at 1:00 AM
   - System B DAG: Runs daily at 3:00 AM

## Configuration Guide

### Adding a New System

To add a new system (e.g., System C):

1. **Create source file configuration:**
   ```yaml
   # configs/sources/system_C_source.yaml
   source_files:
     SourceFileC:
       description: "Source file from SystemC"
       file_config:
         path: "/path/to/source_file_C.csv"
         format: delimited
         delimiter: ","
         # ... more file config
       columns:
         - name: Column1
           data_type: string
           transformations:
             - type: trim
   ```

2. **Create aggregation configuration:**
   ```yaml
   # configs/aggregations/system_C_aggregations.yaml
   aggregated_datasets:
     Customer_Aggregated:
       source: SourceFileC
       group_by: [CustomerId]
       aggregated_columns:
         - name: CustomerName
           aggregation: first
   ```

3. **Create CopyData configuration:**
   ```yaml
   # configs/copydata/system_C_copydata.yaml
   copy_data_tasks:
     CopyData1_Products:
       source: SourceFileC
       destination:
         table: Product
         database: systemC.db
       # ... more config
   ```

4. **Create job configuration:**
   ```yaml
   # configs/jobs/system_C_job.yaml
   system_C_etl_job:
     job_name: system_C_etl_job
     config_references:
       source_file: "sources/system_C_source.yaml"
       aggregations: "aggregations/system_C_aggregations.yaml"
       copy_data: "copydata/system_C_copydata.yaml"
       database_schema: "schema/database_schema.yaml"
     schedule_interval: "0 5 * * *"  # 5:00 AM daily
     # ... task definitions
   ```

5. **Restart Airflow scheduler** to pick up the new DAG.

### Supported Transformations

- `trim`: Remove whitespace from strings
- `upper_case`: Convert to uppercase
- `lower_case`: Convert to lowercase
- `cast`: Convert data types (integer, decimal, boolean, date)
- `substring`: Extract part of a string
- `regex`: Apply regex patterns
- `remove_non_printable`: Remove non-printable characters
- `default_value`: Replace null/empty with default

### Supported Aggregation Functions

- `first`: Take first occurrence
- `last`: Take last occurrence
- `min`: Minimum value
- `max`: Maximum value
- `count`: Count of values
- `sum`: Sum of values
- `avg`: Average of values

## Requirements

See `requirements.txt` for full list:

```
apache-airflow>=2.0.0
dag-factory>=0.19.0
pandas>=1.3.0
pyyaml>=5.4.0
```

## Documentation

For detailed technical documentation, architecture details, and implementation specifics, see [DOCUMENTATION.md](DOCUMENTATION.md).

## References

- [RFP_ETL_Tool_final 1.pdf](data/docs/RFP_ETL_Tool_final%201.pdf) - Original Request for Proposal
- [Kevit-Proposal_For_ETL_Tool_Implementation-v1.0-WP.pdf](data/docs/Kevit-Proposal_For_ETL_Tool_Implementation-v1.0-WP.pdf) - Technical Proposal (pages 17-19 for Technical Approach)

## License

All deliverables, including source code, configuration files, and documentation, remain the sole property of EXUS.
