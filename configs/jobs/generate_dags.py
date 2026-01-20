"""
Generate Airflow DAGs from YAML job configurations using dag-factory.
This file is auto-loaded by Airflow from the dags_folder.
"""

import os
from pathlib import Path
from dagfactory import load_yaml_dags

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
os.sys.path.insert(0, str(PROJECT_ROOT))

# Generate DAGs from all job YAMLs
SYSTEMS = ['system_a', 'system_b']

for system in SYSTEMS:
    job_file = PROJECT_ROOT / "configs" / "jobs" / system / "job.yaml"
    if job_file.exists():
        load_yaml_dags(
            globals_dict=globals(),
            config_filepath=str(job_file)
        )

__all__ = ['system_a_etl', 'system_b_etl']
