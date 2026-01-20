import os
import sys
from pathlib import Path

from dagfactory import load_yaml_dags

# Add project root to Python path so modules can be imported
PROJECT_ROOT = Path(os.path.dirname(__file__)).parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Point to the jobs directory where job configuration YAMLs are stored
# New structure: configs/jobs/{system}/job.yaml
CONFIG_ROOT_DIR = PROJECT_ROOT.joinpath("configs").joinpath("jobs")

# For every YAML job config, generate DAG objects
# Each system has one job.yaml file that defines all tasks
load_yaml_dags(
    globals_dict=globals(),
    dags_folder=CONFIG_ROOT_DIR
)
