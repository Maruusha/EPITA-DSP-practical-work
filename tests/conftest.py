import sys
import os
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'model_service'))

for mod in [
    'airflow', 'airflow.sdk', 'airflow.exceptions',
    'airflow.providers', 'airflow.providers.postgres',
    'airflow.providers.postgres.hooks',
    'airflow.providers.postgres.hooks.postgres',
    'great_expectations', 'great_expectations.expectations',
    'predict', 'db_utility',
    'sqlalchemy', 'sqlalchemy.orm',
]:
    sys.modules[mod] = MagicMock()
