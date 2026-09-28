from datetime import datetime, timezone
from pathlib import Path
import subprocess
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_DIR = Path("/home/walyd/Dev/python-data-project_1")
PROJECT_PYTHON = PROJECT_DIR / ".venv/bin/python"


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="industrial_data_pipeline",
    start_date=datetime(2026, 9, 27, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["data-engineering", "industrial"],
     default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
)
def industrial_pipeline():

    @task
    def load_to_sql() -> None:
        """Charge les données CSV dans SQL Server."""

        subprocess.run(
            [
                str(PROJECT_PYTHON),
                "src/database/load_to_sql.py",
            ],
            cwd=PROJECT_DIR,
            check=True,
        )

    @task
    def read_from_sql() -> None:
        """Extrait les données SQL Server vers Parquet."""

        subprocess.run(
            [
                str(PROJECT_PYTHON),
                "src/database/read_from_sql.py",
            ],
            cwd=PROJECT_DIR,
            check=True,
        )

    @task
    def ingest_weather() -> None:
        """Récupère les données météorologiques depuis l'API."""

        subprocess.run(
            [
                str(PROJECT_PYTHON),
                "src/ingestion/api_ingestion.py",
            ],
            cwd=PROJECT_DIR,
            check=True,
        )

    @task
    def process_industrial_data() -> None:
        """Exécute le traitement PySpark final."""

        subprocess.run(
            [
                str(PROJECT_PYTHON),
                "src/processing/spark_pipeline.py",
            ],
            cwd=PROJECT_DIR,
            check=True,
        )

    # Création des tâches
    sql_load = load_to_sql()
    sql_extract = read_from_sql()
    weather = ingest_weather()
    spark = process_industrial_data()

    # Dépendances
    sql_load >> sql_extract
    [sql_extract, weather] >> spark


industrial_pipeline()