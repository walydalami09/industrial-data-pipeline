from datetime import datetime, timezone
from pathlib import Path
import subprocess

from airflow.sdk import dag, task


# Dossier contenant notre projet industriel.
PROJECT_DIR = Path("/home/walyd/Dev/python-data-project_1")

# Python de notre environnement virtuel PySpark.
PROJECT_PYTHON = PROJECT_DIR / ".venv/bin/python"


@dag(
    dag_id="industrial_data_pipeline",
    start_date=datetime(2026, 9, 27, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["data-engineering", "industrial"],
)
def industrial_pipeline():

    @task
    def ingest_weather():
        """Récupérer les données météo depuis l'API."""

        subprocess.run(
            [str(PROJECT_PYTHON), "src/api_ingestion.py"],
            cwd=PROJECT_DIR,
            check=True,
        )

    @task
    def process_industrial_data():
        """Exécuter le pipeline PySpark."""

        subprocess.run(
            [str(PROJECT_PYTHON), "src/spark_pipeline.py"],
            cwd=PROJECT_DIR,
            check=True,
        )

    # La deuxième tâche attend la réussite de la première.
    ingest_weather() >> process_industrial_data()


industrial_pipeline()