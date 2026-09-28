"""Ingestion des données industrielles dans la couche Bronze."""

import logging
from pathlib import Path

import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_PATH = PROJECT_ROOT / "data" / "raw"
BRONZE_PATH = PROJECT_ROOT / "data" / "lake" / "bronze"


def ingest_csv_to_bronze(
    source_path: Path,
    target_path: Path,
) -> None:
    """Ingère un fichier CSV source dans la couche Bronze au format Parquet."""

    logger.info(
        "Ingestion Bronze : %s",
        source_path.name,
    )

    df = pd.read_csv(source_path)

    target_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_parquet(
        target_path,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    logger.info(
        "Données Bronze enregistrées : %s",
        target_path,
    )


def main() -> None:
    """Alimente la couche Bronze du Data Lake."""

    ingest_csv_to_bronze(
        RAW_PATH / "measurements.csv",
        BRONZE_PATH / "measurements.parquet",
    )

    ingest_csv_to_bronze(
        RAW_PATH / "equipment.csv",
        BRONZE_PATH / "equipment.parquet",
    )

    logger.info("Ingestion Bronze terminée.")


if __name__ == "__main__":
    main()