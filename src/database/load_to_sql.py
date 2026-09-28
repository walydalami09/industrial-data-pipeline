"""Chargement idempotent des données industrielles dans SQL Server."""

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine

from database import DatabaseManager


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "raw"

MEASUREMENTS_PATH = DATA_DIR / "measurements.csv"
EQUIPMENT_PATH = DATA_DIR / "equipment.csv"


# ============================================================
# INGESTION
# ============================================================

def load_csv(file_path: Path) -> pd.DataFrame:
    """Charge un fichier CSV dans un DataFrame pandas."""

    if not file_path.exists():
        raise FileNotFoundError(
            f"Fichier source introuvable : {file_path}"
        )

    df = pd.read_csv(file_path)

    logger.info(
        "Fichier chargé : %s (%s lignes)",
        file_path.name,
        len(df),
    )

    return df


# ============================================================
# SQL LOAD
# ============================================================

def load_dataframe_to_sql(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
) -> None:
    """
    Charge un DataFrame dans SQL Server de manière idempotente.

    Le mode 'replace' garantit qu'une nouvelle exécution avec les
    mêmes données produit le même état final dans la table.
    """

    logger.info(
        "Chargement de %s lignes dans la table '%s'.",
        len(df),
        table_name,
    )

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
    )

    logger.info(
        "Table '%s' chargée avec succès.",
        table_name,
    )


# ============================================================
# VALIDATION
# ============================================================

def count_rows(
    engine: Engine,
    table_name: str,
) -> int:
    """Retourne le nombre de lignes d'une table."""

    with engine.connect() as connection:
        result = connection.exec_driver_sql(
            f"SELECT COUNT(*) FROM {table_name}"
        )

        return result.scalar_one()


# ============================================================
# PIPELINE
# ============================================================

def main() -> None:
    """Charge les données industrielles dans SQL Server."""

    logger.info("Démarrage du chargement SQL Server.")

    database = DatabaseManager()

    try:
        # ----------------------------------------------------
        # Lecture des sources
        # ----------------------------------------------------

        df_measurements = load_csv(
            MEASUREMENTS_PATH
        )

        df_equipment = load_csv(
            EQUIPMENT_PATH
        )

        # ----------------------------------------------------
        # Chargement SQL idempotent
        # ----------------------------------------------------

        load_dataframe_to_sql(
            df_measurements,
            "measurements",
            database.engine,
        )

        load_dataframe_to_sql(
            df_equipment,
            "equipment",
            database.engine,
        )

        # ----------------------------------------------------
        # Vérification
        # ----------------------------------------------------

        measurements_count = count_rows(
            database.engine,
            "measurements",
        )

        equipment_count = count_rows(
            database.engine,
            "equipment",
        )

        logger.info(
            "Nombre de mesures en base : %s",
            measurements_count,
        )

        logger.info(
            "Nombre d'équipements en base : %s",
            equipment_count,
        )

        logger.info(
            "Chargement SQL Server terminé avec succès."
        )

    except Exception:
        logger.exception(
            "Échec du chargement SQL Server."
        )
        raise


if __name__ == "__main__":
    main()