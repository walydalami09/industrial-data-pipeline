"""Chargement des données industrielles dans SQL Server."""

from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine

from database import DatabaseManager


# Configuration des chemins du projet
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "raw"

MEASUREMENTS_PATH = DATA_DIR / "measurements.csv"
EQUIPMENT_PATH = DATA_DIR / "equipment.csv"


def load_csv(file_path: Path) -> pd.DataFrame:
    """Charge un fichier CSV dans un DataFrame pandas."""

    return pd.read_csv(file_path)


def load_dataframe_to_sql(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
) -> None:
    """Charge un DataFrame dans une table SQL Server."""

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
    )


def main() -> None:
    """Charge les données industrielles dans SQL Server."""

    # Connexion à SQL Server
    database = DatabaseManager()

    # Lecture des fichiers sources
    df_measurements = load_csv(MEASUREMENTS_PATH)
    df_equipment = load_csv(EQUIPMENT_PATH)

    # Chargement dans SQL Server
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

    print("Chargement SQL Server terminé.")

    # Vérification
    with database.engine.connect() as connection:
        result = connection.exec_driver_sql(
            "SELECT COUNT(*) FROM measurements"
        )

        count = result.scalar_one()

    print(f"Nombre de mesures en base : {count}")


if __name__ == "__main__":
    main()