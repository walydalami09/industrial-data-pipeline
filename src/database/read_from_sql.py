"""Lecture des données SQL Server et export au format Parquet."""

from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from database import DatabaseManager


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "site_report.parquet"
)


# ============================================================
# REQUÊTE SQL
# ============================================================

SITE_REPORT_QUERY = text("""
    SELECT
        site,
        COUNT(*) AS nb_measurements,
        AVG(temperature) AS average_temperature,
        MAX(temperature) AS max_temperature
    FROM measurements
    WHERE temperature IS NOT NULL
    GROUP BY site
""")


# ============================================================
# FONCTIONS
# ============================================================

def extract_site_report(engine: Engine) -> pd.DataFrame:
    """Extrait les statistiques par site depuis SQL Server."""

    with engine.connect() as connection:
        df = pd.read_sql(
            SITE_REPORT_QUERY,
            connection,
        )

    return df


def save_to_parquet(
    df: pd.DataFrame,
    output_path: Path,
) -> None:
    """Sauvegarde un DataFrame au format Parquet."""

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )


def read_parquet(file_path: Path) -> pd.DataFrame:
    """Lit un fichier Parquet."""

    return pd.read_parquet(file_path)


# ============================================================
# PIPELINE
# ============================================================

def main() -> None:
    """Extrait les données SQL et génère le rapport Parquet."""

    # Préparation de l'accès à SQL Server
    database = DatabaseManager()

    # Extraction SQL -> pandas
    df_site_report = extract_site_report(
        database.engine
    )

    print("Rapport extrait depuis SQL Server :")
    print(df_site_report)

    # Export pandas -> Parquet
    save_to_parquet(
        df_site_report,
        OUTPUT_PATH,
    )

    print(f"\nExport Parquet terminé : {OUTPUT_PATH}")

    # Vérification du fichier généré
    df_parquet = read_parquet(OUTPUT_PATH)

    print("\nDonnées relues depuis Parquet :")
    print(df_parquet)

    print(
        "\nDonnées identiques :",
        df_site_report.equals(df_parquet),
    )


if __name__ == "__main__":
    main()