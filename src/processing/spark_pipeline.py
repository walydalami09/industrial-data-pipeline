"""Enrichissement du rapport industriel avec PySpark."""
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SITE_REPORT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "site_report.parquet"
)

WEATHER_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "weather.parquet"
)

FINAL_REPORT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "final_industrial_report"
)


def create_spark_session() -> SparkSession:
    """Crée la session Spark utilisée par le pipeline."""

    return (
        SparkSession.builder
        .appName("IndustrialDataPipeline")
        .master("local[*]")
        .getOrCreate()
    )


def load_parquet(
    spark: SparkSession,
    path: Path,
) -> DataFrame:
    """Charge un fichier Parquet dans un DataFrame Spark."""

    return spark.read.parquet(str(path))


def enrich_with_weather(
    site_report: DataFrame,
    weather: DataFrame,
) -> DataFrame:
    """Ajoute les données météorologiques au rapport industriel."""

    return (
        site_report.join(
            weather,
            site_report["site"] == weather["city"],
            how="left",
        )
        .drop("city")
    )


def save_parquet(
    df: DataFrame,
    path: Path,
) -> None:
    """Enregistre un DataFrame Spark au format Parquet."""

    df.write.mode("overwrite").parquet(str(path))


def main() -> None:
    """Exécute l'enrichissement final avec Spark."""

    logger.info("Démarrage du pipeline Spark.")

    spark = create_spark_session()

    try:
        logger.info("Chargement du rapport industriel.")
        site_report = load_parquet(
            spark,
            SITE_REPORT_PATH,
        )

        logger.info("Chargement des données météo.")
        weather = load_parquet(
            spark,
            WEATHER_PATH,
        )

        logger.info("Jointure du rapport industriel avec la météo.")
        final_report = enrich_with_weather(
            site_report,
            weather,
        )

        final_report.show()

        logger.info("Export du rapport final.")
        save_parquet(
            final_report,
            FINAL_REPORT_PATH,
        )

        logger.info(
            "Rapport final exporté : %s",
            FINAL_REPORT_PATH,
        )
    except Exception:
        logger.exception("Échec du pipeline Spark.")
        raise
    finally:
        spark.stop()
        logger.info("Session Spark arrêtée.")

    logger.info("Pipeline Spark terminé.")


if __name__ == "__main__":
    main()