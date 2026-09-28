"""Pipeline Lakehouse industriel : Bronze → Silver → Gold + Apache Iceberg."""

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    max as spark_max,
)


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

LAKE_PATH = PROJECT_ROOT / "data" / "lake"

BRONZE_PATH = LAKE_PATH / "bronze"
SILVER_PATH = LAKE_PATH / "silver"
GOLD_PATH = LAKE_PATH / "gold"

# Warehouse physique utilisé par Apache Iceberg
ICEBERG_WAREHOUSE_PATH = LAKE_PATH / "iceberg"

BRONZE_MEASUREMENTS_PATH = (
    BRONZE_PATH / "measurements.parquet"
)

BRONZE_EQUIPMENT_PATH = (
    BRONZE_PATH / "equipment.parquet"
)

BRONZE_WEATHER_PATH = (
    BRONZE_PATH / "weather.parquet"
)

SILVER_INDUSTRIAL_PATH = (
    SILVER_PATH / "industrial_data"
)

GOLD_SITE_REPORT_PATH = (
    GOLD_PATH / "site_report"
)


# ============================================================
# ICEBERG
# ============================================================

ICEBERG_VERSION = "1.11.0"

ICEBERG_PACKAGE = (
    "org.apache.iceberg:"
    "iceberg-spark-runtime-4.1_2.13:"
    f"{ICEBERG_VERSION}"
)

ICEBERG_CATALOG = "lakehouse"
ICEBERG_NAMESPACE = "gold"
ICEBERG_TABLE_NAME = "site_report"

ICEBERG_TABLE = (
    f"{ICEBERG_CATALOG}."
    f"{ICEBERG_NAMESPACE}."
    f"{ICEBERG_TABLE_NAME}"
)


# ============================================================
# SPARK
# ============================================================

def create_spark_session() -> SparkSession:
    """
    Crée une session Spark configurée pour Apache Iceberg.

    Le package Iceberg est récupéré depuis Maven au démarrage.

    Le catalogue "lakehouse" utilise un HadoopCatalog local
    dont le warehouse est stocké dans data/lake/iceberg.
    """

    logger.info(
        "Configuration Spark avec Apache Iceberg %s.",
        ICEBERG_VERSION,
    )

    logger.info(
        "Warehouse Iceberg : %s",
        ICEBERG_WAREHOUSE_PATH,
    )

    return (
        SparkSession.builder
        .appName("IndustrialLakehousePipeline")
        .master("local[*]")

        # Runtime Apache Iceberg compatible Spark 4.1 / Scala 2.13
        .config(
            "spark.jars.packages",
            ICEBERG_PACKAGE,
        )

        # Extensions SQL Iceberg
        .config(
            "spark.sql.extensions",
            (
                "org.apache.iceberg.spark.extensions."
                "IcebergSparkSessionExtensions"
            ),
        )

        # Catalogue Iceberg
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )

        # Catalogue local basé sur le filesystem
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.type",
            "hadoop",
        )

        # Emplacement physique des tables Iceberg
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse",
            str(ICEBERG_WAREHOUSE_PATH),
        )

        .getOrCreate()
    )


# ============================================================
# INGESTION
# ============================================================

def load_parquet(
    spark: SparkSession,
    path: Path,
) -> DataFrame:
    """Charge un dataset Parquet dans Spark."""

    return spark.read.parquet(
        str(path)
    )


# ============================================================
# SILVER
# ============================================================

def build_silver(
    measurements: DataFrame,
    equipment: DataFrame,
    weather: DataFrame,
) -> DataFrame:
    """
    Construit la couche Silver.

    Les mesures industrielles sont enrichies avec :
    - les informations de référence des équipements ;
    - les données météorologiques.

    Les petites tables equipment et weather sont broadcastées
    afin d'éviter des shuffles inutiles.
    """

    # --------------------------------------------------------
    # Measurements + Equipment
    # --------------------------------------------------------

    silver = (
        measurements.alias("m")
        .join(
            broadcast(
                equipment.alias("e")
            ),
            col("m.equipment_id")
            == col("e.equipment_id"),
            how="left",
        )
        .select(
            col("m.equipment_id"),
            col("m.site"),
            col("m.equipment_type"),
            col("m.temperature"),
            col("m.status"),
            col("e.manufacturer"),
            col("e.installation_year"),
        )
    )

    # --------------------------------------------------------
    # + Weather
    # --------------------------------------------------------

    silver = (
        silver.alias("s")
        .join(
            broadcast(
                weather.alias("w")
            ),
            col("s.site")
            == col("w.city"),
            how="left",
        )
        .select(
            col("s.equipment_id"),
            col("s.site"),
            col("s.equipment_type"),
            col("s.temperature"),
            col("s.status"),
            col("s.manufacturer"),
            col("s.installation_year"),
            col("w.measurement_time"),
            col("w.outside_temperature"),
        )
    )

    return silver


# ============================================================
# DATA QUALITY — SILVER
# ============================================================

def validate_silver(
    df: DataFrame,
) -> None:
    """Contrôle la qualité de la couche Silver."""

    logger.info(
        "Contrôle Data Quality Silver."
    )

    # --------------------------------------------------------
    # Dataset non vide
    # --------------------------------------------------------

    if df.isEmpty():
        raise ValueError(
            "Data Quality Silver : dataset vide."
        )

    # --------------------------------------------------------
    # equipment_id obligatoire
    # --------------------------------------------------------

    if (
        df.filter(
            col("equipment_id").isNull()
        )
        .limit(1)
        .count()
        > 0
    ):
        raise ValueError(
            "Data Quality Silver : equipment_id NULL."
        )

    # --------------------------------------------------------
    # site obligatoire
    # --------------------------------------------------------

    if (
        df.filter(
            col("site").isNull()
        )
        .limit(1)
        .count()
        > 0
    ):
        raise ValueError(
            "Data Quality Silver : site NULL."
        )

    # --------------------------------------------------------
    # température industrielle plausible
    # --------------------------------------------------------

    if (
        df.filter(
            col("temperature").isNotNull()
            & (
                (col("temperature") < -50)
                | (col("temperature") > 150)
            )
        )
        .limit(1)
        .count()
        > 0
    ):
        raise ValueError(
            "Data Quality Silver : température incohérente."
        )

    logger.info(
        "Data Quality Silver validée."
    )


# ============================================================
# GOLD
# ============================================================

def build_gold(
    silver: DataFrame,
) -> DataFrame:
    """
    Construit la couche Gold.

    Les mesures sont agrégées par site afin de produire
    un dataset analytique exploitable par la BI ou la
    Data Science.
    """

    return (
        silver
        .filter(
            col("temperature").isNotNull()
        )
        .groupBy("site")
        .agg(
            count("*").alias(
                "nb_measurements"
            ),
            avg("temperature").alias(
                "average_temperature"
            ),
            spark_max("temperature").alias(
                "max_temperature"
            ),
            avg("outside_temperature").alias(
                "average_outside_temperature"
            ),
        )
    )


# ============================================================
# DATA QUALITY — GOLD
# ============================================================

def validate_gold(
    df: DataFrame,
) -> None:
    """Contrôle la qualité de la couche Gold."""

    logger.info(
        "Contrôle Data Quality Gold."
    )

    # --------------------------------------------------------
    # Dataset non vide
    # --------------------------------------------------------

    if df.isEmpty():
        raise ValueError(
            "Data Quality Gold : dataset vide."
        )

    # --------------------------------------------------------
    # site obligatoire
    # --------------------------------------------------------

    if (
        df.filter(
            col("site").isNull()
        )
        .limit(1)
        .count()
        > 0
    ):
        raise ValueError(
            "Data Quality Gold : site NULL."
        )

    # --------------------------------------------------------
    # unicité des sites
    # --------------------------------------------------------

    if (
        df.groupBy("site")
        .count()
        .filter(
            col("count") > 1
        )
        .limit(1)
        .count()
        > 0
    ):
        raise ValueError(
            "Data Quality Gold : sites dupliqués."
        )

    # --------------------------------------------------------
    # température moyenne plausible
    # --------------------------------------------------------

    if (
        df.filter(
            (col("average_temperature") < -50)
            | (col("average_temperature") > 150)
        )
        .limit(1)
        .count()
        > 0
    ):
        raise ValueError(
            "Data Quality Gold : température moyenne incohérente."
        )

    logger.info(
        "Data Quality Gold validée."
    )


# ============================================================
# EXPORT PARQUET
# ============================================================

def save_partitioned_parquet(
    df: DataFrame,
    path: Path,
) -> None:
    """
    Écrit un dataset Parquet partitionné par site.

    overwrite rend l'écriture idempotente :
    relancer le pipeline remplace le résultat précédent.

    Snappy fournit une compression rapide adaptée
    aux traitements analytiques.
    """

    (
        df.write
        .mode("overwrite")
        .option(
            "compression",
            "snappy",
        )
        .partitionBy(
            "site"
        )
        .parquet(
            str(path)
        )
    )


def save_parquet(
    df: DataFrame,
    path: Path,
) -> None:
    """
    Écrit un dataset Parquet compressé avec Snappy.

    Cette sortie est conservée afin de pouvoir comparer
    Parquet classique et Apache Iceberg.
    """

    (
        df.write
        .mode("overwrite")
        .option(
            "compression",
            "snappy",
        )
        .parquet(
            str(path)
        )
    )


# ============================================================
# APACHE ICEBERG
# ============================================================

def create_iceberg_namespace(
    spark: SparkSession,
) -> None:
    """
    Crée le namespace Gold dans le catalogue Iceberg.

    L'équivalent conceptuel est la création d'un schéma
    dans une base de données relationnelle.
    """

    logger.info(
        "Création du namespace Iceberg %s.%s si nécessaire.",
        ICEBERG_CATALOG,
        ICEBERG_NAMESPACE,
    )

    spark.sql(
        f"""
        CREATE NAMESPACE IF NOT EXISTS
        {ICEBERG_CATALOG}.{ICEBERG_NAMESPACE}
        """
    )


def save_iceberg_table(
    spark: SparkSession,
    df: DataFrame,
) -> None:
    """
    Crée ou remplace la table Gold Apache Iceberg.

    Contrairement à un simple dossier Parquet, Iceberg
    maintient des métadonnées et des snapshots de table.
    """

    create_iceberg_namespace(
        spark
    )

    logger.info(
        "Écriture de la table Iceberg : %s",
        ICEBERG_TABLE,
    )

    (
        df.writeTo(
            ICEBERG_TABLE
        )
        .using(
            "iceberg"
        )
        .tableProperty(
            "write.format.default",
            "parquet",
        )
        .createOrReplace()
    )

    logger.info(
        "Table Iceberg écrite avec succès."
    )


def show_iceberg_table(
    spark: SparkSession,
) -> None:
    """Affiche le contenu de la table Iceberg."""

    logger.info(
        "Lecture de la table Iceberg : %s",
        ICEBERG_TABLE,
    )

    iceberg_df = spark.table(
        ICEBERG_TABLE
    )

    iceberg_df.show(
        truncate=False
    )


def show_iceberg_snapshots(
    spark: SparkSession,
) -> None:
    """
    Affiche les snapshots connus par Iceberg.

    Chaque commit d'une table Iceberg peut créer un snapshot,
    ce qui permettra ensuite le time travel.
    """

    logger.info(
        "Snapshots Iceberg :"
    )

    snapshots = spark.sql(
        f"""
        SELECT
            committed_at,
            snapshot_id,
            parent_id,
            operation
        FROM {ICEBERG_TABLE}.snapshots
        ORDER BY committed_at
        """
    )

    snapshots.show(
        truncate=False
    )


def show_iceberg_files(
    spark: SparkSession,
) -> None:
    """
    Affiche les fichiers de données actuellement référencés
    par la table Iceberg.
    """

    logger.info(
        "Fichiers de données Iceberg :"
    )

    files = spark.sql(
        f"""
        SELECT
            file_path,
            file_format,
            record_count
        FROM {ICEBERG_TABLE}.files
        """
    )

    files.show(
        truncate=False
    )


# ============================================================
# PIPELINE
# ============================================================

def main() -> None:
    """
    Exécute le pipeline complet :

    Bronze
        ↓
    Silver
        ↓
    Gold Parquet
        ↓
    Gold Apache Iceberg
    """

    logger.info(
        "Démarrage du pipeline Lakehouse."
    )

    spark = create_spark_session()

    try:

        # ====================================================
        # BRONZE
        # ====================================================

        logger.info(
            "Lecture de la couche Bronze."
        )

        measurements = load_parquet(
            spark,
            BRONZE_MEASUREMENTS_PATH,
        )

        equipment = load_parquet(
            spark,
            BRONZE_EQUIPMENT_PATH,
        )

        weather = load_parquet(
            spark,
            BRONZE_WEATHER_PATH,
        )

        logger.info(
            "Partitions measurements : %s",
            measurements.rdd.getNumPartitions(),
        )

        logger.info(
            "Partitions equipment : %s",
            equipment.rdd.getNumPartitions(),
        )

        logger.info(
            "Partitions weather : %s",
            weather.rdd.getNumPartitions(),
        )

        # ====================================================
        # SILVER
        # ====================================================

        logger.info(
            "Construction de la couche Silver."
        )

        silver = build_silver(
            measurements,
            equipment,
            weather,
        )

        validate_silver(
            silver
        )

        logger.info(
            "Plan d'exécution Spark Silver :"
        )

        silver.explain(
            mode="formatted"
        )

        logger.info(
            "Aperçu Silver :"
        )

        silver.show(
            truncate=False
        )

        logger.info(
            "Écriture de la couche Silver."
        )

        save_partitioned_parquet(
            silver,
            SILVER_INDUSTRIAL_PATH,
        )

        # ====================================================
        # GOLD
        # ====================================================

        logger.info(
            "Construction de la couche Gold."
        )

        gold = build_gold(
            silver
        )

        validate_gold(
            gold
        )

        logger.info(
            "Plan d'exécution Spark Gold :"
        )

        gold.explain(
            mode="formatted"
        )

        logger.info(
            "Aperçu Gold :"
        )

        gold.show(
            truncate=False
        )

        # ====================================================
        # GOLD — PARQUET CLASSIQUE
        # ====================================================

        logger.info(
            "Écriture Gold en Parquet classique."
        )

        save_parquet(
            gold,
            GOLD_SITE_REPORT_PATH,
        )

        # ====================================================
        # GOLD — APACHE ICEBERG
        # ====================================================

        logger.info(
            "Écriture Gold en Apache Iceberg."
        )

        save_iceberg_table(
            spark,
            gold,
        )

        # ====================================================
        # VÉRIFICATION ICEBERG
        # ====================================================

        show_iceberg_table(
            spark
        )

        show_iceberg_snapshots(
            spark
        )

        show_iceberg_files(
            spark
        )

        # ====================================================
        # FIN
        # ====================================================

        logger.info(
            "Couche Silver créée : %s",
            SILVER_INDUSTRIAL_PATH,
        )

        logger.info(
            "Couche Gold Parquet créée : %s",
            GOLD_SITE_REPORT_PATH,
        )

        logger.info(
            "Table Gold Iceberg créée : %s",
            ICEBERG_TABLE,
        )

        logger.info(
            "Warehouse Iceberg : %s",
            ICEBERG_WAREHOUSE_PATH,
        )

    except Exception:

        logger.exception(
            "Échec du pipeline Lakehouse."
        )

        raise

    finally:

        spark.stop()

        logger.info(
            "Session Spark arrêtée."
        )

    logger.info(
        "Pipeline Lakehouse terminé."
    )


if __name__ == "__main__":
    main()