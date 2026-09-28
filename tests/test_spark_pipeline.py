import pytest
from pyspark.sql import SparkSession

from src.processing.spark_pipeline import enrich_with_weather


@pytest.fixture
def spark() -> SparkSession:
    """Crée une session Spark pour les tests."""

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("SparkTests")
        .getOrCreate()
    )

    yield session

    session.stop()


def test_enrich_with_weather(spark: SparkSession) -> None:
    """Vérifie que les données météo sont correctement jointes."""

    site_report = spark.createDataFrame(
        [
            ("Paris", 2, 90.0),
            ("Toulouse", 1, 68.1),
        ],
        ["site", "nb_measurements", "average_temperature"],
    )

    weather = spark.createDataFrame(
        [
            ("Paris", 18.0),
            ("Toulouse", 22.0),
        ],
        ["city", "outside_temperature"],
    )

    result = enrich_with_weather(
        site_report,
        weather,
    )

    rows = {
    row["site"]: row
    for row in result.collect()
    }

    assert len(rows) == 2

    assert rows["Paris"]["outside_temperature"] == 18.0
    assert rows["Toulouse"]["outside_temperature"] == 22.0