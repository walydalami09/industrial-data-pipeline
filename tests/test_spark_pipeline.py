import pytest
from pyspark.sql import SparkSession

from src.processing.spark_pipeline import (
    build_gold,
    build_silver,
    validate_silver,
)


# ============================================================
# FIXTURE SPARK
# ============================================================

@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """
    Crée une session Spark locale utilisée
    pendant les tests.
    """

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("SparkPipelineTests")
        .getOrCreate()
    )

    yield session

    session.stop()


# ============================================================
# TEST SILVER
# ============================================================

def test_build_silver(
    spark: SparkSession,
) -> None:
    """
    Vérifie que les mesures sont correctement enrichies
    avec les données equipment et weather.
    """

    # -------------------------
    # Measurements
    # -------------------------

    measurements = spark.createDataFrame(
        [
            (
                "EQ001",
                "Paris",
                "Pump",
                95.2,
                "ALERT",
            ),
            (
                "EQ002",
                "Toulouse",
                "Motor",
                68.1,
                "OK",
            ),
        ],
        [
            "equipment_id",
            "site",
            "equipment_type",
            "temperature",
            "status",
        ],
    )

    # -------------------------
    # Equipment
    # -------------------------

    equipment = spark.createDataFrame(
        [
            (
                "EQ001",
                "Siemens",
                2018,
            ),
            (
                "EQ002",
                "ABB",
                2020,
            ),
        ],
        [
            "equipment_id",
            "manufacturer",
            "installation_year",
        ],
    )

    # -------------------------
    # Weather
    # -------------------------

    weather = spark.createDataFrame(
        [
            (
                "Paris",
                "2026-09-28T14:00",
                18.0,
            ),
            (
                "Toulouse",
                "2026-09-28T14:00",
                22.0,
            ),
        ],
        [
            "city",
            "measurement_time",
            "outside_temperature",
        ],
    )

    # -------------------------
    # Build Silver
    # -------------------------

    result = build_silver(
        measurements=measurements,
        equipment=equipment,
        weather=weather,
    )

    rows = {
        row["equipment_id"]: row
        for row in result.collect()
    }

    # -------------------------
    # Assertions
    # -------------------------

    assert len(rows) == 2

    assert (
        rows["EQ001"]["manufacturer"]
        == "Siemens"
    )

    assert (
        rows["EQ001"]["outside_temperature"]
        == 18.0
    )

    assert (
        rows["EQ002"]["manufacturer"]
        == "ABB"
    )

    assert (
        rows["EQ002"]["outside_temperature"]
        == 22.0
    )


# ============================================================
# TEST GOLD
# ============================================================

def test_build_gold(
    spark: SparkSession,
) -> None:
    """
    Vérifie l'agrégation des données Silver
    pour construire la couche Gold.
    """

    silver = spark.createDataFrame(
        [
            (
                "EQ001",
                "Paris",
                90.0,
                18.0,
            ),
            (
                "EQ002",
                "Paris",
                100.0,
                20.0,
            ),
            (
                "EQ003",
                "Toulouse",
                68.0,
                22.0,
            ),
        ],
        [
            "equipment_id",
            "site",
            "temperature",
            "outside_temperature",
        ],
    )

    result = build_gold(silver)

    rows = {
        row["site"]: row
        for row in result.collect()
    }

    # -------------------------
    # Paris
    # -------------------------

    assert (
        rows["Paris"]["nb_measurements"]
        == 2
    )

    assert (
        rows["Paris"]["average_temperature"]
        == pytest.approx(95.0)
    )

    assert (
        rows["Paris"]["max_temperature"]
        == pytest.approx(100.0)
    )

    assert (
        rows["Paris"][
            "average_outside_temperature"
        ]
        == pytest.approx(19.0)
    )

    # -------------------------
    # Toulouse
    # -------------------------

    assert (
        rows["Toulouse"]["nb_measurements"]
        == 1
    )

    assert (
        rows["Toulouse"]["average_temperature"]
        == pytest.approx(68.0)
    )


# ============================================================
# TEST DATA QUALITY SILVER
# ============================================================

def test_validate_silver_invalid_temperature(
    spark: SparkSession,
) -> None:
    """
    Vérifie que la Data Quality détecte
    une température industrielle incohérente.
    """

    silver = spark.createDataFrame(
        [
            (
                "EQ001",
                "Paris",
                200.0,
            ),
        ],
        [
            "equipment_id",
            "site",
            "temperature",
        ],
    )

    with pytest.raises(
        ValueError,
        match="température incohérente",
    ):
        validate_silver(silver)