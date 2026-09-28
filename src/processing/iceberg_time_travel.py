"""Démonstration des snapshots et du Time Travel Apache Iceberg."""

from spark_pipeline import (
    ICEBERG_TABLE,
    create_spark_session,
)


def main() -> None:
    spark = create_spark_session()

    try:
        # ====================================================
        # 1. ÉTAT ACTUEL
        # ====================================================

        print("\n=== TABLE ACTUELLE ===")

        spark.table(
            ICEBERG_TABLE
        ).orderBy("site").show(truncate=False)

        # ====================================================
        # 2. SNAPSHOTS AVANT MODIFICATION
        # ====================================================

        print("\n=== SNAPSHOTS AVANT MODIFICATION ===")

        snapshots_before = spark.sql(
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

        snapshots_before.show(truncate=False)

        # On récupère le snapshot actuellement actif.
        first_snapshot_id = (
            snapshots_before
            .orderBy("committed_at")
            .first()["snapshot_id"]
        )

        print(
            f"\nSnapshot initial : {first_snapshot_id}"
        )

        # ====================================================
        # 3. MODIFICATION DE LA TABLE
        # ====================================================

        print("\n=== MODIFICATION ICEBERG ===")

        spark.sql(
            f"""
            UPDATE {ICEBERG_TABLE}
            SET average_temperature = 999.0
            WHERE site = 'Paris'
            """
        )

        print(
            "La température moyenne de Paris "
            "a été volontairement modifiée à 999."
        )

        # ====================================================
        # 4. ÉTAT ACTUEL APRÈS MODIFICATION
        # ====================================================

        print("\n=== TABLE ACTUELLE APRÈS UPDATE ===")

        spark.table(
            ICEBERG_TABLE
        ).orderBy("site").show(truncate=False)

        # ====================================================
        # 5. SNAPSHOTS APRÈS MODIFICATION
        # ====================================================

        print("\n=== SNAPSHOTS APRÈS MODIFICATION ===")

        snapshots_after = spark.sql(
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

        snapshots_after.show(truncate=False)

        # ====================================================
        # 6. TIME TRAVEL
        # ====================================================

        print(
            "\n=== TIME TRAVEL : SNAPSHOT INITIAL ==="
        )

        historical_df = (
            spark.read
            .option(
                "versionAsOf",
                str(first_snapshot_id),
            )
            .table(
                ICEBERG_TABLE
            )
        )

        historical_df.orderBy(
            "site"
        ).show(
            truncate=False
        )

        print(
            "\nLe snapshot historique conserve "
            "la valeur originale de Paris."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()