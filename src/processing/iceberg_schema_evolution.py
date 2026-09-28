"""Démonstration de l'évolution de schéma avec Apache Iceberg."""

from spark_pipeline import (
    ICEBERG_TABLE,
    create_spark_session,
)


def main() -> None:
    spark = create_spark_session()

    try:

        # ====================================================
        # 1. SCHÉMA ACTUEL
        # ====================================================

        print("\n=== SCHÉMA AVANT ÉVOLUTION ===")

        spark.table(
            ICEBERG_TABLE
        ).printSchema()

        # ====================================================
        # 2. AJOUT D'UNE COLONNE
        # ====================================================

        print("\n=== AJOUT DE LA COLONNE temperature_gap ===")

        spark.sql(
            f"""
            ALTER TABLE {ICEBERG_TABLE}
            ADD COLUMN temperature_gap DOUBLE
            """
        )

        # ====================================================
        # 3. NOUVEAU SCHÉMA
        # ====================================================

        print("\n=== SCHÉMA APRÈS ÉVOLUTION ===")

        spark.table(
            ICEBERG_TABLE
        ).printSchema()

        # ====================================================
        # 4. ÉTAT DES DONNÉES
        # ====================================================

        print("\n=== DONNÉES APRÈS AJOUT DE LA COLONNE ===")

        spark.table(
            ICEBERG_TABLE
        ).orderBy(
            "site"
        ).show(
            truncate=False
        )

        # ====================================================
        # 5. CALCUL DE LA NOUVELLE COLONNE
        # ====================================================

        print("\n=== CALCUL DE temperature_gap ===")

        spark.sql(
            f"""
            UPDATE {ICEBERG_TABLE}

            SET temperature_gap =
                average_temperature
                - average_outside_temperature

            WHERE average_outside_temperature IS NOT NULL
            """
        )

        # ====================================================
        # 6. RÉSULTAT
        # ====================================================

        print("\n=== TABLE APRÈS UPDATE ===")

        spark.table(
            ICEBERG_TABLE
        ).orderBy(
            "site"
        ).show(
            truncate=False
        )

        # ====================================================
        # 7. HISTORIQUE DES SNAPSHOTS
        # ====================================================

        print("\n=== SNAPSHOTS ICEBERG ===")

        spark.sql(
            f"""
            SELECT
                committed_at,
                snapshot_id,
                parent_id,
                operation
            FROM {ICEBERG_TABLE}.snapshots
            ORDER BY committed_at
            """
        ).show(
            truncate=False
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()