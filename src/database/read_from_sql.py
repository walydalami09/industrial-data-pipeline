import pandas as pd
from sqlalchemy import text

# Récupérer notre connexion SQL Server.
from database import engine


# ============================================================
# 1. REQUÊTE SQL
# ============================================================

query = text("""
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
# 2. RÉCUPÉRATION DES DONNÉES DANS PANDAS
# ============================================================

with engine.connect() as connection:

    df_site_report = pd.read_sql(query, connection)


# ============================================================
# 3. AFFICHAGE DU DATAFRAME
# ============================================================

print(df_site_report)

print("\nType du résultat :")
print(type(df_site_report))

# ============================================================
# 4. EXPORT DU DATAFRAME AU FORMAT PARQUET
# ============================================================

df_site_report.to_parquet(
    "data/site_report.parquet",
    engine="pyarrow",
    compression="snappy",
    index=False
)

print("Export Parquet terminé.")

# ============================================================
# 5. LECTURE DU FICHIER PARQUET
# ============================================================

df_parquet = pd.read_parquet("data/site_report.parquet")

print("\nDonnées relues depuis Parquet :")
print(df_parquet)
print("\nVérification des données :")
print(df_site_report.equals(df_parquet))