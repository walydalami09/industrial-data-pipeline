import pandas as pd
from sqlalchemy import text

# Récupérer le moteur de connexion défini dans database.py.
from database import engine


# ============================================================
# 1. CHARGEMENT DES FICHIERS CSV
# ============================================================

# Charger les mesures industrielles dans un DataFrame.
df_measurements = pd.read_csv("data/measurements.csv")

# Charger le référentiel des équipements dans un DataFrame.
df_equipment = pd.read_csv("data/equipment.csv")


# ============================================================
# 2. CHARGEMENT DES DATAFRAMES DANS SQL SERVER
# ============================================================

# Créer la table measurements et y insérer les données.
df_measurements.to_sql(
    "measurements",
    engine,
    if_exists="replace",
    index=False
)

# Créer la table equipment et y insérer les données.
df_equipment.to_sql(
    "equipment",
    engine,
    if_exists="replace",
    index=False
)

print("Chargement SQL Server terminé.")


# ============================================================
# 3. VÉRIFICATION DU CHARGEMENT
# ============================================================

# Exécuter une requête SQL pour compter les mesures enregistrées.
with engine.connect() as connection:

    result = connection.execute(
        text("SELECT COUNT(*) FROM measurements")
    )

    print("Nombre de mesures en base :", result.scalar())