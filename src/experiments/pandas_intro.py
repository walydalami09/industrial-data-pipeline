import pandas as pd

# Charger les mesures industrielles dans un DataFrame.

df = pd.read_csv("data/measurements.csv")
df_equipment = pd.read_csv("data/equipment.csv")

#creation des deux dataframe 
valid_measurements = df[df["temperature"].notna()]
rejected_measurements = df[df["temperature"].isna()]

# ============================================================
# AGRÉGATIONS PAR SITE
# ============================================================

# Température moyenne par site.
average_by_site = valid_measurements.groupby("site")["temperature"].mean()

# Nombre de mesures par site.
count_by_site = valid_measurements.groupby("site").size()

print("\nTempérature moyenne par site :")
print(average_by_site)

print("\nNombre de mesures par site :")
print(count_by_site)

site_report = valid_measurements.groupby("site").agg(
    nb_measurements=("equipment_id", "size"),
    average_temperature=("temperature", "mean"),
    max_temperature=("temperature", "max")
).reset_index()

print(site_report)


# Afficher les premières lignes.
print(df_equipment.head())
print(df.head())
df_enriched = pd.merge(
    df,
    df_equipment,
    on="equipment_id",
    how="left",
    validate="many_to_one",
    indicator=True
)

print(df_enriched)

def load_measurements_pandas(file_path: str) -> pd.DataFrame:
    """
    Charge un fichier CSV et retourne un DataFrame pandas.
    """
    df = pd.read_csv(file_path)
    return df 

def clean_measurements(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Sépare les mesures valides et rejetées selon la température.
    """
    valid_measurements = df[df["temperature"].notna()]
    rejected_measurements = df[df["temperature"].isna()]
    return valid_measurements , rejected_measurements 

def generate_site_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Génère les indicateurs par site à partir des mesures valides.

    Indicateurs :
        - Nombre de mesures.
        - Température moyenne.
        - Température maximale.
    """
    site_report = df.groupby("site").agg(
    nb_measurements=("equipment_id", "size"),
    average_temperature=("temperature", "mean"),
    max_temperature=("temperature", "max")
    ).reset_index()
    return site_report

def enrich_measurements( df: pd.DataFrame, df_equipment: pd.DataFrame
                        ) -> pd.DataFrame:
    """
    Enrichit les mesures avec le référentiel des équipements.

    Effectue une jointure LEFT sur equipment_id.
    Vérifie que chaque équipement est unique dans le référentiel.

    Retour :
        DataFrame contenant les mesures enrichies.
    """
    df_enriched = pd.merge(
    df,
    df_equipment,
    on="equipment_id",
    how="left",
    validate="many_to_one",
    indicator=True
    )
    return df_enriched

# ============================================================
# EXÉCUTION DU PIPELINE PANDAS
# ============================================================

# 1. Charger les deux fichiers CSV.
df = load_measurements_pandas("data/measurements.csv")
df_equipment = load_measurements_pandas("data/equipment.csv")

# 2. Séparer les mesures valides et rejetées.
valid_measurements, rejected_measurements = clean_measurements(df)

# 3. Enrichir les mesures valides avec le référentiel.
df_enriched = enrich_measurements(valid_measurements, df_equipment)

# 4. Générer le rapport par site.
site_report = generate_site_report(df_enriched)

# Exporter le rapport pandas dans un fichier CSV.
site_report.to_csv("data/site_report.csv", index=False)

# Exporter les mesures enrichies.
df_enriched.to_csv("data/enriched_measurements.csv", index=False)

