import csv 
import json


# ============================================================
# 1. CHARGEMENT ET VALIDATION DES DONNÉES
# ============================================================

def load_measurements(file_path: str) -> tuple[list[dict], list[dict]]:
    """
    Lit un fichier CSV et sépare les mesures valides des mesures rejetées.

    Retour :
        - Liste des mesures valides (température convertie en float).
        - Liste des mesures rejetées (température non convertible).
    """

    list_measure_valid = []
    list_measure_non_valid = []

    with open(file_path, mode="r", encoding="utf-8") as file:

        reader = csv.DictReader(file)

        for measurement in reader:

            try:
                # Conversion de la température en nombre.
                measurement["temperature"] = float(measurement["temperature"])

                list_measure_valid.append(measurement)

            except ValueError:
                # Conservation des mesures dont la température est invalide.
                measurement["rejection_reason"] = "Invalid temperature"

                list_measure_non_valid.append(measurement)

    return list_measure_valid, list_measure_non_valid


# ============================================================
# 2. COMPTAGE DES MESURES PAR SITE
# ============================================================

def count_equipment_by_site(measurements: list[dict]) -> dict:
    """
    Compte le nombre de mesures pour chaque site.

    Exemple :
        {"Toulouse": 2, "Bordeaux": 2, "Paris": 1}
    """

    nb_equipement_site = {}

    for measurement in measurements:

        site = measurement["site"]

        nb_equipement_site[site] = nb_equipement_site.get(site, 0) + 1

    return nb_equipement_site


# ============================================================
# 3. TEMPÉRATURE MOYENNE PAR SITE
# ============================================================

def average_temperature_by_site(measurements: list[dict]) -> dict:
    """
    Calcule la température moyenne des mesures de chaque site.
    """

    nb_equipement_site = {}
    temp_total_par_site = {}
    moy_temp_site = {}

    # Première boucle : compter les mesures et additionner les températures.
    for measurement in measurements:

        site = measurement["site"]
        temperature = measurement["temperature"]

        nb_equipement_site[site] = nb_equipement_site.get(site, 0) + 1

        temp_total_par_site[site] = (
            temp_total_par_site.get(site, 0) + temperature
        )

    # Deuxième boucle : calculer les moyennes.
    for site, temp in temp_total_par_site.items():

        moy_temp_site[site] = temp / nb_equipement_site[site]

    return moy_temp_site


# ============================================================
# 4. FILTRAGE DES ANOMALIES
# ============================================================

def filter_anomalie(
    measurements: list[dict],
    statuses: set[str]
) -> list[dict]:
    """
    Retourne les mesures dont le statut appartient aux statuts demandés.

    Exemple :
        statuses = {"ERROR", "WARNING"}
    """

    anomalie = []

    for measurement in measurements:

        status = measurement["status"]

        if status in statuses:
            anomalie.append(measurement)

    return anomalie


# ============================================================
# 5. GÉNÉRATION DU RAPPORT INDUSTRIEL
# ============================================================

def generate_equipment_report(
    measurements: list[dict],
    statuses: set[str]
) -> dict:
    """
    Assemble les différents indicateurs dans un dictionnaire.

    Réutilise les fonctions de comptage, de moyenne et de filtrage.
    """

    equipement_rapport = {}

    equipement_rapport["total_measurements"] = len(measurements)

    equipement_rapport["equipment_by_site"] = count_equipment_by_site(
        measurements
    )

    equipement_rapport["average_temperature_by_site"] = (
        average_temperature_by_site(measurements)
    )

    equipement_rapport["anomalies"] = filter_anomalie(
        measurements,
        statuses
    )

    return equipement_rapport


# ============================================================
# 6. ENREGISTREMENT D'UN FICHIER JSON
# ============================================================

def save_json(data: dict | list, file_path: str) -> None:
    """
    Enregistre un dictionnaire ou une liste dans un fichier JSON.

    Entrées :
        data : données à enregistrer.
        file_path : chemin du fichier de destination.

    Retour :
        Aucun. La fonction écrit directement dans un fichier.
    """

    with open(file_path, mode="w", encoding="utf-8") as file:

        json.dump(data, file, indent=4)


# ============================================================
# EXÉCUTION DU PIPELINE
# ============================================================

# 1. Charger les données du CSV.
valid_measurements, rejected_measurements = load_measurements(
    "data/measurements.csv"
)

# 2. Calculer les indicateurs uniquement sur les mesures valides.
report = generate_equipment_report(
    valid_measurements,
    {"ERROR", "WARNING"}
)

# 3. Enregistrer le rapport industriel.
save_json(
    report,
    "data/report.json"
)

# 4. Enregistrer séparément les mesures rejetées.
save_json(
    rejected_measurements,
    "data/rejected_measurements.json"
)

# 5. Afficher un résumé dans le terminal.
print("Pipeline terminé.")
print("Mesures valides :", len(valid_measurements))
print("Mesures rejetées :", len(rejected_measurements))