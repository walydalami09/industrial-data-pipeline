from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 1. DÉMARRAGE DE SPARK
# ============================================================

# Créer une session Spark pour exécuter les traitements sur notre PC.
spark = (
    SparkSession.builder
    .appName("IndustrialDataPipeline")
    .master("local[*]")
    .getOrCreate()
)


# ============================================================
# 2. LECTURE DES DONNÉES INDUSTRIELLES
# ============================================================

# Lire le CSV et créer un DataFrame Spark.
df = (
    spark.read
    .option("header", True)       # Première ligne = noms des colonnes.
    .option("inferSchema", True)  # Détecter les types des colonnes.
    .option("nullValue", "N/A")   # Interpréter N/A comme une valeur manquante.
    .csv("data/measurements.csv")
)

print("\nDonnées industrielles :")
df.show()

print("\nSchéma du DataFrame :")
df.printSchema()


# ============================================================
# 3. NETTOYAGE DES DONNÉES
# ============================================================

# Conserver uniquement les mesures dont la température est renseignée.
valid_measurements = df.filter(
    F.col("temperature").isNotNull()
)

# ============================================================
# 4. ENRICHISSEMENT AVEC LE RÉFÉRENTIEL DES ÉQUIPEMENTS
# ============================================================

# Charger le référentiel des équipements.
df_equipment = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/equipment.csv")
)

# Effectuer une jointure LEFT sur equipment_id.
df_enriched = valid_measurements.join(
    df_equipment,
    on="equipment_id",
    how="left"
)

print("\nMesures enrichies :")
df_enriched.show()


# ============================================================
# 5. AGRÉGATION DES MESURES PAR SITE
# ============================================================

# Calculer plusieurs indicateurs pour chaque site industriel.
site_report = df_enriched.groupBy("site").agg(
    F.count("*").alias("nb_measurements"),
    F.avg("temperature").alias("average_temperature"),
    F.max("temperature").alias("max_temperature")
)

print("\nRapport industriel par site :")
site_report.show()


# ============================================================
# 6. EXPORT DU RAPPORT AU FORMAT PARQUET
# ============================================================

# Spark écrit les résultats dans un dossier contenant
# un ou plusieurs fichiers Parquet.
site_report.write.mode("overwrite").parquet(
    "data/spark_site_report"
)

print("\nExport Parquet terminé.")

# Afficher le nombre de partitions du DataFrame.
print("Partitions :", site_report.rdd.getNumPartitions())

# Afficher le plan d'exécution physique et logique.
site_report.explain(True)



# ============================================================
# 7. ENRICHISSEMENT DU RAPPORT AVEC LA MÉTÉO
# ============================================================

# Lire les données météo générées par notre script API.
df_weather = spark.read.parquet("data/weather.parquet")

print("\nDonnées météo :")
df_weather.show()

# Enrichir le rapport industriel avec la température extérieure.
final_report = site_report.join(
    df_weather,
    site_report["site"] == df_weather["city"],
    how="left"
)

print("\nRapport industriel enrichi avec la météo :")
final_report.show()

final_report.write.mode("overwrite").parquet(
    "data/final_industrial_report"
)

print("\nRapport industriel final exporté.")


# ============================================================
# 8. ARRÊT DE SPARK
# ============================================================

# Libérer les ressources utilisées par la session Spark.
spark.stop()

print("Pipeline Spark terminé.")