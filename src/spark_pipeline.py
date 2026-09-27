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
# 4. AGRÉGATION DES MESURES PAR SITE
# ============================================================

# Calculer plusieurs indicateurs pour chaque site industriel.
site_report = valid_measurements.groupBy("site").agg(
    F.count("*").alias("nb_measurements"),
    F.avg("temperature").alias("average_temperature"),
    F.max("temperature").alias("max_temperature")
)

print("\nRapport industriel par site :")
site_report.show()


# ============================================================
# 5. EXPORT DU RAPPORT AU FORMAT PARQUET
# ============================================================

# Spark écrit les résultats dans un dossier contenant
# un ou plusieurs fichiers Parquet.
site_report.write.mode("overwrite").parquet(
    "data/spark_site_report"
)

print("\nExport Parquet terminé.")


# ============================================================
# 6. ARRÊT DE SPARK
# ============================================================

# Libérer les ressources utilisées par la session Spark.
spark.stop()

print("Pipeline Spark terminé.")