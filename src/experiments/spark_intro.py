from pyspark.sql import SparkSession


# ============================================================
# 1. CRÉATION DE LA SESSION SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("IndustrialDataPipeline")
    .master("local[*]")
    .getOrCreate()
)


# ============================================================
# 2. LECTURE DU FICHIER PARQUET
# ============================================================

df_spark = spark.read.parquet("data/site_report.parquet")


# ============================================================
# 3. AFFICHAGE DES DONNÉES
# ============================================================

df_spark.show()

df_spark.printSchema()


# ============================================================
# 4. ARRÊT DE SPARK
# ============================================================

spark.stop()