from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SparkTest")
    .master("local[2]")
    .getOrCreate()
)

# Identifier la version Hadoop utilisée par Spark.
print(
    "Version Hadoop :",
    spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
)

df = spark.range(5)

df.show()

print("Nombre de lignes :", df.count())

# ============================================================
# TEST D'ÉCRITURE PARQUET
# ============================================================

# print("Début de l'export Parquet")

# df.write.mode("overwrite").parquet(
# "data/spark_test_output"
# )

# print("Export Parquet terminé")

spark.stop()

print("Test terminé")