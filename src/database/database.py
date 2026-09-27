from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# ============================================================
# 1. CONFIGURATION DE LA CONNEXION
# ============================================================

connection_url = URL.create(
    "mssql+pyodbc",
    host=r".\SQLEXPRESS",
    database="industrial_data",
    query={
        "driver": "ODBC Driver 17 for SQL Server",
        "Trusted_Connection": "yes",
        "TrustServerCertificate": "yes"
    }
)


# ============================================================
# 2. CRÉATION DU MOTEUR SQLALCHEMY
# ============================================================

engine = create_engine(connection_url)


# ============================================================
# 3. TEST DE LA CONNEXION
# ============================================================

# Ce bloc s'exécute uniquement lorsque database.py est lancé directement.
# Il ne s'exécute pas lorsqu'un autre fichier importe engine.

if __name__ == "__main__":

    with engine.connect() as connection:

        result = connection.execute(
            text("SELECT @@VERSION")
        )

        print(result.scalar())


# ============================================================
# 4. CRÉATION DE LA BASE INDUSTRIELLE — ARCHIVE
# ============================================================

# Ce bloc a servi à créer la base industrial_data.
# Il est conservé comme note, mais n'est plus exécuté.

# IMPORTANT : pour réutiliser ce bloc, il faut un moteur connecté
# à la base master, car industrial_data pourrait ne pas encore exister.

# with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:

#     connection.execute(
#         text("""
#             IF DB_ID('industrial_data') IS NULL
#                 CREATE DATABASE industrial_data
#         """)
#     )

# print("Base industrial_data prête.")