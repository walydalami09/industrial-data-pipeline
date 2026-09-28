"""Gestion de la connexion à SQL Server."""

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


class DatabaseManager:
    """Gère la connexion à la base SQL Server."""

    def __init__(self) -> None:
        self.host = self._get_env_variable("DB_HOST")
        self.port = int(self._get_env_variable("DB_PORT"))
        self.database = self._get_env_variable("DB_NAME")
        self.username = self._get_env_variable("DB_USER")
        self.password = self._get_env_variable("DB_PASSWORD")
        self.driver = self._get_env_variable("DB_DRIVER")

        self.engine = self._create_engine()

    @staticmethod
    def _get_env_variable(name: str) -> str:
        """Récupère une variable d'environnement obligatoire."""

        value = os.getenv(name)

        if not value:
            raise ValueError(
                f"La variable d'environnement {name} est manquante."
            )

        return value

    def _create_engine(self) -> Engine:
        """Crée le moteur SQLAlchemy."""

        connection_url = URL.create(
            "mssql+pyodbc",
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            query={
                "driver": self.driver,
                "TrustServerCertificate": "yes",
            },
        )

        return create_engine(connection_url)

    def test_connection(self) -> str:
        """Teste la connexion et retourne la version de SQL Server."""

        with self.engine.connect() as connection:
            result = connection.execute(
                text("SELECT @@VERSION")
            )

            return result.scalar_one()


def main() -> None:
    """Teste la connexion à SQL Server."""

    database = DatabaseManager()

    version = database.test_connection()

    print("Connexion SQL Server réussie.")
    print(version)


if __name__ == "__main__":
    main()