FROM python:3.13.15-slim-trixie

WORKDIR /app

# Dépendances système :
# - Java pour PySpark
# - curl / ca-certificates pour installer le dépôt Microsoft
# - unixODBC pour la couche ODBC Linux
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre-headless \
        curl \
        ca-certificates \
        unixodbc \
        libgssapi-krb5-2 && \
    rm -rf /var/lib/apt/lists/*

# Ajout du dépôt Microsoft pour Debian 13
RUN curl -sSL -O https://packages.microsoft.com/config/debian/13/packages-microsoft-prod.deb && \
    dpkg -i packages-microsoft-prod.deb && \
    rm packages-microsoft-prod.deb && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 && \
    rm -rf /var/lib/apt/lists/*

# Copie de la liste des dépendances Python
COPY requirements.txt .

# Installation des dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copie du projet dans l'image
COPY . .

# Commande exécutée par défaut au démarrage du conteneur
CMD ["python", "src/processing/spark_pipeline.py"]