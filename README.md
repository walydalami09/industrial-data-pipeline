# Industrial Data Pipeline

End-to-end Data Engineering project designed to simulate an industrial data platform collecting, processing and enriching equipment and weather data.

The project implements a complete data pipeline using **Python, SQL Server, Apache Spark, Apache Airflow, Parquet, Docker and GitHub Actions**, from raw data ingestion to automated processing and CI validation.

## Architecture

```text
                     ┌─────────────────────┐
                     │   Industrial CSV    │
                     │ Equipment / Sensors │
                     └──────────┬──────────┘
                                │
                                ▼
                         Python ingestion
                                │
                                ▼
                     ┌─────────────────────┐
                     │     SQL Server      │
                     │ Industrial database │
                     └──────────┬──────────┘
                                │
                         SQL aggregation
                                │
                                ▼
                      site_report.parquet
                                │
                                │
Open-Meteo API                  │
      │                         │
      ▼                         │
Python API ingestion            │
      │                         │
      ▼                         │
weather.parquet ────────────────┤
                                │
                                ▼
                       ┌─────────────────┐
                       │  Apache Spark   │
                       │ Data processing │
                       └────────┬────────┘
                                │
                                ▼
                    final_industrial_report
                           (Parquet)

Pipeline orchestration: Apache Airflow
Containerization: Docker / Docker Compose
Testing: pytest
CI: GitHub Actions
```

## Data Pipeline

The pipeline combines industrial equipment measurements with external weather data.

### 1. Industrial data ingestion

Raw CSV files contain equipment information and sensor measurements.

Python ingestion scripts load these datasets into **SQL Server** using **SQLAlchemy** and **pyodbc**.

### 2. SQL transformation

Industrial measurements are aggregated by site in SQL Server.

The resulting dataset contains indicators such as:

- number of measurements
- average temperature
- maximum temperature

The aggregated dataset is exported to **Apache Parquet**.

### 3. Weather API ingestion

Weather data is collected from the **Open-Meteo REST API** for the industrial sites.

The ingestion layer uses:

- `requests`
- object-oriented Python
- error handling
- configurable HTTP timeout

The resulting weather dataset is stored in Parquet format.

### 4. Distributed processing with Spark

**PySpark** reads the industrial and weather Parquet datasets and combines them into the final industrial report.

Spark is used for:

- distributed data processing
- joins
- transformations
- Parquet I/O
- execution plan analysis

### 5. Workflow orchestration

The complete pipeline is orchestrated with **Apache Airflow**.

```text
load_to_sql
     │
     ▼
read_from_sql ───────┐
                     │
                     ▼
               Spark processing
                     ▲
                     │
ingest_weather ──────┘
```

Airflow manages task dependencies, retries and pipeline execution.

## Tech Stack

| Category | Technologies |
|---|---|
| Language | Python 3.13 |
| Data Processing | pandas, NumPy, PySpark |
| Database | SQL Server |
| Database Access | SQLAlchemy, pyodbc |
| Data Format | Apache Parquet, PyArrow |
| API | REST, requests, Open-Meteo |
| Orchestration | Apache Airflow |
| Testing | pytest |
| Containerization | Docker, Docker Compose |
| CI | GitHub Actions |
| Version Control | Git, GitHub |
| Environment | Linux / WSL |

## Project Structure

```text
industrial-data-pipeline/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── dags/
│   └── industrial_pipeline.py
│
├── data/
│   ├── raw/
│   └── processed/
│
├── src/
│   ├── database/
│   │   ├── database.py
│   │   ├── load_to_sql.py
│   │   └── read_from_sql.py
│   │
│   ├── ingestion/
│   │   └── api_ingestion.py
│   │
│   ├── processing/
│   │   └── spark_pipeline.py
│   │
│   └── experiments/
│
├── tests/
│   ├── test_api_ingestion.py
│   ├── test_basics.py
│   └── test_spark_pipeline.py
│
├── Dockerfile
├── compose.yaml
├── requirements.txt
├── .env.example
└── README.md
```

## Testing

The project includes automated tests with **pytest**.

The tests cover:

- Python functions
- API ingestion using mocks
- Spark transformations

Run the test suite with:

```bash
python -m pytest -v
```

## Docker

The application can be packaged as a Docker image containing the Python environment, Java runtime, PySpark and the required dependencies.

Build the image:

```bash
docker build -t industrial-data-pipeline .
```

Run the Spark pipeline:

```bash
docker run --rm \
  --env-file .env \
  -v "$(pwd)/data:/app/data" \
  industrial-data-pipeline
```

The project also provides a Docker Compose configuration:

```bash
docker compose up --build
```

## Continuous Integration

A **GitHub Actions CI pipeline** automatically validates the project on pushes and pull requests to `main`.

```text
Git push
   │
   ▼
GitHub Actions
   │
   ▼
pytest
   │
   ▼
Docker build
   │
   ▼
CI validation
```

The Docker build only runs after the automated tests succeed.

## Configuration

Database credentials are provided through environment variables and are not committed to Git.

Create a local `.env` file based on:

```bash
cp .env.example .env
```

Example configuration:

```env
DB_HOST=your_sql_server_host
DB_PORT=1433
DB_NAME=industrial_data
DB_USER=your_username
DB_PASSWORD=your_password
DB_DRIVER=ODBC Driver 18 for SQL Server
```

## Engineering Practices

This project applies several Data Engineering practices:

- modular Python architecture
- separation of ingestion, database and processing layers
- environment-based configuration
- automated testing
- API mocking
- structured logging
- pipeline retries
- workflow orchestration
- containerized execution
- CI validation
- columnar storage with Parquet
- distributed processing with Spark

## Future Improvements

Possible extensions include:

- Data Lake / Lakehouse architecture
- Delta Lake or Apache Iceberg
- cloud object storage
- data quality checks
- monitoring and observability
- infrastructure deployment through CI/CD

## Author

**Walyd Alami**

Data Engineer / Engineering student at ISEP