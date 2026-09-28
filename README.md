# Industrial Data Pipeline

End-to-end Data Engineering and AI project designed to simulate a modern industrial data platform collecting, processing, validating and analyzing equipment and weather data.

The project covers the complete data lifecycle, from raw ingestion to a **Bronze / Silver / Gold Lakehouse architecture**, with distributed processing using **Apache Spark**, table management with **Apache Iceberg**, machine learning with **scikit-learn**, and a local **Retrieval-Augmented Generation (RAG)** pipeline using Sentence Transformers, Ollama and Llama.

The project also includes SQL Server integration, Apache Airflow orchestration, automated testing, Docker containerization and GitHub Actions CI.

---

## Architecture

```text
                         DATA SOURCES
                ┌─────────────┴─────────────┐
                │                           │
                ▼                           ▼
        Industrial CSV                Open-Meteo API
     equipment / sensors              Weather data
                │                           │
                └─────────────┬─────────────┘
                              │
                              ▼
                     Python Ingestion
                              │
                              ▼
                    ┌─────────────────┐
                    │     BRONZE      │
                    │   Raw Parquet   │
                    └────────┬────────┘
                             │
                             ▼
                       Apache Spark
                             │
                   joins / enrichment
                             │
                             ▼
                    ┌─────────────────┐
                    │     SILVER      │
                    │ Clean / Enriched│
                    │ Partitioned data│
                    └────────┬────────┘
                             │
                       Data Quality
                             │
                             ▼
                    ┌─────────────────┐
                    │      GOLD       │
                    │ Analytics-ready │
                    │   aggregates    │
                    └───────┬─────────┘
                            │
               ┌────────────┴────────────┐
               │                         │
               ▼                         ▼
            Parquet                Apache Iceberg
                                   ACID table format
                                         │
                                         ▼
                                snapshots / time travel
                                 schema evolution

Additional processing paths:

Industrial data ──► SQL Server ──► SQL analytics

Industrial data ──► scikit-learn ──► ML model ──► predictions

Documents ──► Sentence Transformers ──► Embeddings
                                         │
                                         ▼
                                   Vector Search
                                         │
                                         ▼
                                      Top-K
                                         │
                                         ▼
                               Context + Question
                                         │
                                         ▼
                                  Llama 3.2 3B
                                    via Ollama
                                         │
                                         ▼
                                    RAG Answer
```

Pipeline orchestration: **Apache Airflow**  
Containerization: **Docker / Docker Compose**  
Testing: **pytest**  
CI: **GitHub Actions**

---

## Data Pipeline

### 1. Data Ingestion

Industrial equipment and measurement data are ingested from CSV files.

Weather information is retrieved from the **Open-Meteo REST API** using Python.

The ingestion layer includes:

- Python modules and classes
- REST API calls with `requests`
- error handling
- structured logging
- environment-based configuration
- HTTP timeout management
- Parquet serialization

---

## 2. SQL Server Integration

Industrial data can also be loaded into **SQL Server** using:

- SQLAlchemy
- pyodbc
- environment-based credentials

SQL transformations include:

- aggregations
- CTEs
- analytical functions
- `DENSE_RANK`
- site-level industrial indicators

The SQL layer demonstrates integration between Python applications and a relational database.

---

## 3. Bronze Layer

Raw datasets are stored in the Lakehouse Bronze layer using **Apache Parquet**.

Examples include:

```text
data/lake/bronze/
├── equipment.parquet
├── measurements.parquet
└── weather.parquet
```

The Bronze layer preserves source data before business transformations.

---

## 4. Silver Layer

Apache Spark transforms and enriches the Bronze datasets.

Industrial measurements are joined with:

- equipment reference data
- weather information

The resulting Silver dataset contains enriched information such as:

- equipment identifier
- site
- equipment type
- temperature
- status
- manufacturer
- installation year
- weather measurement time
- outside temperature

Small reference datasets are broadcast during Spark joins to reduce unnecessary shuffle operations.

Silver data is physically partitioned by site.

```text
data/lake/silver/industrial_data/

├── site=Bordeaux/
├── site=Lyon/
├── site=Paris/
└── site=Toulouse/
```

---

## 5. Data Quality

Data quality checks are integrated directly into the Spark pipeline.

Examples include:

- dataset must not be empty
- `equipment_id` must not be null
- `site` must not be null
- industrial temperatures must remain within plausible limits
- Gold datasets are validated before publication

Invalid datasets raise explicit exceptions and prevent corrupted data from progressing through the pipeline.

---

## 6. Gold Layer

The Gold layer produces analytics-ready datasets for BI, reporting or Data Science.

Industrial measurements are aggregated by site.

Generated indicators include:

- number of measurements
- average industrial temperature
- maximum industrial temperature
- average outside temperature

```text
Silver
   │
   ▼
groupBy(site)
   │
   ├── COUNT
   ├── AVG temperature
   ├── MAX temperature
   └── AVG outside_temperature
   │
   ▼
Gold
```

---

## 7. Apache Parquet

The project uses **Apache Parquet** as its primary analytical file format.

The implementation covers:

- PyArrow
- columnar storage
- Snappy compression
- physical partitioning
- Spark Parquet I/O

Parquet provides efficient analytical reads and integrates naturally with Spark and modern Lakehouse architectures.

---

## 8. Apache Iceberg

The Gold dataset can also be managed as an **Apache Iceberg table**.

Iceberg adds a table-management layer on top of analytical files and provides capabilities such as:

- ACID table semantics
- metadata management
- snapshots
- schema evolution
- UPDATE operations
- time travel

The project includes dedicated examples for:

```text
src/processing/iceberg_time_travel.py
src/processing/iceberg_schema_evolution.py
```

This demonstrates the difference between a file format such as **Parquet** and a modern table format such as **Apache Iceberg**.

---

## 9. Machine Learning

A lightweight Machine Learning workflow is included using **scikit-learn**.

The example predicts industrial equipment temperature using features such as:

- outside temperature
- equipment age
- equipment load

The workflow covers:

```text
Synthetic dataset
      │
      ▼
Train / Test Split
      │
      ▼
RandomForestRegressor
      │
      ▼
Model evaluation
   MAE / R²
      │
      ▼
Model persistence
      │
      ▼
Inference
```

The trained model is serialized locally using `joblib`.

The model artifact itself is excluded from Git because it can be regenerated from the training code.

---

## 10. Embeddings and Vector Search

The project introduces semantic search using **Sentence Transformers**.

Documents are converted into dense vector representations called embeddings.

```text
Document
   │
   ▼
Sentence Transformer
   │
   ▼
Embedding vector
```

The current multilingual embedding model is:

```text
paraphrase-multilingual-MiniLM-L12-v2
```

It was selected to support the French industrial documents used by the project.

A user question is also converted into an embedding and compared with document vectors using **cosine similarity**.

The most relevant documents are returned using Top-K retrieval.

---

## 11. Retrieval-Augmented Generation (RAG)

The project implements a complete local RAG pipeline.

```text
User Question
      │
      ▼
Sentence Transformer
      │
      ▼
Question Embedding
      │
      ▼
Cosine Similarity
      │
      ▼
Vector Search
      │
      ▼
Top-K Documents
      │
      ▼
Context Construction
      │
      ▼
Llama 3.2 3B
via Ollama
      │
      ▼
Generated Answer
```

The retrieval layer provides external knowledge to the LLM at query time.

The LLM is instructed to answer only from the retrieved context and not to invent missing information.

This demonstrates an important RAG principle:

> Retrieval quality directly influences generation quality.

No LLM fine-tuning is required: relevant external information is injected into the prompt dynamically.

---

## 12. Workflow Orchestration

The pipeline includes **Apache Airflow** orchestration.

Airflow manages:

- task dependencies
- scheduling
- retries
- failure handling
- pipeline execution

Example workflow:

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

---

## 13. Testing

Automated tests are implemented using **pytest**.

The current tests cover:

- API ingestion with mocked HTTP responses
- API error handling
- Spark Silver transformations
- Spark Gold aggregations
- Data Quality validation

Run the test suite with:

```bash
python -m pytest -v
```

Current test suite:

```text
5 passed
```

---

## 14. Docker

The project can be packaged inside a Docker image containing:

- Python
- Java runtime
- PySpark
- Microsoft ODBC Driver
- project dependencies

Build the image:

```bash
docker build -t industrial-data-pipeline .
```

Run the pipeline:

```bash
docker run --rm \
  --env-file .env \
  -v "$(pwd)/data:/app/data" \
  industrial-data-pipeline
```

Docker Compose is also available:

```bash
docker compose up --build
```

---

## 15. Continuous Integration

A **GitHub Actions** workflow validates the project automatically.

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

The Docker build is executed only after the automated tests succeed.

---

## Tech Stack

| Category | Technologies |
|---|---|
| Language | Python 3.13 |
| Data Processing | pandas, NumPy, PySpark |
| Database | SQL Server |
| Database Access | SQLAlchemy, pyodbc |
| Data Formats | Apache Parquet, PyArrow |
| Lakehouse | Bronze / Silver / Gold |
| Table Format | Apache Iceberg |
| API | REST, requests, Open-Meteo |
| Orchestration | Apache Airflow |
| Machine Learning | scikit-learn |
| ML Model | Random Forest |
| Embeddings | Sentence Transformers |
| Vector Search | Cosine Similarity / Top-K |
| LLM | Llama 3.2 3B |
| Local LLM Runtime | Ollama |
| Testing | pytest |
| Containerization | Docker, Docker Compose |
| CI/CD | GitHub Actions |
| Version Control | Git, GitHub |
| Environment | Linux / WSL |

---

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
│   ├── processed/        # generated, ignored by Git
│   └── lake/             # generated, ignored by Git
│
├── models/               # generated ML models, ignored by Git
│
├── src/
│   ├── database/
│   │   ├── database.py
│   │   ├── load_to_sql.py
│   │   └── read_from_sql.py
│   │
│   ├── ingestion/
│   │   ├── api_ingestion.py
│   │   └── bronze_ingestion.py
│   │
│   ├── machine_learning/
│   │   ├── __init__.py
│   │   ├── train_model.py
│   │   └── predict.py
│   │
│   ├── processing/
│   │   ├── spark_pipeline.py
│   │   ├── iceberg_time_travel.py
│   │   └── iceberg_schema_evolution.py
│   │
│   └── rag/
│       ├── __init__.py
│       ├── embeddings.py
│       ├── vector_search.py
│       └── rag_pipeline.py
│
├── tests/
│   ├── test_api_ingestion.py
│   └── test_spark_pipeline.py
│
├── Dockerfile
├── compose.yaml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## Configuration

Create the local configuration from the provided template:

```bash
cp .env.example .env
```

Database configuration:

```env
DB_HOST=your_sql_server_host
DB_PORT=1433
DB_NAME=industrial_data
DB_USER=your_username
DB_PASSWORD=your_password
DB_DRIVER=ODBC Driver 18 for SQL Server
```

Ollama configuration:

```env
OLLAMA_URL=http://localhost:11434/api/generate
OLLAMA_MODEL=llama3.2:3b
```

The `.env` file contains local configuration and credentials and is excluded from Git.

---

## Running the RAG

Ollama must be running with the required model available.

Example model:

```bash
ollama pull llama3.2:3b
```

Run the RAG pipeline:

```bash
python src/rag/rag_pipeline.py
```

Example question:

```text
Quel équipement semble présenter un problème de surchauffe ?
```

The system:

1. embeds the question;
2. performs vector similarity search;
3. retrieves the Top-K relevant documents;
4. builds the LLM context;
5. sends the context and question to Llama;
6. generates a grounded answer.

---

## Running the Machine Learning Pipeline

Train the model:

```bash
python src/machine_learning/train_model.py
```

Run inference:

```bash
python src/machine_learning/predict.py
```

---

## Engineering Practices

The project demonstrates several Data Engineering and software engineering practices:

- modular Python architecture
- ETL / ELT concepts
- idempotent processing
- environment-based configuration
- SQL integration
- REST API ingestion
- structured logging
- error handling
- Data Quality validation
- columnar storage
- physical data partitioning
- distributed Spark processing
- broadcast joins
- Spark execution plan analysis
- Bronze / Silver / Gold architecture
- Apache Iceberg table management
- schema evolution
- time travel
- workflow orchestration
- automated testing
- API mocking
- containerization
- continuous integration
- Machine Learning training and inference
- embeddings
- semantic vector search
- Retrieval-Augmented Generation
- local LLM execution

---

## Key Learning Outcomes

This project was built as a practical exploration of a modern Data Engineering stack.

It covers the transition from traditional pipelines:

```text
CSV → Python → SQL → Parquet
```

to distributed processing:

```text
Parquet → Spark → Partitioned datasets
```

to a modern Lakehouse:

```text
Bronze → Silver → Gold → Iceberg
```

and finally Data & AI workloads:

```text
Data → Machine Learning
```

and:

```text
Documents → Embeddings → Vector Search → RAG → LLM
```

---

## Future Improvements

Possible extensions include:

- persistent vector database such as FAISS, Qdrant or similar
- advanced document chunking
- metadata filtering
- hybrid search
- production-grade reranking
- model and data observability
- cloud object storage
- deployment on a cloud platform
- production API for ML and RAG inference

---

## Author

**Walyd Alami**

Data Engineer / Engineering student at ISEP