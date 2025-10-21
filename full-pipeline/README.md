# Data Stack Framework - Complete Lakehouse Architecture with Medallion

This project implements a complete Lakehouse architecture using the Medallion structure (Bronze → Silver → Gold) with open source tools for data ingestion, processing, orchestration, and querying of Stack Overflow data.

## 🏗️ System Architecture

### Main Components

- **Apache Spark (PySpark)**: Distributed processing engine for transformations
- **Apache Airflow**: Workflow orchestrator with DAGs
- **DLT (Data Load Tool)**: Data ingestion from external sources
- **Apache Iceberg**: Table format for Lakehouse
- **Nessie**: Metadata catalog and data versioning
- **MinIO**: S3-compatible storage
- **Trino**: SQL query engine for Silver layer
- **Dremio**: Analytics platform for Gold layer
- **PostgreSQL**: Database for Airflow metadata

### Medallion Structure

```
Bronze Layer (Raw Data)
├── Raw data in Parquet format
├── Overwrite allowed
├── Source: StackOverflow Dataset (7 tables)
└── Contains annual data (2019-2020)

Silver Layer (Curated Data)
├── Iceberg tables with MERGE writes
├── Curated, normalized and historical data
├── Includes fecha_cargue column
└── Deduplication and data cleaning

Gold Layer (Analytics Ready)
├── Iceberg tables with MERGE writes
├── KPIs, metrics and aggregations
├── Includes fecha_cargue column
└── Data optimized for analysis
```

## 🚀 Quick Start

### 1. Start Infrastructure

```bash
docker compose up -d
```

This command will start all necessary services:
- Spark Master and Worker
- Airflow Web Server and Scheduler
- MinIO (Object Storage)
- Nessie (Metadata Catalog)
- Dremio (Gold Layer Query Engine)
- Trino (Silver Layer Query Engine)
- PostgreSQL (Airflow Metadata)

### 2. Configure MinIO

1. Access MinIO web interface: http://localhost:9001/login
   - **Username**: `admin`
   - **Password**: `password`

2. Verify that buckets were created automatically:
   ```
   bronze/     # Raw data (Bronze Layer)
   silver/     # Curated data (Silver Layer)
   gold/       # Analytical data (Gold Layer)
   ```

### 3. Configure Airflow

1. Access Airflow web interface: http://localhost:8080/login
   - **Username**: `admin`
   - **Password**: `admin`

2. Configure Spark connection:
   - Go to **Admin > Connections**: http://localhost:8080/connection/list/
   - Create a new connection with the following parameters:
     - **Conn Id**: `spark_default`
     - **Conn Type**: `Spark`
     - **Host**: `spark://spark-master`
     - **Port**: `7077`
     - Leave other fields with their default values

### 4. Execute Complete ETL Pipeline

1. Go to DAGs tab: http://localhost:8080/home
2. Search and click on the DAG `spark_load_to_iceberg`
3. Activate the DAG and execute it manually

The pipeline will execute the following stages in sequence:

#### Bronze Layer (Ingestion with DLT)
- **Source**: StackOverflow Dataset from public S3
- **Tables**: posts, votes, comments, users, badges, postlinks, posthistory
- **Years**: 2019-2020 (annual data)
- **Format**: Parquet
- **Destination**: MinIO bucket `bronze`

#### Silver Layer (Transformation with PySpark + Iceberg)
- **Processing**: Deduplication, normalization, cleaning
- **Format**: Apache Iceberg with Nessie
- **Characteristics**: 
  - MERGE writes (no overwrite)
  - `fecha_cargue` column for auditing
  - `anio` column extracted from filename
- **Destination**: MinIO bucket `silver` + Nessie catalog

#### Gold Layer (Aggregations with PySpark + Iceberg)
- **Generated KPIs**:
  - `post_counts_by_user`: Count of questions and answers per user
  - `vote_stats_per_post`: Vote statistics per post
  - `top_tags`: Most popular tags
  - `user_engagement`: User engagement metrics
  - `badges_summary`: Badge summary per user
- **Format**: Apache Iceberg with Nessie
- **Destination**: MinIO bucket `gold` + Nessie catalog

## 📊 Query Data

### Query Silver Layer - Trino

```bash
docker exec -it trino_g4 trino
```

Once inside Trino, you can execute SQL queries to explore Silver layer data:

```sql
-- View all available Silver tables
USE iceberg.silver;

SHOW TABLES;
```

### Query Gold Layer - Dremio

1. Access Dremio: http://localhost:9047/signup
2. Create your user account
3. Add a new data source:

#### General Configuration:
- **Type**: Nessie Source
- **Name**: `nessie`
- **Nessie endpoint URL**: `http://nessie:19120/api/v2/`
- **Nessie authentication type**: `None`

#### Storage Configuration:
- **AWS access key**: `admin`
- **AWS access secret**: `password`
- **Disable**: "Encrypt connection"

#### Connection Properties:
- **Name**: `fs.s3a.path.style.access` | **Value**: `true`
- **Name**: `fs.s3a.endpoint` | **Value**: `minio:9000`
- **Name**: `dremio.s3.compat` | **Value**: `true`

Now you can query Gold layer tables from Dremio.

## 🔧 Services and Ports

| Service | Port | URL | Description |
|---------|------|-----|-------------|
| Airflow Web UI | 8080 | http://localhost:8080 | Orchestration interface |
| Spark Master UI | 9090 | http://localhost:9090 | Spark monitoring |
| MinIO Console | 9001 | http://localhost:9001 | Storage management |
| MinIO API | 9000 | http://localhost:9000 | S3-compatible API |
| Dremio | 9047 | http://localhost:9047 | Gold layer query engine |
| Trino | 8081 | http://localhost:8081 | Silver layer query engine |
| Nessie | 19120 | http://localhost:19120 | Metadata catalog |
| Jupyter | 8888 | http://localhost:8888 | Interactive development |

## 📁 Project Structure

```
full-pipeline/
├── dags/                           # Airflow DAGs
│   └── spark_to_iceberg_dag.py    # Main pipeline DAG
├── jobs/                           # Processing scripts
│   ├── bronze.py                   # Bronze ingestion with DLT
│   ├── silver.py                   # Silver transformation with PySpark
│   └── gold.py                     # Gold aggregations with PySpark
├── dlt/                            # DLT configuration
│   └── secrets.toml                # MinIO credentials for DLT
├── docker-compose.yaml             # Service configuration
├── dockerfile.airflow              # Custom Airflow image
├── requirements.txt                # Python dependencies
├── airflow.env                     # Airflow environment variables
├── diagrama-arquitectura.png       # Architecture diagram
└── README.md                       # This file
```

### Logs and Debugging

- **Airflow Logs**: Available in Airflow web interface
- **Spark Logs**: Accessible in Spark Master web interface
- **Docker Logs**: `docker logs <container_name>`

### Useful Commands

```bash
# View container status
docker compose ps

# View logs for a specific service
docker logs <container_name>

# Restart a service
docker compose restart <service_name>

# Stop all services
docker compose down

# Clean volumes (WARNING! Deletes all data)
docker compose down -v

# View logs in real time
docker compose logs -f <service_name>
```

### Pipeline Verification

```bash
# Verify buckets exist in MinIO
docker exec -it minio_project_g4 mc ls myminio/

# Verify tables in Nessie
curl http://localhost:19120/api/v1/trees/main

# Verify Trino connection
docker exec -it trino_g4 trino --execute "SHOW CATALOGS;"

# Verify Dremio connection
curl http://localhost:9047/api/v3/project
```