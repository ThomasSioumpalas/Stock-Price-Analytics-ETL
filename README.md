Market Data ETL Pipeline

A production-style data engineering pipeline that streams financial market data from the Alpha Vantage API through Apache Kafka into Databricks, transforms it with dbt, and orchestrates everything with Apache Airflow.

# Architecture

Alpha Vantage API
↓
Python Producer polls the API daily, pushes to Kafka topic
↓
Apache Kafka message broker (ZooKeeper + Broker + Kafka UI)
↓
Python Consumer reads from Kafka, writes raw data to Databricks Delta Lake
↓
dbt (Databricks) 3-layer medallion transformation
├── staging clean, cast, deduplicate
├── intermediate enrich with rolling avg, % change, anomaly flags
└── marts price history + alerts tables
↓
Apache Airflow orchestrates the full pipeline on a daily schedule

# Tech Stack
"
| Data Source | Alpha Vantage REST API |
| Message Broker | Apache Kafka 7.4.4 (Confluent) |
| Coordination | Apache ZooKeeper 7.4.4 |
| Kafka UI | Provectus Kafka UI |
| Data Warehouse | Databricks (Delta Lake) |
| Transformation | dbt-databricks 1.11.x |
| Orchestration | Apache Airflow 2.9.1 |
| Metadata DB | PostgreSQL 15 |
| Infrastructure | Docker + Docker Compose |
| Language | Python 3.12 |
"
# Project Structure

ETL/
├── docker-compose.yml # all services: Kafka, Airflow, Postgres
├── .env  
├── .gitignore
├── producer.py # Alpha Vantage → Kafka
├── consumer.py # Kafka → Databricks raw table
├── dags/
│ └── market_pipeline_dag.py # Airflow DAG
└── market_pipeline/ # dbt project
├── dbt_project.yml
├── profiles.yml # reads credentials from env vars
└── models/
├── staging/
│ ├── sources.yml
│ ├── schema.yml
│ └── stg_market_prices.sql
├── intermediate/
│ └── int_market_prices.sql
└── marts/
├── schema.yml
├── mart_price_history.sql
└── mart_alerts.sql

# Prerequisites

- Docker Desktop installed and running
- Python 3.12+
- Alpha Vantage API key (free at [alphavantage.co](https://www.alphavantage.co/support/#api-key))
- Databricks workspace (free Community Edition at [community.cloud.databricks.com](https://community.cloud.databricks.com))

# Setup

1. Clone the repo and create your `.env` file

```bash
git clone <your-repo-url>
cd ETL
```

Create a `.env` file in the project root:

```bash
# Kafka UI
KAFKA_UI_USER=admin
KAFKA_UI_PASSWORD=pass

# Alpha Vantage
ALPHA_VANTAGE_API_KEY=your_key_here

# Databricks
DATABRICKS_HOST=https://dbc-xxxxxxxx.cloud.databricks.com
DATABRICKS_TOKEN=dapi_xxxxxxxxxxxxxxxxxxxx
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxxxxxxxxxx
DATABRICKS_CATALOG=workspace
DATABRICKS_SCHEMA=market_data
```

2. Create the schema in Databricks

Open the Databricks SQL Editor and run:

```sql
CREATE SCHEMA IF NOT EXISTS market_data;
```

3. Start all services

```bash
docker compose up -d
```

Wait 2-3 minutes for Airflow to finish installing dependencies. Services will be available at:

| Kafka UI | http://localhost:8080/kafkaui | admin / pass |
| Airflow | http://localhost:8081 | admin / admin |

4. Install Python dependencies locally

```bash
pip install confluent-kafka databricks-sql-connector python-dotenv requests dbt-databricks
```

Running the Pipeline
Manual run (for testing)

```bash
# produce data to Kafka
python producer.py

# consume from Kafka into Databricks
python consumer.py

# run dbt transformations
cd market_pipeline
dbt run
dbt test
```

# Automated run via Airflow

1. Go to `http://localhost:8081`
2. Find the `market_pipeline` DAG
3. Toggle it **on**
4. Click the play button to trigger a manual run

The DAG runs automatically at **6:00 AM Monday–Friday**.

# dbt Models

* Staging — `stg_market_prices`

Reads from `raw_market_prices` and:

- Casts string columns to correct types (`DOUBLE`, `BIGINT`, `DATE`)
- Renames columns to snake_case (`open` → `open_price`)
- Deduplicates rows using `QUALIFY`

* Intermediate — `int_market_prices`

Builds on staging and calculates:

- Daily price change in dollars
- Daily price change percentage
- 5-day rolling average close price
- Anomaly flag (price moved more than 2% in one day)

* Marts

**`mart_price_history`** — full historical archive with a human-readable `day_label` column (`Strong Up`, `Up`, `Flat`, `Down`, `Strong Down`). Used for dashboards and reporting.

**`mart_alerts`** — filtered view of only anomaly days (`is_anomaly = true`). Used for alerting on significant price movements.

# Airflow DAG

The `market_pipeline` DAG runs 6 tasks in sequence:

```
fetch_and_produce          Alpha Vantage API → Kafka topic
        ↓
consume_to_databricks      Kafka → Databricks raw_market_prices
        ↓
dbt_staging                dbt run stg_market_prices
        ↓
dbt_intermediate           dbt run int_market_prices
        ↓
dbt_marts                  dbt run mart_price_history + mart_alerts
        ↓
dbt_test                   dbt test (data quality checks)
```

Each task retries twice on failure with a 5-minute delay between attempts.

# Kafka Topics

| `market-prices` | Daily OHLCV data per symbol, produced by `producer.py` |
| `__consumer_offsets` | Internal Kafka topic tracking consumer group offsets |

# Data Quality Tests

dbt tests run automatically at the end of every pipeline run:

| `stg_market_prices` | `trade_date` | not_null, unique |
| `stg_market_prices` | `symbol` | not_null |
| `stg_market_prices` | `close_price` | not_null |
| `mart_price_history` | `trade_date` | not_null, unique |
| `mart_alerts` | `trade_date` | not_null |
| `mart_alerts` | `is_anomaly` | not_null |
