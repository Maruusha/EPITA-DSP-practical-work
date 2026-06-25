# 🌬️☀️ Renewable Energy Production Prediction

**Course:** EPITA Data Science in Production (DSP)

A production-grade MLOps platform for renewable energy forecasting. The system automates data ingestion and validation, trains and promotes machine learning models via MLflow, serves predictions through a FastAPI backend, and provides a Streamlit interface for manual and batch predictions.

## Table of Contents

- [🌬️☀️ Renewable Energy Production Prediction](#️️-renewable-energy-production-prediction)
  - [Table of Contents](#table-of-contents)
  - [Features](#features)
  - [Prerequisites](#prerequisites)
  - [Quick Start](#quick-start)
    - [1. Clone the repository](#1-clone-the-repository)
    - [2. Create the environment file](#2-create-the-environment-file)
  - [Optional: Train the Model](#optional-train-the-model)
  - [Run the Project](#run-the-project)
    - [1. Build Airflow resources](#1-build-airflow-resources)
    - [2. Build the containers](#2-build-the-containers)
    - [3. Start the stack](#3-start-the-stack)
  - [Access the Services](#access-the-services)
  - [Verify Database \& Tables](#verify-database--tables)
    - [Option 1: Use Docker Compose](#option-1-use-docker-compose)
    - [Option 2: Use the container name directly](#option-2-use-the-container-name-directly)
  - [Stopping the Project](#stopping-the-project)
  - [Project Structure](#project-structure)
  - [Airflow DAGs](#airflow-dags)
  - [CI/CD Pipeline](#cicd-pipeline)
  - [Notes](#notes)

---

## Features

- Automated data ingestion every 5 minutes with Apache Airflow
- Schema and data quality validation using Great Expectations
- Criticality-based alerting to Microsoft Teams (High / Medium / Low)
- Batch prediction every 2 minutes on validated good data via FastAPI
- Weekly automated model retraining with champion/challenger promotion via MLflow
- Hot-swappable ML models — no restart required after promotion
- Streamlit web interface for single predictions, batch CSV uploads, and prediction history
- PostgreSQL tracking for predictions, data quality stats, training baselines, and input features
- Grafana dashboards for monitoring predictions, data quality, and training statistics
- Great Expectations data docs served via nginx

---

## Prerequisites

Install the following before running the project:

- Git
- Docker Desktop with Docker Compose enabled
- Python 3.9+ (only required if you plan to train the model locally)

---

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/Maruusha/EPITA-DSP-practical-work.git
cd EPITA-DSP-practical-work
```

### 2. Create the environment file

Copy the provided template to create the required `.env` file.

- On Windows:

```powershell
copy .env.template .env
```

- On Mac/Linux:

```bash
cp .env.template .env
```

---

## Optional: Train the Model

A pre-trained model is included in `model_service/baseline_model.pkl`. If you want to retrain the model locally:

```bash
pip install pandas scikit-learn==1.6.1 numpy
python model_training/train_evaluate.py
```

This regenerates `model_service/baseline_model.pkl`.

---

## Run the Project

### 1. Build Airflow resources

```bash
docker compose build airflow-init
```

### 2. Build the containers

```bash
docker compose build
```

### 3. Start the stack

```bash
docker compose up -d
```

> Allow 2 to 3 minutes for Airflow and PostgreSQL to initialize before accessing the services.

---

## Access the Services

| Service | URL | Notes |
|---|---|---|
| Streamlit UI | http://localhost:8501 | Predict energy and upload batch CSV files |
| Airflow Web UI | http://localhost:8081 | User: `airflow`, Password: `airflow` |
| FastAPI docs | http://localhost:8080/docs | API documentation and prediction endpoints |
| MLflow UI | http://localhost:5000 | Experiment tracking and model registry |
| Great Expectations docs | http://localhost:8090 | Data validation reports |
| Grafana | http://localhost:3000 | Monitoring dashboards |

---

## Verify Database & Tables

After the stack is running and predictions have been created, confirm the PostgreSQL tables and data with the following commands.

### Option 1: Use Docker Compose

```bash
docker compose exec db psql -U user -d predictions_db -c "\dt"
```

Check the main tables:

```bash
docker compose exec db psql -U user -d predictions_db -c "SELECT * FROM energy_sources LIMIT 10;"
```
```bash
docker compose exec db psql -U user -d predictions_db -c "SELECT * FROM predictions ORDER BY predict_date DESC LIMIT 10;"
```
```bash
docker compose exec db psql -U user -d predictions_db -c "SELECT * FROM data_quality_stats ORDER BY ingestion_timestamp DESC LIMIT 10;"
```
```bash
docker compose exec db psql -U user -d predictions_db -c "SELECT * FROM training_statistics ORDER BY id DESC LIMIT 10;"
```
```bash
docker compose exec db psql -U user -d predictions_db -c "SELECT * FROM input_data ORDER BY id DESC LIMIT 10;"
```
```bash
docker compose exec db psql -U user -d predictions_db -c "SELECT * FROM drift_statistics ORDER BY check_date DESC LIMIT 10;"
```

### Option 2: Use the container name directly

```bash
docker exec -it postgres psql -U user -d predictions_db -c "\dt"
```

Then inspect saved rows:

```bash
docker exec -it postgres psql -U user -d predictions_db -c "SELECT * FROM predictions ORDER BY predict_date DESC LIMIT 10;"
```
```bash
docker exec -it postgres psql -U user -d predictions_db -c "SELECT * FROM data_quality_stats ORDER BY ingestion_timestamp DESC LIMIT 10;"
```

These queries should show the tables:

- `energy_sources` — Known energy source types (Wind, Solar, Mixed)
- `input_data` — Full feature vectors logged at inference time for drift detection
- `predictions` — Prediction records with model version, energy source, and timestamps
- `data_quality_stats` — Per-ingestion error rates, criticality, and schema validity
- `training_statistics` — Baseline production stats saved after each model promotion
- `drift_statistics` — Production and covariate drift stats computed daily from `data/drift_data/`

If entries exist in `predictions` and `data_quality_stats`, the project is saving results successfully.

---

## Stopping the Project

| Command | Stops Containers? | Removes Containers? | Removes Networks? | Removes Volumes? |
|---|---|---|---|---|
| `docker compose stop` | ✅ Yes | ❌ No | ❌ No | ❌ No |
| `docker compose down` | ✅ Yes | ✅ Yes | ✅ Yes | ❌ No |
| `docker compose down -v` | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |

Use this command to stop containers without removing any Docker resources:

```bash
docker compose stop
```

Use this command to stop and remove containers and networks, but keep persistent volumes:

```bash
docker compose down
```

Use this command to stop and remove containers, networks, and volumes (reset database state completely):

```bash
docker compose down -v
```

---

## Project Structure

- `.github/` — CI/CD GitHub Actions workflows (ci.yml for tests, cd.yml for releases)
- `dags/` — Airflow DAG definitions for ingestion, validation, batch prediction, model training, and drift detection
- `data/` — raw, good, bad, archived, and drift dataset storage
- `grafana/` — Grafana dashboard definitions and provisioning config
- `gx/` — Great Expectations configuration and validation suites
- `logs/` — Airflow runtime logs
- `model_service/` — FastAPI backend, Pydantic schemas, database helpers, and the trained model
- `model_training/` — standalone model training script (generates baseline_model.pkl)
- `tests/` — unit tests for criticality, file splitting, Pydantic models, and stats
- `webapp/` — Streamlit user interface

---

## Airflow DAGs

### `ingestion_validate_data` — every 5 minutes

Picks a random CSV from `data/raw_data/`, validates it against 9 Great Expectations rules (columns, value ranges, valid days/months/seasons/sources, production 0–25,000 MWh), saves quality metrics to PostgreSQL, sends Teams alerts for High/Medium criticality issues, then splits valid and invalid rows into `data/good_data/` and `data/bad_data/`.

### `prediction_job` — every 2 minutes

Scans `data/good_data/` for unprocessed CSV files, sends batches of 100 rows to the FastAPI `/predict` endpoint, and marks files as processed. Skips if no new files are found.

### `training_job` — weekly on Sundays at 2 AM

Aggregates all good data (requires 500+ rows), trains a RandomForest pipeline (OneHotEncoding + SelectFromModel feature selection + RandomForest 100 trees), logs the run to MLflow, compares RMSLE against the current `@champion` model, promotes if better, triggers a hot-swap via FastAPI `/reload-model`, archives processed files, and sends Teams alerts on failure.

---

## CI/CD Pipeline

### CI (`ci.yml`) — triggers on pull requests to `develop`

1. **Lint** — runs `flake8` (max line length 120)
2. **Tests** — runs `pytest tests/ -v` with Python 3.12

### CD (`cd.yml`) — triggers on push to `main`

Automatically creates a GitHub Release and bumps the version based on commit message keywords:

| Keyword in commit message | Version bump |
|---|---|
| `[major]` (case-insensitive) | `v1.3.2` → `v2.0.0` |
| `[minor]` (case-insensitive) | `v1.3.2` → `v1.4.0` |
| *(no keyword)* | `v1.3.2` → `v1.3.3` |

---

## Notes

- The Airflow pipeline validates incoming raw data and separates it into `data/good_data/` or `data/bad_data/`.
- The FastAPI service loads the MLflow `@champion` model on startup and falls back to `baseline_model.pkl` if MLflow is unavailable. The active model version is included in every prediction response.
- Predictions and validation metadata are stored in PostgreSQL.
- Input features are logged to `input_data` at inference time to support future data drift analysis.
- Teams webhook alerts are sent for High criticality (error rate > 50% or invalid schema) and Medium criticality (error rate 10–50%).
- If the Docker stack is running, local Python installation is only required for model training or development.
