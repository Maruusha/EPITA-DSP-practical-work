# 🌬️☀️ Renewable Energy Production Prediction

**Course:** EPITA Data Science in Production (DSP)

A complete end-to-end renewable energy prediction platform. This project automates data ingestion and validation, trains and serves a machine learning model, and provides a Streamlit interface for predictions.

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
  - [Notes](#notes)

---

## Features

- Automated data ingestion and validation with Apache Airflow
- Data quality checks using Great Expectations
- Machine learning model serving with FastAPI
- Streamlit web interface for manual and batch predictions
- PostgreSQL tracking for prediction history and data quality metrics
- Separate raw/good/bad data handling for incoming datasets

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
git clone <paste-your-repository-url-here>
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

- `energy_sources`
- `predictions`
- `data_quality_stats`

If entries exist in `predictions` and `data_quality_stats`, the project is saving results successfully.

---

## Stopping the Project

Stop the running containers:

```bash
docker compose down
```

To remove volumes and reset the database state completely:

```bash
docker compose down -v
```

---

## Project Structure

- `dags/` — Airflow DAG definitions for ingestion, validation, and scheduled predictions
- `data/` — raw, good, and bad dataset storage
- `gx/` — Great Expectations configuration and validation suites
- `model_service/` — FastAPI backend, database helpers, and the trained model
- `model_training/` — model training script
- `webapp/` — Streamlit user interface

---

## Notes

- The Airflow pipeline validates incoming raw data and separates it into `data/good_data/` or `data/bad_data/`.
- Predictions and validation metadata are stored in PostgreSQL.
- If the Docker stack is running, local Python installation is only required for model training or development.
