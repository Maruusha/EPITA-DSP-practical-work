# Not used at the moment

FROM apache/airflow:3.1.7

USER root

# Set working directory
WORKDIR /dags

COPY requirements.txt .

# Install dependencies as the airflow user to keep permissions correct
USER airflow
RUN pip install --no-cache-dir -r requirements.txt
