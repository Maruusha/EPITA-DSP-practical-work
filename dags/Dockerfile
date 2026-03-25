# this file Not used at the moment

FROM apache/airflow:3.1.7

USER airflow

# Set working directory
#WORKDIR /dags
COPY requirements.txt /opt/airflow/requirements.txt

# Install dependencies as the airflow user to keep permissions correct
USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
