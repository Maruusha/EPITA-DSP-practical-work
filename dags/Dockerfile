FROM apache/airflow:3.1.7

USER airflow

COPY requirements.txt /opt/airflow/requirements.txt

# Install dependencies as the airflow user to keep permissions correct
USER airflow
RUN bash -c '$( head -1 $(which airflow) | cut -c3-) -m pip install --no-cache-dir -r /opt/airflow/requirements.txt'
