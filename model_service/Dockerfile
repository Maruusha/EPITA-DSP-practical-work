# Use the tiangolo FastAPI base image
FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9

# Install PostgreSQL system dependencies (needed for psycopg2)
RUN pip install --no-cache-dir sqlalchemy psycopg2-binary

# Set the working directory inside the container
WORKDIR /app

# Copy aLL logic files into the container
# This includes main.py, predict.py, and db_utility.py
COPY . /app

# The tiangolo image automatically looks for /app/main.py 
# and runs it on port 80. 