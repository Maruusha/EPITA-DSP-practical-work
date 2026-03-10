# Use the tiangolo FastAPI base image
FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9

# Set the working directory inside the container
WORKDIR /app

# Copy aLL logic files into the container
# This includes main.py, predict.py, and db_utility.py, etc...
COPY . /app

RUN sed -i 's/\r$//' /app/prestart.sh && chmod +x /app/prestart.sh

# Install requirements
RUN pip install --no-cache-dir -r requirements.txt

# The tiangolo image automatically looks for /app/main.py 
# and runs it on port 80. 