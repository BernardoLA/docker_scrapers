# Start from the base image
FROM apache/airflow:latest

# Switch to root user to install system packages
USER root

# Install gcc and other necessary packages
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Copy requirements.txt
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

