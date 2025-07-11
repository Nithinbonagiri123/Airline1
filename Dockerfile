FROM openjdk:11-slim

# Set working directory
WORKDIR /app

# Install Python and required system packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    gcc \
    libc-dev \
    procps \
    iputils-ping \
    curl \
    bash && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy project source code
COPY . .

# Download NLTK data
RUN python3 setup_nltk.py
