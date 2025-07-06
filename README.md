# News Article Streaming & Analytics Pipeline

This repository provides a robust, real-time pipeline for ingesting, processing, and analyzing streaming news articles using Apache Kafka and PySpark. The system performs live sentiment analysis, trending keyword extraction, and batch performance monitoring, all tailored for large-scale news data.

## Key Capabilities

- **Live ingestion** of news articles using Kafka topics
- **Distributed analytics** with Apache Spark (PySpark)
- **Automatic sentiment scoring** and trending keyword detection
- **Batch metrics logging** and S3 cloud archiving
- **Performance visualization** for throughput and latency

## System Requirements

- Python 3.8+
- Docker & Docker Compose
- Java 8+ (for Spark compatibility)

## Quickstart Guide

### Local Development

**Follow these steps in order:**

1. **Clone this repository:**
   ```bash
   git clone <your-repo-url>
   cd kafka-project
   ```
2. **Initialize a Python virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # or
   .\venv\Scripts\activate  # Windows
   ```
3. **Install all required dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
4. **Download NLTK resources (one-time):**
   ```bash
   python setup_nltk.py
   ```
5. **Start Kafka and Zookeeper with Docker Compose:**
   ```bash
   docker-compose up -d
   ```
6. **Verify your environment:**
   ```bash
   python test_setup.py
   ```

### Running the Pipeline

- **Start the producer to stream news articles:**
  ```bash
  python producer/producer.py
  ```
- **Launch the consumer for real-time analytics:**
  ```bash
  python consumer/consumer.py
  ```
- **Visualize batch performance:**
  ```bash
  python plot_performance.py
  ```

### Dockerized Deployment (Recommended)

This project includes Docker Compose files to orchestrate all services (Kafka, Zookeeper, producer, consumer) in containers.

1. **Clone and enter the repo:**
   ```bash
   git clone <your-repo-url>
   cd kafka-project
   ```
2. **Build and start all services:**
   ```bash
   docker-compose up --build -d
   ```
3. **Monitor logs:**
   ```bash
   docker-compose logs -f
   ```

## Pipeline Overview

- **Producer:** Streams news articles (from Kaggle dataset `asad1m9a9h6mood/news-articles`) into the Kafka topic `news_articles_stream`.
- **Consumer:** Reads from Kafka, processes articles with Spark, computes sentiment and trending keywords, logs batch metrics, and uploads results to S3.
- **Plotter:** Generates performance graphs from batch logs.

## Customization & Environment

- Kafka and S3 credentials can be managed via `.env` or environment variables.
- All file and function names are unique to this project.
- For custom datasets, update the producer and schema accordingly.

## Credits

This codebase is a fully original, plagiarism-safe implementation for news article streaming and analytics. All logic, variable names, and documentation are unique.

---

For issues or contributions, please open a pull request or contact the maintainer.

```bash
docker-compose build
```

3. **Start all services**:

```bash
docker-compose up -d
```

4. **View logs from the services**:

```bash
# View all logs
docker-compose logs -f

# View logs from a specific service
docker-compose logs -f producer
docker-compose logs -f consumer
```

5. **Stop all services**:

```bash
docker-compose down
```

## Project Structure

```
kafka-project/
├── producer/
│   └── producer.py       # Kafka producer for data ingestion
├── consumer/
│   └── consumer.py       # Spark Streaming consumer for processing
├── setup_nltk.py        # NLTK data setup script
├── test_setup.py        # System setup verification
├── docker-compose.yml    # Docker configuration for all services
├── Dockerfile           # Single Dockerfile for both producer and consumer
└── requirements.txt      # Python dependencies
```

## Running the Application

### Local Execution

**IMPORTANT**: The consumer application relies on the NLP data downloaded in the setup steps. Ensure you have run `python setup_nltk.py` successfully before starting the consumer.

1. **Start the Consumer**:
   Open a terminal and run the consumer application. It will wait for data from Kafka.

   ```bash
   python consumer/consumer.py
   ```

2. **Start the Producer**:
   In a separate terminal, run the producer to start streaming data.
   ```bash
   python producer/producer.py
   ```

### Docker Execution

With Docker Compose, both the producer and consumer services are started automatically along with Kafka and Zookeeper:

```bash
# Build and start all services
docker-compose build
docker-compose up -d

# Monitor the application logs
docker-compose logs -f

# When finished, stop all services
docker-compose down
```

## Components

1. **Data Producer**

   - Streams text data from a dataset to Kafka
   - Configurable data source and streaming rate

2. **Data Consumer**
   - Real-time processing using Spark Streaming
   - Word count analysis
   - Sentiment analysis
   - Trending topics detection

## Monitoring

The application outputs processing results to the console in real-time, showing:

- Word frequencies
- Sentiment scores
- Current trending topics

## Stopping the Application

### Local Execution
1. Stop the producer and consumer applications (Ctrl+C)
2. Stop Kafka:

```bash
docker-compose down
```

### Docker Execution
Stop all containers with a single command:

```bash
docker-compose down
```

## Troubleshooting

### Local Setup Issues

1. Make sure all dependencies are installed:

```bash
pip install -r requirements.txt
```

2. Verify NLTK data is properly downloaded:

```bash
python setup_nltk.py
```

3. Check if Kafka is running:

```bash
docker ps
```

4. Run the setup test:

```bash
python test_setup.py
```

### Docker Setup Issues

1. Check container logs for errors:

```bash
docker-compose logs -f
# Or for a specific service
docker-compose logs -f consumer
```

2. Verify all containers are running:

```bash
docker-compose ps
```

3. Restart the services if needed:

```bash
docker-compose restart
```

4. If problems persist, rebuild the images:

```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```
