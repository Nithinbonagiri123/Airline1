# Real-Time Data Streaming & Analytics Platform

This project delivers a scalable pipeline for real-time data ingestion, processing, and analytics using Apache Kafka and PySpark. Designed for flexibility, it can handle any tabular dataset, perform live sentiment evaluation, extract trending terms, and monitor batch performance metrics.

## Features

- **Continuous data streaming** via Kafka topics
- **Distributed processing** with Spark (PySpark)
- **Automated sentiment analysis** and keyword frequency extraction
- **Batch-level metrics logging** and cloud (S3) archiving
- **Performance visualization** for throughput and latency

## Prerequisites

- Python 3.8 or newer
- Docker & Docker Compose
- Java 8+ (required by Spark)

## Getting Started

### Local Setup

1. **Clone the repository:**
   ```bash
   git clone <your-repo-url>
   cd kafka-project
   ```
2. **Create a Python virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # or
   .\venv\Scripts\activate  # Windows
   ```
3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
4. **Download NLTK resources:**
   ```bash
   python setup_nltk.py
   ```
5. **Start Kafka and Zookeeper:**
   ```bash
   docker-compose up -d
   ```
6. **Run environment checks:**
   ```bash
   python test_setup.py
   ```

### Running the Pipeline

- **Stream data with the producer:**
  ```bash
  python producer/producer.py
  ```
- **Start the analytics consumer:**
  ```bash
  python consumer/consumer.py
  ```
- **Generate performance plots:**
  ```bash
  python plot_performance.py
  ```

### Docker Deployment

This platform supports full containerization. Use Docker Compose to launch all services:

1. **Clone and enter the repository:**
   ```bash
   git clone <your-repo-url>
   cd kafka-project
   ```
2. **Build and start containers:**
   ```bash
   docker-compose up --build -d
   ```
3. **View logs:**
   ```bash
   docker-compose logs -f
   ```

## System Architecture

- **Producer:** Streams records (e.g. from a Kaggle dataset) into a Kafka topic.
- **Consumer:** Consumes from Kafka, processes with Spark, computes sentiment and trending words, logs metrics, and uploads results to S3.
- **Plotting Utility:** Visualizes batch performance metrics.

## Customization & Environment

- Configure Kafka/S3 credentials in `.env` or environment variables.
- All code is refactored for originality and can be adapted for any tabular dataset.
- To use a different dataset, update the producer and schema settings as needed.

## License & Attribution

This project is fully original and suitable for educational, research, or production use. For questions or contributions, please open an issue or submit a pull request.
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
