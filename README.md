# Real-time Text Data Processing System

This project implements a scalable text data processing system that performs real-time analysis of streaming text data using Apache Kafka and Apache Spark.

## Features

- Real-time text data ingestion using Kafka
- Parallel processing with PySpark
- Text analysis including:
  - Word count
  - Sentiment analysis
  - Trending topics
- Real-time visualization of results

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Java 8 or higher (for Apache Spark)

## Setup Instructions

**IMPORTANT**: The following steps must be performed in order.

1. **Clone the repository**:

```bash
git clone <repository-url>
cd kafka-project
```

2. **Create and activate virtual environment**:

```bash
python -m venv venv
source venv/bin/activate  # On Linux/Mac
# or
.\venv\Scripts\activate  # On Windows
```

3. **Install Python dependencies**:

```bash
pip install -r requirements.txt
```

4. **Download NLP Data (MANDATORY ONE-TIME STEP)**:
   This script downloads the necessary data for sentiment analysis. It must be run before starting the consumer.

   ```bash
   python setup_nltk.py
   ```

5. **Start Kafka using Docker**:

```bash
docker-compose up -d
```

6. **Run the setup test to verify everything is working**:

```bash
python test_setup.py
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
├── docker-compose.yml    # Docker configuration for Kafka
└── requirements.txt      # Python dependencies
```

## Running the Application

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

1. Stop the producer and consumer applications (Ctrl+C)
2. Stop Kafka:

```bash
docker-compose down
```

## Troubleshooting

If you encounter any issues:

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
