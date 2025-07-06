import os
import time
import json
import pandas as pd
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import kagglehub


def setup_kafka_producer(max_retries=10, kafka_env_var='KAFKA_NEWS_SERVERS', default_host='localhost:9092'):
    """
    Initialize and return a Kafka producer instance.
    Retries connection if the broker is unavailable.
    """
    logger = logging.getLogger("dataset_streamer")
    broker_addr = os.environ.get(kafka_env_var, default_host)
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker_addr,
                value_serializer=lambda record: json.dumps(record).encode('utf-8')
            )
            logger.info(f"Kafka producer connected to {broker_addr}")
            return producer
        except NoBrokersAvailable as err:
            logger.warning(f"Kafka broker not available (attempt {attempt}/{max_retries}). Retrying in 4s...")
            time.sleep(4)
    logger.error("Failed to connect to Kafka after multiple attempts.")
    return None


def stream_csv_to_kafka(producer, topic, csv_file, id_col='article_id', content_col='content', extra_cols=None, delay=0.5, row_limit=1000):
    """
    Send rows from a CSV file to a Kafka topic as JSON messages.
    Args:
        producer: KafkaProducer instance
        topic: Kafka topic name
        csv_file: Path to CSV file
        id_col, content_col: Column names for ID and content
        extra_cols: List of extra columns to include (optional)
        delay: Seconds to wait between messages
        row_limit: Number of last rows to send
    """
    logger = logging.getLogger("dataset_streamer")
    try:
        df = pd.read_csv(csv_file)
        if content_col not in df.columns or id_col not in df.columns:
            logger.error(f"CSV missing required columns: {id_col}, {content_col}")
            return
        df = df.dropna(subset=[content_col])
        if row_limit:
            df = df.tail(row_limit)
        cols = [id_col, content_col] + (extra_cols if extra_cols else [])
        for _, row in df.iterrows():
            message = {col: row[col] for col in cols if col in row}
            # Add timestamp if available, else use current time
            message['timestamp'] = float(row.get('published_at', time.time()))
            producer.send(topic, value=message)
            logger.debug(f"Sent to Kafka: {message}")
            time.sleep(delay)
        logger.info(f"Finished streaming {len(df)} records to Kafka topic '{topic}'.")
    except Exception as e:
        logger.error(f"Error streaming CSV to Kafka: {e}")


def download_kaggle_dataset(dataset_path, filename):
    """
    Download a dataset from Kaggle using kagglehub and return the local file path.
    """
    try:
        local_dir = kagglehub.dataset_download(dataset_path)
        return os.path.join(local_dir, filename)
    except Exception as e:
        logging.getLogger("dataset_streamer").error(f"Failed to download dataset: {e}")
        return None


def main():
    logging.basicConfig(level=logging.INFO)
    producer = setup_kafka_producer()
    if not producer:
        return
    topic = 'news_articles_stream'
    kaggle_dataset = "asad1m9a9h6mood/news-articles"
    kaggle_csv = "news_articles.csv"
    csv_path = download_kaggle_dataset(kaggle_dataset, kaggle_csv)
    if csv_path:
        stream_csv_to_kafka(
            producer,
            topic,
            csv_path,
            id_col='article_id',
            content_col='content',
            extra_cols=['title', 'author', 'published_at'],
            delay=0.5,
            row_limit=1000
        )
    producer.close()
    logging.getLogger("dataset_streamer").info("Kafka producer connection closed.")


if __name__ == "__main__":
    main()