import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json
import kagglehub
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_producer():
    retries = 12
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Kafka producer created successfully.")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka broker not available. Retrying in 5 seconds... ({i+1}/{retries})")
            time.sleep(5)
    logger.error("Failed to connect to Kafka broker after multiple retries.")
    return None

def stream_data(producer, topic, csv_file):
    try:
        df = pd.read_csv(csv_file)
        
        # Ensure 'clean_text' column exists and handle missing values
        if 'clean_text' not in df.columns:
            logger.error("Error: 'clean_text' column not found in the CSV.")
            return
        
        df = df.dropna(subset=['clean_text'])
        df = df.head(150)  # Increased sample size slightly

        for index, row in df.iterrows():
            # Create a message that matches the consumer's schema
            message = {
                "id": index,
                "text": row['clean_text']
            }
            producer.send(topic, value=message)
            logger.info(f"Sent: {message}")
            time.sleep(0.5) # Reduced sleep time to speed up streaming
            
    except FileNotFoundError:
        logger.error(f"Error: The file {csv_file} was not found.")
    except Exception as e:
        logger.error(f"An error occurred during data streaming: {e}")

if __name__ == "__main__":
    kafka_producer = create_producer()
    if kafka_producer:
        kafka_topic = 'text_data'
        try:
            path = kagglehub.dataset_download("saurabhshahane/twitter-sentiment-dataset")
            csv_path = os.path.join(path, "Twitter_Data.csv")
            stream_data(kafka_producer, kafka_topic, csv_path)
        except Exception as e:
            logger.error(f"Failed to download or process dataset: {e}")
        finally:
            kafka_producer.close()
            logger.info("Kafka producer closed.") 