#!/usr/bin/env python3
import argparse
import time
import logging
import os
import random
import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, length, split
from pyspark.sql.functions import size as spark_size
from pyspark.sql.types import FloatType, StringType, StructType, StructField

from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

# Try to import Kafka libraries but don't fail if they're not available
try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
    kafka_available = True
except ImportError:
    kafka_available = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('load_testing.log')
    ]
)
logger = logging.getLogger(__name__)

# Initialize NLTK
try:
    nltk.data.find('vader_lexicon')
except LookupError:
    nltk.download('vader_lexicon')

# Constants
KAFKA_TOPIC = "airline_review_load_test"
DEFAULT_KAFKA_BROKER = "localhost:9092"

def generate_synthetic_data(size=1000):
    """Generate synthetic airline review data for testing"""
    logger.info(f"Generating {size} synthetic airline review records")
    airlines = ['IndiGo', 'Air India', 'SpiceJet', 'Vistara', 'GoAir']
    titles = ['Good Experience', 'Bad Experience', 'Average Flight', 'Delayed Flight', 'Excellent Service']
    data = []
    
    for i in range(size):
        airline = random.choice(airlines)
        rating = random.randint(1, 10)
        
        # Create more realistic reviews based on rating
        if rating >= 7:
            review = f"I had a great experience with {airline}. The flight was on time and the staff was friendly."
        elif rating >= 4:
            review = f"My flight with {airline} was okay. Some delays but overall acceptable service."
        else:
            review = f"I had a terrible experience with {airline}. The flight was delayed and staff was unhelpful."
            
        data.append({
            'AirLine_Name': airline,
            'Rating - 10': rating,
            'Title': random.choice(titles),
            'Name': f"Customer {i}",
            'Date': '2023-01-01',
            'Review': review,
            'Recommond': 'Yes' if rating > 5 else 'No',
            'timestamp': time.time()
        })
    
    return pd.DataFrame(data)

def setup_kafka():
    """Set up Kafka topic for streaming tests if it doesn't exist"""
    if not kafka_available:
        logger.error("Kafka libraries not available. Cannot run streaming tests.")
        return False
        
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=DEFAULT_KAFKA_BROKER,
            client_id='load_test_admin'
        )
        
        # Check if topic exists, create if it doesn't
        try:
            topics = admin_client.list_topics()
            if KAFKA_TOPIC not in topics:
                logger.info(f"Creating Kafka topic: {KAFKA_TOPIC}")
                topic_list = [NewTopic(
                    name=KAFKA_TOPIC,
                    num_partitions=4,
                    replication_factor=1
                )]
                admin_client.create_topics(new_topics=topic_list)
        except Exception as e:
            logger.warning(f"Error checking/creating topics: {e}")
        
        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=DEFAULT_KAFKA_BROKER,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        return producer
    except Exception as e:
        logger.error(f"Failed to setup Kafka: {e}")
        return None

def send_to_kafka(producer, data, batch_size=100, delay=0.001):
    """Send data to Kafka in batches"""
    if producer is None or not kafka_available:
        return False
        
    total_sent = 0
    start_time = time.time()
    
    try:
        for i, row in enumerate(data.to_dict('records')):
            producer.send(KAFKA_TOPIC, value=row)
            total_sent += 1
            
            # Send in batches and add small delay to prevent overwhelming
            if i % batch_size == 0:
                producer.flush()
                time.sleep(delay)
        
        # Final flush
        producer.flush()
        elapsed = time.time() - start_time
        logger.info(f"Sent {total_sent} records to Kafka in {elapsed:.2f} seconds")
        return True
    except Exception as e:
        logger.error(f"Error sending to Kafka: {e}")
        return False

def process_with_threads(worker_count, data):
    """Process data using Python ThreadPoolExecutor"""
    logger.info(f"Processing with {worker_count} threads")
    
    # Initialize sentiment analyzer
    sia = SentimentIntensityAnalyzer()
    
    # Function to process a single record
    def process_record(record):
        review = str(record['Review'])
        result = {
            'airline': record['AirLine_Name'],
            'rating': record['Rating - 10'],
            'word_count': len(review.split()),
            'sentiment': sia.polarity_scores(review)['compound'],
            'text_length': len(review)
        }
        return result
    
    # Split data into chunks for each worker
    records = data.to_dict('records')
    chunk_size = max(1, len(records) // worker_count)
    chunks = [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]
    
    # Process data with ThreadPoolExecutor
    start_time = time.time()
    results = []
    
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        # Map each chunk to a worker
        futures = [executor.submit(lambda chunk: [process_record(r) for r in chunk], chunk) 
                  for chunk in chunks]
        
        # Collect results
        for future in futures:
            results.extend(future.result())
    
    elapsed = time.time() - start_time
    
    return pd.DataFrame(results), elapsed

def process_with_spark(worker_count, data):
    """Process data using Spark with specified number of workers"""
    logger.info(f"Processing with Spark using {worker_count} workers")
    
    # Initialize Spark with the specified number of workers
    spark = SparkSession.builder \
        .appName("LoadTestingSpark") \
        .config("spark.sql.shuffle.partitions", str(worker_count)) \
        .config("spark.default.parallelism", str(worker_count)) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Convert data to Spark DataFrame
    data_records = data.to_dict('records')
    spark_df = spark.createDataFrame(data_records)
    
    # Register UDF for sentiment analysis
    @udf(FloatType())
    def get_sentiment_udf(text):
        sia = SentimentIntensityAnalyzer()
        return float(sia.polarity_scores(str(text))['compound'])
    
    # Process with Spark
    start_time = time.time()
    
    result_df = spark_df.withColumn('word_count', spark_size(split(col('Review'), ' '))) \
             .withColumn('sentiment', get_sentiment_udf(col('Review'))) \
             .withColumn('text_length', length(col('Review'))) \
             .select('AirLine_Name', 'Rating - 10', 'word_count', 'sentiment', 'text_length')
    
    # Force computation
    result_count = result_df.count()
    elapsed = time.time() - start_time
    
    # Convert back to pandas for consistent return
    pandas_result = result_df.toPandas()
    
    # Clean up
    spark.stop()
    
    return pandas_result, elapsed

def consume_from_kafka(worker_count, data_size, timeout=60):
    """Consume and process messages from Kafka"""
    if not kafka_available:
        logger.error("Kafka libraries not available. Cannot consume from Kafka.")
        return None, 0
    
    try:
        # Create consumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=DEFAULT_KAFKA_BROKER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=f'load_test_group_{int(time.time())}',  # Unique group ID
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Initialize sentiment analyzer
        sia = SentimentIntensityAnalyzer()
        
        # Process messages with ThreadPoolExecutor
        start_time = time.time()
        message_count = 0
        results = []
        
        # Process messages until we've processed enough or timed out
        logger.info(f"Starting to consume messages, expecting {data_size} records")
        
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = []
            
            # Function to process a single message
            def process_message(message):
                record = message.value
                review = str(record.get('Review', ''))
                result = {
                    'airline': record.get('AirLine_Name', ''),
                    'rating': record.get('Rating - 10', 0),
                    'word_count': len(review.split()),
                    'sentiment': sia.polarity_scores(review)['compound'],
                    'text_length': len(review)
                }
                return result
            
            # Poll for messages until we get enough or timeout
            while message_count < data_size and (time.time() - start_time) < timeout:
                # Poll for messages with a short timeout
                messages = consumer.poll(timeout_ms=1000, max_records=min(500, data_size - message_count))
                
                if not messages:
                    continue
                
                # Process received messages
                for tp, msgs in messages.items():
                    for message in msgs:
                        futures.append(executor.submit(process_message, message))
                        message_count += 1
                        
                        # Break if we have enough messages
                        if message_count >= data_size:
                            break
                    if message_count >= data_size:
                        break
                
                logger.info(f"Consumed {message_count}/{data_size} messages...")
            
            # Collect results from futures
            for future in futures:
                results.append(future.result())
        
        elapsed = time.time() - start_time
        logger.info(f"Processed {message_count} messages in {elapsed:.2f} seconds")
        
        # Close consumer
        consumer.close()
        
        return pd.DataFrame(results), elapsed
    
    except Exception as e:
        logger.error(f"Error consuming from Kafka: {e}")
        return None, 0

def run_stream_test(data_sizes, worker_counts):
    """Run streaming mode tests"""
    if not kafka_available:
        logger.error("Kafka libraries not available. Cannot run streaming tests.")
        return None
    
    results = []
    
    # Setup Kafka
    producer = setup_kafka()
    if producer is None:
        return None
    
    for data_size in data_sizes:
        # Generate test data
        data = generate_synthetic_data(data_size)
        
        # Send data to Kafka
        logger.info(f"Sending {data_size} records to Kafka")
        if not send_to_kafka(producer, data):
            continue
        
        for worker_count in worker_counts:
            logger.info(f"Testing with {worker_count} workers on {data_size} records")
            
            # Consume and process from Kafka
            result_df, elapsed = consume_from_kafka(worker_count, data_size)
            
            if result_df is not None:
                throughput = data_size / elapsed if elapsed > 0 else 0
                
                results.append({
                    'mode': 'stream',
                    'data_size': data_size,
                    'worker_count': worker_count,
                    'elapsed_time': elapsed,
                    'throughput': throughput
                })
                
                logger.info(f"Stream test: {data_size} records, {worker_count} workers, {elapsed:.2f} seconds, {throughput:.2f} records/sec")
    
    # Close producer
    if producer:
        producer.close()
    
    return pd.DataFrame(results) if results else None

def run_batch_test(data_sizes, worker_counts):
    """Run batch mode tests"""
    results = []
    
    for data_size in data_sizes:
        # Generate test data
        data = generate_synthetic_data(data_size)
        
        for worker_count in worker_counts:
            logger.info(f"Testing with {worker_count} workers on {data_size} records")
            
            # Process with threads
            thread_result, thread_elapsed = process_with_threads(worker_count, data)
            thread_throughput = data_size / thread_elapsed if thread_elapsed > 0 else 0
            
            results.append({
                'mode': 'batch-threads',
                'data_size': data_size,
                'worker_count': worker_count,
                'elapsed_time': thread_elapsed,
                'throughput': thread_throughput
            })
            
            logger.info(f"Batch-threads test: {data_size} records, {worker_count} workers, {thread_elapsed:.2f} seconds, {thread_throughput:.2f} records/sec")
            
            # Process with Spark
            spark_result, spark_elapsed = process_with_spark(worker_count, data)
            spark_throughput = data_size / spark_elapsed if spark_elapsed > 0 else 0
            
            results.append({
                'mode': 'batch-spark',
                'data_size': data_size,
                'worker_count': worker_count,
                'elapsed_time': spark_elapsed,
                'throughput': spark_throughput
            })
            
            logger.info(f"Batch-spark test: {data_size} records, {worker_count} workers, {spark_elapsed:.2f} seconds, {spark_throughput:.2f} records/sec")
    
    return pd.DataFrame(results)

def visualize_results(results):
    """Create visualizations from test results"""
    if results is None or len(results) == 0:
        logger.error("No results to visualize")
        return
    
    # Create directory for results
    os.makedirs('load_test_results', exist_ok=True)
    
    # Save raw results
    results.to_csv('load_test_results/load_test_results.csv', index=False)
    
    # Create plot figures
    plt.figure(figsize=(15, 10))
    
    # 1. Execution time by worker count
    plt.subplot(2, 2, 1)
    for mode in results['mode'].unique():
        for size in results['data_size'].unique():
            df_subset = results[(results['mode'] == mode) & (results['data_size'] == size)]
            if not df_subset.empty:
                plt.plot(
                    df_subset['worker_count'], 
                    df_subset['elapsed_time'], 
                    marker='o', 
                    label=f"{mode} - {size} records"
                )
    
    plt.xlabel('Worker Count')
    plt.ylabel('Execution Time (s)')
    plt.title('Execution Time vs Worker Count')
    plt.grid(True)
    plt.legend()
    
    # 2. Throughput by worker count
    plt.subplot(2, 2, 2)
    for mode in results['mode'].unique():
        for size in results['data_size'].unique():
            df_subset = results[(results['mode'] == mode) & (results['data_size'] == size)]
            if not df_subset.empty:
                plt.plot(
                    df_subset['worker_count'], 
                    df_subset['throughput'], 
                    marker='o', 
                    label=f"{mode} - {size} records"
                )
    
    plt.xlabel('Worker Count')
    plt.ylabel('Throughput (records/sec)')
    plt.title('Throughput vs Worker Count')
    plt.grid(True)
    plt.legend()
    
    # 3. Scalability (speedup)
    plt.subplot(2, 2, 3)
    for mode in results['mode'].unique():
        for size in results['data_size'].unique():
            df_subset = results[(results['mode'] == mode) & (results['data_size'] == size)]
            if not df_subset.empty and len(df_subset) > 1:
                # Calculate speedup relative to single worker
                baseline = df_subset[df_subset['worker_count'] == min(df_subset['worker_count'])]['elapsed_time'].values[0]
                speedups = [baseline / t for t in df_subset['elapsed_time']]
                plt.plot(
                    df_subset['worker_count'], 
                    speedups, 
                    marker='o', 
                    label=f"{mode} - {size} records"
                )
    
    # Add ideal speedup line
    max_workers = results['worker_count'].max()
    plt.plot([1, max_workers], [1, max_workers], 'k--', label='Ideal Speedup')
    
    plt.xlabel('Worker Count')
    plt.ylabel('Speedup')
    plt.title('Scalability: Speedup vs Worker Count')
    plt.grid(True)
    plt.legend()
    
    # 4. Efficiency
    plt.subplot(2, 2, 4)
    for mode in results['mode'].unique():
        for size in results['data_size'].unique():
            df_subset = results[(results['mode'] == mode) & (results['data_size'] == size)]
            if not df_subset.empty and len(df_subset) > 1:
                # Calculate efficiency (speedup / worker count)
                baseline = df_subset[df_subset['worker_count'] == min(df_subset['worker_count'])]['elapsed_time'].values[0]
                efficiencies = [(baseline / t) / w * 100 for t, w in zip(df_subset['elapsed_time'], df_subset['worker_count'])]
                plt.plot(
                    df_subset['worker_count'], 
                    efficiencies, 
                    marker='o', 
                    label=f"{mode} - {size} records"
                )
    
    plt.axhline(y=100, color='k', linestyle='--', label='Ideal Efficiency (100%)')
    plt.xlabel('Worker Count')
    plt.ylabel('Efficiency (%)')
    plt.title('Parallel Efficiency vs Worker Count')
    plt.grid(True)
    plt.legend()
    
    plt.tight_layout()
    plt.savefig('load_test_results/load_test_summary.png', dpi=300, bbox_inches='tight')
    
    # Only show plot in interactive mode
    if 'ipykernel' in sys.modules:
        plt.show()
    else:
        plt.close()
    
    # Create separate plots for each mode and data size
    for mode in results['mode'].unique():
        plt.figure(figsize=(12, 6))
        
        for size in results['data_size'].unique():
            df_subset = results[(results['mode'] == mode) & (results['data_size'] == size)]
            if not df_subset.empty:
                plt.plot(
                    df_subset['worker_count'], 
                    df_subset['throughput'], 
                    marker='o', 
                    linewidth=2,
                    label=f"{size} records"
                )
        
        plt.xlabel('Worker Count')
        plt.ylabel('Throughput (records/sec)')
        plt.title(f'Throughput vs Worker Count for {mode}')
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig(f'load_test_results/throughput_{mode}.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    logger.info("Visualizations saved to load_test_results directory")

def parse_arguments():
    parser = argparse.ArgumentParser(description='Load testing script for data processing')
    parser.add_argument('--mode', choices=['stream', 'batch', 'both'], default='both',
                       help='Processing mode: stream, batch, or both')
    parser.add_argument('--data-sizes', type=str, default='1000,5000,10000',
                       help='Comma-separated list of data sizes to test')
    parser.add_argument('--worker-counts', type=str, default='1,2,4,8',
                       help='Comma-separated list of worker counts to test')
    
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    # Parse arguments
    data_sizes = [int(x) for x in args.data_sizes.split(',')]
    worker_counts = [int(x) for x in args.worker_counts.split(',')]
    
    logger.info(f"Running load tests with:")
    logger.info(f"  Mode: {args.mode}")
    logger.info(f"  Data sizes: {data_sizes}")
    logger.info(f"  Worker counts: {worker_counts}")
    
    all_results = []
    
    # Run tests based on mode
    if args.mode in ['batch', 'both']:
        logger.info("Running batch mode tests")
        batch_results = run_batch_test(data_sizes, worker_counts)
        if batch_results is not None:
            all_results.append(batch_results)
    
    if args.mode in ['stream', 'both']:
        if kafka_available:
            logger.info("Running stream mode tests")
            stream_results = run_stream_test(data_sizes, worker_counts)
            if stream_results is not None:
                all_results.append(stream_results)
        else:
            logger.error("Kafka libraries not available. Skipping stream tests.")
    
    # Combine results and visualize
    if all_results:
        combined_results = pd.concat(all_results, ignore_index=True)
        visualize_results(combined_results)
        
        # Print summary
        print("\nLoad Testing Results Summary:")
        print(combined_results.to_string(index=False))
    else:
        logger.error("No test results were generated")

if __name__ == "__main__":
    main()
