import os
import sys
import time

# Point Spark to the Python executable in the current virtual environment
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, split, explode, length, avg, count, desc, udf
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import logging

# Configure logging
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

# The NLTK data is now expected to be pre-downloaded by running setup_nltk.py
# We no longer handle the download within the application code.

# UDF for sentiment analysis
@udf(FloatType())
def get_sentiment(text):
    # Lazy initialization of the sentiment analyzer on the worker.
    # It assumes the vader_lexicon is already available in the NLTK data path.
    if not hasattr(get_sentiment, "sia"):
        get_sentiment.sia = SentimentIntensityAnalyzer()
    
    try:
        if not isinstance(text, str) or len(text.strip()) == 0:
            return 0.0
        return float(get_sentiment.sia.polarity_scores(str(text))['compound'])
    except Exception as e:
        logger.error(f"Error in sentiment UDF: {e}")
        return 0.0

def process_unified_batch(df, epoch_id):
    """
    A single, unified function to process each micro-batch of data. This fulfills the
    requirement of using a multiprocessing library (Spark) for batch processing.
    """
    start_time = time.time()
    print(f"\n--- Processing Batch {epoch_id} ---")

    df.persist()
    record_count = df.count()

    if record_count == 0:
        print("Status: No new data in this batch. Waiting for next trigger...")
        df.unpersist()
        return

    print(f"Status: Received {record_count} new records. Starting analysis...")

    # --- Sentiment Analysis (MapReduce Implemented) ---
    print("\n=== Sentiment Analysis Summary ===")
    sentiment_df = df.withColumn("sentiment", get_sentiment(col("text")))
    sentiment_summary = sentiment_df.agg(
        avg("sentiment").alias("average_sentiment"),
        count("*").alias("tweet_count")
    )
    sentiment_summary.show()

    # --- Word Count & Hashtag Trends (MapReduce Implemented) ---
    words_df = df.select(explode(split(col("text"), " ")).alias("word")) \
                 .filter(length(col("word")) > 1)
    words_df.persist()

    print("\n=== Top 5 Trending Words ===")
    word_counts = words_df.groupBy("word").count().orderBy(desc("count"))
    word_counts.show(5, truncate=False)

    print("\n=== Top 5 Trending Hashtags ===")
    hashtag_counts = words_df.filter(col("word").startswith("#")) \
                             .groupBy("word").count().orderBy(desc("count"))
    hashtag_counts.show(5, truncate=False)

    df.unpersist()
    words_df.unpersist()
    
    end_time = time.time()
    print(f"--- Batch {epoch_id} processing finished in {end_time - start_time:.2f} seconds ---")

def create_spark_session():
    """Create a resource-constrained and stable Spark session."""
    return SparkSession.builder \
        .appName("StableKafkaConsumer") \
        .master("local[2]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()

def main():
    spark = None
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("ERROR")

        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "text_data") \
            .option("startingOffsets", "earliest") \
            .load()

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("text", StringType(), True)
        ])
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        query = parsed_df.writeStream \
            .foreachBatch(process_unified_batch) \
            .trigger(processingTime='15 seconds') \
            .option("checkpointLocation", "final_checkpoint") \
            .start()

        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Shutdown requested by user.")
    except Exception as e:
        logger.error(f"An error occurred in the main application: {e}", exc_info=True)
    finally:
        if spark:
            print("\nShutting down Spark session...")
            spark.stop()
            print("Spark session stopped successfully.")

if __name__ == "__main__":
    main() 