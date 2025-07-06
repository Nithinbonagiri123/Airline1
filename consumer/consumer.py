import os
import sys
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json as spark_from_json,
    col as spark_col,
    explode as spark_explode,
    split as spark_split,
    avg as spark_avg,
    length as spark_length,
    desc as spark_desc,
    count as spark_count,
    current_timestamp as spark_now,
    udf as spark_udf,
    lower as spark_lower,
    window as spark_window,
)
from pyspark.sql.types import (
    StructType as SparkStructType,
    StructField as SparkStructField,
    StringType as SparkStringType,
    IntegerType as SparkIntType,
    FloatType as SparkFloatType,
    BooleanType as SparkBoolType,
    DoubleType as SparkDoubleType,
)
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from .s3_upload_helper import upload_to_s3

# Configure Spark environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 pyspark-shell"

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_stream_consumer")

# Prepare stopword set for filtering
STOPWORDS = set(stopwords.words("english"))

def is_not_stopword(word):
    """True if word is not a stopword."""
    return word not in STOPWORDS

remove_stopwords_udf = spark_udf(is_not_stopword, SparkBoolType())

# Sentiment analysis UDF
def compute_sentiment(text):
    if not hasattr(compute_sentiment, "analyzer"):
        compute_sentiment.analyzer = SentimentIntensityAnalyzer()
    try:
        if isinstance(text, str) and text.strip():
            return float(compute_sentiment.analyzer.polarity_scores(text)["compound"])
        return 0.0
    except Exception as exc:
        logger.warning(f"Sentiment UDF error: {exc}")
        return 0.0

sentiment_udf = spark_udf(compute_sentiment, SparkFloatType())

def batch_analysis(df, batch_num):
    """
    Analyze a batch of records for sentiment and trending words.
    """
    start_time = time.time()
    logger.info(f"[Batch {batch_num}] Analysis started.")
    df.persist()
    count = df.count()
    if count < 1:
        logger.info("Batch is empty. Skipping...")
        df.unpersist()
        return
    logger.info(f"Processing {count} records...")
    # Calculate latency if timestamp present
    avg_latency = None
    if "timestamp" in df.columns:
        latency_df = df.withColumn("latency", (time.time() - spark_col("timestamp")))
        avg_latency = latency_df.agg(spark_avg("latency")).collect()[0][0]
        logger.info(f"Average latency: {avg_latency:.2f} s")
    # Sentiment analysis
    if "content" in df.columns:
        df = df.withColumn("sentiment", sentiment_udf(spark_col("content")))
        avg_sentiment = df.agg(spark_avg("sentiment")).collect()[0][0]
        logger.info(f"Mean sentiment: {avg_sentiment:.3f}")
    # Trending terms
    if "content" in df.columns:
        words_df = df.select(spark_explode(spark_split(spark_lower(spark_col("content")), "\\W+")).alias("word"))
        words_df = words_df.filter(remove_stopwords_udf(spark_col("word")))
        top_words = words_df.groupBy("word").count().orderBy(spark_desc("count")).limit(10).collect()
        logger.info("Top 10 words: " + ", ".join(f"{row['word']}({row['count']})" for row in top_words))
    # Save batch to S3 (optional, can be customized)
    output_path = f"batch_{batch_num}_results.csv"
    df.toPandas().to_csv(output_path, index=False)
    upload_to_s3(output_path, os.environ.get("S3_BUCKET", "default-bucket"), output_path)
    df.unpersist()
    logger.info(f"[Batch {batch_num}] Analysis complete in {time.time() - start_time:.2f}s.")

    # Sentiment analysis on article content
    print("\n>>> Sentiment overview <<<")
    sentiment_results = latency_df.withColumn(
        "sentiment_score", sentiment_udf(select_col("content"))
    )
    sentiment_stats = sentiment_results.agg(
        mean("sentiment_score").alias("mean_sentiment"),
        total_count("*").alias("article_count"),
    )
    sentiment_stats.show()

    # Trending words (excluding stopwords)
    print("\n>>> Top Keywords <<<")
    words_flat = latency_df.select(
        flatten(split_text(to_lower(select_col("content")), " ")).alias("keyword")
    ).filter(str_length(select_col("keyword")) > 1)
    keywords_filtered = words_flat.filter(remove_stopwords_udf(select_col("keyword")))
    keyword_counts = (
        keywords_filtered.groupBy("keyword").count().orderBy(descending("count"))
    )
    keyword_counts.show(5, truncate=False)

    # Trending hashtags
    print("\n>>> Top Hashtags <<<")
    hashtags = (
        words_flat.filter(select_col("keyword").startswith("#"))
        .groupBy("keyword")
        .count()
        .orderBy(descending("count"))
    )
    hashtags.show(5, truncate=False)

    # 5-min window trending terms
    enriched_df = latency_df.withColumn("proc_time", now_ts())
    words_with_time = enriched_df.select(
        flatten(split_text(to_lower(select_col("content")), " ")).alias("keyword"),
        select_col("proc_time"),
    ).filter(str_length(select_col("keyword")) > 1)
    windowed_keywords = (
        words_with_time.filter(remove_stopwords_udf(select_col("keyword")))
        .groupBy(
            time_window(select_col("proc_time"), "5 minutes", "5 minutes"),
            select_col("keyword"),
        )
        .count()
        .orderBy(descending("count"))
    )
    print("\n>>> 5-min Window Top Words <<<")
    windowed_keywords.show(5, truncate=False)

    batch_df.unpersist()
    elapsed = _time.time() - start
    throughput = num_records / elapsed if elapsed > 0 else 0
    print(f"Throughput: {throughput:.2f} articles/sec")

    # CSV logging (refactored)
    metrics_file = "news_metrics.csv"
    write_header = (
        not os.path.exists(metrics_file) or os.path.getsize(metrics_file) == 0
    )
    with open(metrics_file, "a", newline="") as out_csv:
        writer = csv.writer(out_csv)
        if write_header:
            writer.writerow(
                ["batch_idx", "num_records", "elapsed", "throughput", "avg_delay"]
            )
        writer.writerow([batch_idx, num_records, elapsed, throughput, avg_delay])

    # S3 upload (refactored)
    s3_upload(metrics_file, "news-analytics-bucket", "metrics/news_metrics.csv")
    print(f"### Batch {batch_idx} complete ({elapsed:.2f} s) ###")


def build_spark_session():
    """Instantiate a Spark session with resource constraints."""
    return (
        SparkSession.builder.appName("NewsArticleStreamConsumer")
        .master("local[2]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )


def main_stream():
    """
    Main entrypoint for the news article streaming consumer.
    Reads from Kafka, parses news article records, and launches the analysis pipeline.
    """
    spark = None
    try:
        spark = build_spark_session()
        spark.sparkContext.setLogLevel("ERROR")
        # Kafka connection (topic and server name refactored for news context)
        kafka_broker = os.environ.get("KAFKA_NEWS_SERVERS", "localhost:9092")
        log.info(f"Connecting to Kafka at {kafka_broker}")
        news_topic = "news_articles_stream"
        # Define schema for news articles
        news_schema = SchemaType(
            [
                SchemaField("article_id", IntCol(), True),
                SchemaField("title", StringCol(), True),
                SchemaField("content", StringCol(), True),
                SchemaField("author", StringCol(), True),
                SchemaField("published_at", DoubleCol(), True),
            ]
        )
        kafka_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_broker)
            .option("subscribe", news_topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 50)
            .load()
        )
        parsed_articles = kafka_stream.select(
            parse_json(select_col("value").cast("string"), news_schema).alias("article")
        ).select("article.*")
        # Start streaming analysis
        query = (
            parsed_articles.writeStream.foreachBatch(analyze_article_batch)
            .trigger(processingTime="15 seconds")
            .option("checkpointLocation", "news_stream_checkpoint")
            .start()
        )
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Shutdown requested by user.")
    except Exception as err:
        log.error(f"Fatal error in stream: {err}", exc_info=True)
    finally:
        if spark:
            print("\nClosing Spark session...")
            spark.stop()


if __name__ == "__main__":
    main_stream()
