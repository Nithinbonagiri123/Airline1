import time
import pandas as pd
import matplotlib.pyplot as plt
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, length, split
from pyspark.sql.functions import size as spark_size
from pyspark.sql.types import FloatType
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import random
import os
import kagglehub

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('airline_performance_comparison.log')
    ]
)
logger = logging.getLogger(__name__)

# Initialize NLTK
try:
    nltk.data.find('vader_lexicon')
except LookupError:
    nltk.download('vader_lexicon')

def load_kaggle_dataset():
    """Load the Kaggle airline customer review dataset used in the producer"""
    try:
        logger.info("Downloading Kaggle dataset...")
        path = kagglehub.dataset_download("jagathratchakan/indian-airlines-customer-reviews")
        csv_path = os.path.join(path, "Indian_Domestic_Airline.csv")
        
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Clean the data - drop rows with missing review text
        df = df.dropna(subset=['Review'])
        
        logger.info(f"Loaded {len(df)} reviews from Kaggle dataset")
        return df
    except Exception as e:
        logger.error(f"Error loading Kaggle dataset: {str(e)}")
        raise

def get_sample_data(size=1000):
    """Get sample airline review data from the Kaggle dataset with optimized loading"""
    try:
        # Try to load the full dataset from Kaggle
        try:
            df = load_kaggle_dataset()
            
            # If requested size is larger than available, use all and log a warning
            if size > len(df):
                logger.warning(f"Requested size {size} is larger than dataset size {len(df)}. Using all available reviews.")
                return df
            
            # For very large samples, use a more memory-efficient approach
            if size > 10000:
                logger.info(f"Sampling {size} reviews (large sample, this might take a moment)...")
                # Use numpy for faster random sampling with large datasets
                import numpy as np
                sample_indices = np.random.choice(len(df), size=size, replace=False)
                return df.iloc[sample_indices]
            else:
                # For smaller samples, use pandas sample
                return df.sample(n=size, random_state=42)
        except Exception as kaggle_err:
            logger.warning(f"Could not download Kaggle dataset: {kaggle_err}")
            # Fall through to the manual data creation below
            raise ValueError("Kaggle dataset unavailable")
            
    except Exception as e:
        logger.warning(f"Creating synthetic airline review data instead: {str(e)}")
        # Fallback to generating airline review data
        logger.info("Generating synthetic airline review data")
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
                'Recommond': 'Yes' if rating > 5 else 'No'
            })
        return pd.DataFrame(data)

# Sequential processing
def process_sequential(df):
    """Process airline reviews sequentially"""
    sia = SentimentIntensityAnalyzer()
    results = []
    
    for _, row in df.iterrows():
        review = str(row['Review'])
        
        # Word count
        word_count = len(review.split())
        
        # Sentiment analysis
        sentiment = sia.polarity_scores(review)['compound']
        
        # Text length
        text_length = len(review)
        
        results.append({
            'airline': row['AirLine_Name'],
            'rating': row['Rating - 10'],
            'word_count': word_count,
            'sentiment': sentiment,
            'text_length': text_length
        })
    
    return pd.DataFrame(results)

# Parallel processing with Spark
def process_parallel(spark, df):
    """Process airline reviews in parallel using Spark"""
    # Convert pandas DataFrame to Spark DataFrame - using a method compatible with all pandas versions
    
    # Register UDF for sentiment analysis
    @udf(FloatType())
    def get_sentiment_udf(text):
        sia = SentimentIntensityAnalyzer()
        return float(sia.polarity_scores(str(text))['compound'])
    
    # Process in parallel
    # Convert the DataFrame to a list of rows for compatibility with all pandas versions
    data_list = df.to_dict('records')
    spark_df = spark.createDataFrame(data_list)
    
    result_df = spark_df.withColumn('word_count', spark_size(split(col('Review'), ' '))) \
                 .withColumn('sentiment', get_sentiment_udf(col('Review'))) \
                 .withColumn('text_length', length(col('Review'))) \
                 .select('AirLine_Name', 'Rating - 10', 'word_count', 'sentiment', 'text_length')
    
    return result_df

def run_comparison():
    # Sample sizes to test 
    sample_sizes = [100, 500, 1000, 2000, 5000, 10000, 15000]
    
    # Initialize Spark with optimized configurations for larger datasets
    spark = SparkSession.builder \
        .appName("AirlineReviewPerformanceComparison") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.default.parallelism", "8") \
        .getOrCreate()
        
    # Set log level to WARN to reduce output noise
    spark.sparkContext.setLogLevel("WARN")
    
    results = []
    
    for size in sample_sizes:
        print(f"\nProcessing sample size: {size}")
        df = get_sample_data(size)
        
        # Sequential processing
        start_time = time.time()
        seq_result = process_sequential(df)  # Process and store the result
        seq_time = time.time() - start_time
        
        # Parallel processing
        start_time = time.time()
        par_df = process_parallel(spark, df)
        # Force execution of the Spark job
        par_df.count()  # This ensures the computation is actually performed
        par_time = time.time() - start_time
        
        # Calculate speedup
        speedup = seq_time / par_time if par_time > 0 else 0
        
        results.append({
            'sample_size': size,
            'sequential_time': seq_time,
            'parallel_time': par_time,
            'speedup': speedup
        })
        
        print(f"Sample size: {size}")
        print(f"Sequential time: {seq_time:.2f} seconds")
        print(f"Parallel time: {par_time:.2f} seconds")
        print(f"Speedup: {speedup:.2f}x")
        
        # Additional analysis: calculate average sentiment for sequential and parallel
        avg_sentiment_seq = seq_result['sentiment'].mean()
        avg_sentiment_par = par_df.agg({'sentiment': 'mean'}).collect()[0][0]
        
        print(f"Avg sentiment (sequential): {avg_sentiment_seq:.3f}")
        print(f"Avg sentiment (parallel): {avg_sentiment_par:.3f}")
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    
    # Save results to CSV
    results_df.to_csv('airline_performance_comparison.csv', index=False)
    
    # Enhanced visualization
    plt.figure(figsize=(12, 8))
    
    # 1. Execution Time Comparison
    plt.subplot(2, 2, 1)
    plt.plot(results_df['sample_size'], results_df['sequential_time'], 'b-o', label='Sequential')
    plt.plot(results_df['sample_size'], results_df['parallel_time'], 'r-s', label='Parallel')
    plt.xlabel('Number of Airline Reviews')
    plt.ylabel('Execution Time (s)')
    plt.title('Execution Time Comparison')
    plt.legend()
    plt.grid(True)
    
    # 2. Speedup
    plt.subplot(2, 2, 2)
    plt.plot(results_df['sample_size'], results_df['speedup'], 'g-s', label='Speedup')
    plt.axhline(y=1, color='r', linestyle='--')
    plt.title('Speedup (Sequential / Parallel)')
    plt.xlabel('Number of Airline Reviews')
    plt.ylabel('Speedup (x)')
    plt.grid(True)
    
    # 3. Throughput Comparison (reviews/second)
    plt.subplot(2, 2, 3)
    results_df['seq_throughput'] = results_df['sample_size'] / results_df['sequential_time']
    results_df['par_throughput'] = results_df['sample_size'] / results_df['parallel_time']
    plt.plot(results_df['sample_size'], results_df['seq_throughput'], 'b-o', label='Sequential')
    plt.plot(results_df['sample_size'], results_df['par_throughput'], 'r-s', label='Parallel')
    plt.title('Throughput Comparison')
    plt.xlabel('Number of Airline Reviews')
    plt.ylabel('Reviews Processed per Second')
    plt.legend()
    plt.grid(True)
    
    # 4. Efficiency (Speedup / Number of Cores)
    plt.subplot(2, 2, 4)
    num_cores = spark.sparkContext.defaultParallelism
    results_df['efficiency'] = (results_df['speedup'] / num_cores) * 100
    plt.plot(results_df['sample_size'], results_df['efficiency'], 'm-D', label='Efficiency')
    plt.axhline(y=100, color='r', linestyle='--', label='Ideal (100%)')
    plt.title(f'Parallel Efficiency (Using {num_cores} Cores)')
    plt.xlabel('Number of Airline Reviews')
    plt.ylabel('Efficiency (%)')
    plt.legend()
    plt.grid(True)
    
    plt.tight_layout()
    
    # Save high-resolution figure
    plt.savefig('airline_performance_comparison.png', dpi=300, bbox_inches='tight')
    
    # Also save a CSV with detailed metrics
    results_df.to_csv('airline_performance_metrics.csv', index=False)
    
    # Log completion
    logger.info("Performance testing completed. Results saved to 'airline_performance_comparison.png' and 'airline_performance_metrics.csv'")
    
    # Only show plot if running interactively
    import sys
    if 'ipykernel' in sys.modules:
        plt.show()
    
    spark.stop()
    return results_df

if __name__ == "__main__":
    results = run_comparison()
    print("\nAirline Review Performance Comparison Results:")
    print(results.to_string())
