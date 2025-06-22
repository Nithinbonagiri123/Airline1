import os
import sys
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json

def test_kafka_connection():
    """Test if we can connect to Kafka."""
    print("Testing Kafka connection...")
    retries = 5
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✓ Successfully connected to Kafka")
            producer.close()
            return True
        except NoBrokersAvailable:
            if i < retries - 1:
                print(f"Kafka not available, retrying in 5 seconds... ({i+1}/{retries})")
                time.sleep(5)
            else:
                print("✗ Failed to connect to Kafka")
                return False

def test_python_packages():
    """Test if required Python packages are installed."""
    print("\nChecking required Python packages...")
    required_packages = [
        'pyspark',
        'nltk',
        'pandas',
        'matplotlib',
        'seaborn',
        'kafka'
    ]
    
    all_passed = True
    for package in required_packages:
        try:
            __import__(package)
            print(f"✓ {package} is installed")
        except ImportError:
            print(f"✗ {package} is NOT installed")
            all_passed = False
    return all_passed

def main():
    print("Running system setup tests...\n")
    
    # Test Python packages
    packages_ok = test_python_packages()
    
    # Test Kafka connection
    kafka_ok = test_kafka_connection()
    
    print("\nTest Summary:")
    print("-------------")
    print(f"Python Packages: {'✓ OK' if packages_ok else '✗ FAILED'}")
    print(f"Kafka Connection: {'✓ OK' if kafka_ok else '✗ FAILED'}")
    
    if packages_ok and kafka_ok:
        print("\nAll tests passed! The system is ready to run.")
        sys.exit(0)
    else:
        print("\nSome tests failed. Please fix the issues before running the application.")
        sys.exit(1)

if __name__ == "__main__":
    main() 