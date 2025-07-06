import sys
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json

def validate_kafka_setup():
    """
    Validate the Kafka setup by checking the connection to the local Kafka broker.
    """
    print("[Environment Check] Validating Kafka setup...")
    max_attempts = 4
    for attempt in range(max_attempts):
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("[Kafka] Setup validated successfully ✓")
            kafka_producer.close()
            return True
        except NoBrokersAvailable:
            if attempt < max_attempts - 1:
                print(f"[Kafka] Setup not validated, retrying ({attempt+1}/{max_attempts})...")
                time.sleep(4)
            else:
                print("[Kafka] Setup validation failed ✗")
                return False

def validate_python_environment():
    """
    Validate the Python environment by checking for required packages.
    """
    print("[Environment Check] Validating Python environment...")
    required_packages = ['pyspark', 'nltk', 'pandas', 'matplotlib', 'seaborn', 'kafka']
    all_packages_found = True
    for package in required_packages:
        try:
            __import__(package)
            print(f"[Python] {package} found ✓")
        except ImportError:
            print(f"[Python] {package} not found ✗")
            all_packages_found = False
    return all_packages_found

def run_environment_checks():
    print("\nAirline Customer Review Processing Pipeline: Environment Checks\n")
    python_environment_valid = validate_python_environment()
    kafka_setup_valid = validate_kafka_setup()
    print("\n--- Environment Check Results ---")
    print(f"Python Environment: {'✓ Valid' if python_environment_valid else '✗ Invalid'}")
    print(f"Kafka Setup: {'✓ Valid' if kafka_setup_valid else '✗ Invalid'}")
    if python_environment_valid and kafka_setup_valid:
        print("\nEnvironment checks passed. You may proceed with the pipeline.")
        sys.exit(0)
    else:
        print("\nPlease resolve the above issues before proceeding with the pipeline.")
        sys.exit(1)

if __name__ == "__main__":
    run_environment_checks()