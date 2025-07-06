import os
import sys
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json

def verify_kafka_broker():
    """
    Attempt to establish a connection to the local Kafka broker.
    Retries several times before failing.
    """
    print("[Check] Kafka broker connectivity...")
    attempts = 5
    for n in range(attempts):
        try:
            producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("[Kafka] Connected ✓")
            producer.close()
            return True
        except NoBrokersAvailable:
            if n < attempts - 1:
                print(f"[Kafka] Not available, retry {n+1}/{attempts} (waiting 5s)")
                time.sleep(5)
            else:
                print("[Kafka] Connection FAILED ✗")
                return False

def verify_python_dependencies():
    """
    Ensure all required Python packages for the news pipeline are installed.
    """
    print("\n[Check] Python package requirements...")
    deps = [
        'pyspark',
        'nltk',
        'pandas',
        'matplotlib',
        'seaborn',
        'kafka'
    ]
    all_ok = True
    for dep in deps:
        try:
            __import__(dep)
            print(f"[Python] {dep} ✓")
        except ImportError:
            print(f"[Python] {dep} ✗ (NOT installed)")
            all_ok = False
    return all_ok

def run_system_checks():
    print("News Article Pipeline: System Setup Checks\n")
    py_ok = verify_python_dependencies()
    kafka_ok = verify_kafka_broker()
    print("\n--- Setup Summary ---")
    print(f"Python Packages: {'✓ OK' if py_ok else '✗ FAILED'}")
    print(f"Kafka Broker: {'✓ OK' if kafka_ok else '✗ FAILED'}")
    if py_ok and kafka_ok:
        print("\nAll checks passed. Ready to launch the pipeline!")
        sys.exit(0)
    else:
        print("\nSome checks failed. Please resolve before running the application.")
        sys.exit(1)

if __name__ == "__main__":
    run_system_checks()