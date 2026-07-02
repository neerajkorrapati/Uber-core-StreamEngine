import json
import os
import signal
import sys
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Load environment configuration from the root directory
load_dotenv(dotenv_path="../../.env")

KAFKA_BROKER = os.getenv("KAFKA_BROKER_URL", "localhost:19092")
TOPIC_NAME = os.getenv("TELEMETRY_TOPIC", "driver-telemetry")
GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "uber-matching-processor")

print(f"[BOOT] Initializing Processing Core connecting to Broker: {KAFKA_BROKER}")

# Initialize our Pure-Python Kafka Consumer Group Gateway
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    group_id=GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    auto_commit_interval_ms=5000  # Commit read markers back to broker every 5 seconds
)

def handle_shutdown(signum, frame):
    """Gracefully closes the consumer network sockets on system interruption."""
    print("\n[SHUTDOWN] Signal captured. Committing final offsets and closing consumer connection...")
    consumer.close()
    print("[SHUTDOWN] Processing core cleanly offline.")
    sys.exit(0)

# Register OS interruption handlers (Ctrl + C)
signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def run_engine_loop():
    print(f"[RUNNING] Active on Topic: '{TOPIC_NAME}' | Group ID: '{GROUP_ID}'")
    print("[WAITING] Monitoring partitions for telemetry vectors...")
    
    # Infinite polling loop reading incoming message frames
    for message in consumer:
        payload = message.value
        partition = message.partition
        offset = message.offset
        
        # Extract individual telemetry vectors
        driver_id = payload.get("driver_id")
        lat = payload.get("latitude")
        lon = payload.get("longitude")
        status = payload.get("status")
        
        print(f"[PROCESSING] Part: {partition} | Off: {offset} -> {driver_id} is {status} at [{lat}, {lon}]")

if __name__ == "__main__":
    run_engine_loop()