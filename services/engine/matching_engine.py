import json
import os
import signal
import sys
from dotenv import load_dotenv
from kafka import KafkaConsumer
import redis

# Load environment configuration from the root directory
load_dotenv(dotenv_path="../../.env")

KAFKA_BROKER = os.getenv("KAFKA_BROKER_URL", "localhost:19092")
TOPIC_NAME = os.getenv("TELEMETRY_TOPIC", "driver-telemetry")
GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "uber-matching-processor")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

print(f"[BOOT] Initializing Processing Core...")
print(f"       -> Kafka Broker: {KAFKA_BROKER}")
print(f"       -> Redis State Grid: {REDIS_HOST}:{REDIS_PORT}")

# Initialize our Redis Connection Pool Client
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Initialize our Pure-Python Kafka Consumer Group Gateway
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    group_id=GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    auto_commit_interval_ms=5000
)

def handle_shutdown(signum, frame):
    print("\n[SHUTDOWN] Gracefully closing network consumer sockets...")
    consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def run_engine_loop():
    print(f"[RUNNING] Consuming from '{TOPIC_NAME}'. Updating RAM state grid...")
    
    for message in consumer:
        payload = message.value
        driver_id = payload.get("driver_id")
        
        # Structure a clean state object for our RAM dictionary
        state_snapshot = {
            "latitude": payload.get("latitude"),
            "longitude": payload.get("longitude"),
            "status": payload.get("status"),
            "last_updated": payload.get("timestamp")
        }
        
        # HSET key field value -> Updates our Redis Hash dictionary instantly
        redis_client.hset(
            name="driver:state",
            key=driver_id,
            value=json.dumps(state_snapshot)
        )
        
        print(f"[CACHE UPDATED] Redis Hash -> {driver_id} updated successfully.")

if __name__ == "__main__":
    run_engine_loop()