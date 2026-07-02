import json
import os
import time
from dotenv import load_dotenv
import redis

load_dotenv(dotenv_path="../../.env")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Connect to Redis
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def dispatch_rider_query():
    print("\n[RIDER API] Incoming ride request detected!")
    print("[QUERYING] Scanning Redis in-memory state grid...")
    
    start_time = time.perf_counter()
    
    # Fetch all values out of our Redis Hash table instantly from RAM
    all_drivers = redis_client.hgetall("driver:state")
    
    execution_time_ms = (time.perf_counter() - start_time) * 1000
    
    if not all_drivers:
        print("[ALERT] No active vehicle states found in cache grid.")
        return

    print(f"[SUCCESS] Retrieved state entries for {len(all_drivers)} active drivers.")
    print(f"[PERFORMANCE] Core RAM cache query completed in {execution_time_ms:.4f} ms")
    print("-" * 60)
    
    # Print out a slice of current driver reality
    count = 0
    for d_id, data_str in all_drivers.items():
        data = json.loads(data_str)
        if data.get("status") == "AVAILABLE":
            print(f"-> {d_id} is AVAILABLE at Lat: {data['latitude']}, Lon: {data['longitude']}")
            count += 1
            if count >= 3:  # Only show the first 3 available to keep terminal clean
                break

if __name__ == "__main__":
    # Test query the system
    dispatch_rider_query()