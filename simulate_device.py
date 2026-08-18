import time
import random
import requests
import sys
import os

# Ensure the gateway path is accessible to import compiled protobufs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "gateway")))

try:
    from app.pb import telemetry_pb2
except ImportError:
    print("Error: telemetry_pb2 not found. Make sure you ran the protoc compilation step!")
    sys.exit(1)

GATEWAY_URL = "http://localhost:8000/api/v1/telemetry"
DEVICE_ID = "pi-edge-sensor-01"

def generate_payload():
    """Generates a serialized Protocol Buffers binary payload."""
    msg = telemetry_pb2.MetricReading()
    msg.device_id = DEVICE_ID
    msg.timestamp = int(time.time() * 1000)
    msg.temperature = round(random.uniform(20.0, 45.0), 2)
    msg.voltage = round(random.uniform(3.1, 3.5), 2)
    
    # Simulate 3D Accelerometer
    msg.acceleration.x = round(random.uniform(-1.0, 1.0), 2)
    msg.acceleration.y = round(random.uniform(9.0, 10.5), 2)
    msg.acceleration.z = round(random.uniform(-0.5, 0.5), 2)
    
    # Simulate 3D Gyroscope
    msg.gyro.x = round(random.uniform(-0.1, 0.1), 2)
    msg.gyro.y = round(random.uniform(-0.1, 0.1), 2)
    msg.gyro.z = round(random.uniform(0.9, 1.1), 2)
    
    # Metadata
    msg.metadata["firmware"] = "v1.2.0-beta"
    msg.metadata["environment"] = "staging"
    
    return msg.SerializeToString()

def main():
    print(f"Starting virtual edge device simulator for '{DEVICE_ID}'...")
    print(f"Sending telemetry packets to {GATEWAY_URL}. Press Ctrl+C to stop.\n")
    
    try:
        while True:
            binary_payload = generate_payload()
            
            # Send as binary stream via POST
            response = requests.post(
                GATEWAY_URL, 
                data=binary_payload, 
                headers={"Content-Type": "application/octet-stream"}
            )
            
            if response.status_code == 202:
                print(f"[{time.strftime('%H:%M:%S]')} Sent packet successfully — Response: {response.json()}")
            else:
                print(f"[{time.strftime('%H:%M:%S]')} Failed to send — Status: {response.status_code}, Body: {response.text}")
                
            # Send a packet every 1 second
            time.sleep(1.0)
            
    except KeyboardInterrupt:
        print("\nSimulation stopped by user.")

if __name__ == "__main__":
    main()