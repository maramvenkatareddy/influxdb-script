import random
import time
from influxdb_client import InfluxDBClient, Point, WritePrecision

# InfluxDB Configuration
URL = "https://d0ymte4kaf-fffegzhdcwt67p.timestream-influxdb.ap-south-1.on.aws:8181"  # Read/Write DB endpoint
TOKEN = "<YOUR_API_TOKEN>"  # Replace with your actual token (store securely in AWS Secrets Manager)
DATABASE = "test-db"  # Database name
TABLE = "SensorData"  # Table name
ORG = "_admin"  # Use the same org name as shown in AWS Timestream Console

# Initialize client (disable SSL verify only if testing internal endpoint)
client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
write_api = client.write_api()

# Configuration
SENSORS = [f"sensor-{i}" for i in range(1, 6)]
TOTAL_RECORDS = 50

def write_record(sensor_id: str, temperature: float, humidity: float, count: int):
    """Write a single record to InfluxDB."""
    point = (
        Point(TABLE)
        .tag("sensor_id", sensor_id)
        .field("temperature", temperature)
        .field("humidity", humidity)
        .time(time.time_ns(), WritePrecision.NS)
    )
    
    write_api.write(bucket=DATABASE, org=ORG, record=point)
    print(f"[{count}/{TOTAL_RECORDS}] Written: {sensor_id} | Temp={temperature:.2f}°C | Humidity={humidity:.2f}%")

# Main loop
print(f"Starting to write {TOTAL_RECORDS} records...")
print("="*80)

try:
    for i in range(TOTAL_RECORDS):
        sensor_id = SENSORS[i % len(SENSORS)]
        temperature = round(random.uniform(20.0, 30.0), 2)
        humidity = round(random.uniform(30.0, 70.0), 2)
        write_record(sensor_id, temperature, humidity, i + 1)
        time.sleep(0.1)
    print("="*80)
    print(f"Successfully wrote {TOTAL_RECORDS} records to {DATABASE}.{TABLE}")
except Exception as e:
    print(f"Error during write: {e}")
finally:
    write_api.__del__()  # Clean up write thread
    client.close()
