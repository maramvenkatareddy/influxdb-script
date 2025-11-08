import random
import time
from influxdb_client import InfluxDBClient, Point, WritePrecision

# InfluxDB Configuration
URL = "https://d0ymte4kaf-fffegzhdcwt67p.timestream-influxdb.ap-south-1.on.aws:8181" # Primary read/write endpoint
TOKEN = ""  # Please provide the token which will be stored in the aws secrets manager
DATABASE = "test-db"  # Database Name
TABLE = "SensorData" # table name
ORG = "_admin"  # Org which will be stored in the AWS secrets Manager
 
# Initialize client
client = InfluxDBClient(url=URL, token=TOKEN)
write_api = client.write_api()

# Configuration
SENSORS = [f"sensor-{i}" for i in range(1, 6)]
TOTAL_RECORDS = 50
records_written = 0

def write_record(sensor_id: str, temperature: float, humidity: float) -> None:
    """Write a single record to InfluxDB."""
    point = (
        Point(TABLE)
        .tag("sensor_id", sensor_id)
        .field("temperature", temperature)
        .field("humidity", humidity)
        .time(time.time_ns(), WritePrecision.NS)
    )
    
    write_api.write(bucket=DATABASE, org=ORG, record=point)
    print(f"[{records_written + 1}/{TOTAL_RECORDS}] Written: {sensor_id} | Temp={temperature:.2f}°C | Humidity={humidity:.2f}%")

# Main loop - write exactly 50 records
print(f"Starting to write {TOTAL_RECORDS} records...")
print("="*80)

sensor_index = 0
while records_written < TOTAL_RECORDS:
    # Round-robin through sensors
    sensor_id = SENSORS[sensor_index % len(SENSORS)]
    
    temperature = round(random.uniform(20.0, 30.0), 2)
    humidity = round(random.uniform(30.0, 70.0), 2)
    
    write_record(sensor_id, temperature, humidity)
    records_written += 1
    sensor_index += 1
    
    # Small delay to avoid overwhelming the database
    time.sleep(0.1)  # 100ms delay

print("="*80)
print(f"✅ Successfully wrote {records_written} records to {DATABASE}.{TABLE}")

# Close the client
client.close()
