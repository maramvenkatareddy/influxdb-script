from influxdb import InfluxDBClient
import pandas as pd
from datetime import datetime

HOST = "itck599okv-fffegzhdcwt67p.timestream-influxdb.ap-south-1.on.aws"  # Query endpoint
PORT = 8181
TOKEN = " " 
DATABASE = "test-db"

client = InfluxDBClient(
    host=HOST,
    port=PORT,
    password=TOKEN,
    database=DATABASE,
    ssl=True,
    verify_ssl=True,
    headers={'Accept': 'application/json'}
)

print("="*100)
print(f"SENSOR DATA REPORT - Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*100)

# Query 1: Total count of all data
print("\n📊 QUERY 1: TOTAL DATA COUNT")
print("-"*100)
query_count = "SELECT COUNT(*) FROM SensorData"
result_count = client.query(query_count)
points_count = list(result_count.get_points())
if points_count:
    total_count = points_count[0]['count_humidity']  # or any field
    print(f"Total records in database: {total_count:,}")
else:
    print("No data found")

# Query 2: Latest 5 records (most recent by time)
print("\n\n📈 QUERY 2: LATEST 5 RECORDS (Most Recent)")
print("-"*100)
query_latest = "SELECT * FROM SensorData ORDER BY time DESC LIMIT 5"
result_latest = client.query(query_latest)
points_latest = list(result_latest.get_points())
df_latest = pd.DataFrame(points_latest)
if not df_latest.empty:
    print(df_latest.to_string(index=False))
else:
    print("No data found")

# Query 3: First 5 records (oldest by time)
print("\n\n📉 QUERY 3: FIRST 5 RECORDS (Oldest)")
print("-"*100)
query_first = "SELECT * FROM SensorData ORDER BY time ASC LIMIT 5"
result_first = client.query(query_first)
points_first = list(result_first.get_points())
df_first = pd.DataFrame(points_first)
if not df_first.empty:
    print(df_first.to_string(index=False))
else:
    print("No data found")

# Query 4: ALL DATA
print("\n\n📦 QUERY 4: ALL DATA (Complete Dataset)")
print("-"*100)
query_all = "SELECT * FROM SensorData"
result_all = client.query(query_all)
points_all = list(result_all.get_points())
df_all = pd.DataFrame(points_all)

if not df_all.empty:
    print(f"Total records retrieved: {len(df_all):,}")
    print(f"\nFirst 10 records:")
    print(df_all.head(10).to_string(index=False))
    print(f"\n... (showing first 10 of {len(df_all):,} total records)")
    
    # Export all data to CSV
    csv_filename = f"all_sensor_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_all.to_csv(csv_filename, index=False)
    print(f"\n✅ All data exported to: {csv_filename}")
    
    # Show data statistics
    print("\n📊 DATA STATISTICS:")
    print("-"*100)
    print(f"Date Range: {df_all['time'].min()} to {df_all['time'].max()}")
    print(f"\nRecords per sensor:")
    sensor_counts = df_all['sensor_id'].value_counts().sort_index()
    for sensor, count in sensor_counts.items():
        print(f"  {sensor}: {count:,} records")
    
    print(f"\nTemperature Statistics:")
    print(f"  Min: {df_all['temperature'].min():.2f}°C")
    print(f"  Max: {df_all['temperature'].max():.2f}°C")
    print(f"  Average: {df_all['temperature'].mean():.2f}°C")
    
    print(f"\nHumidity Statistics:")
    print(f"  Min: {df_all['humidity'].min():.2f}%")
    print(f"  Max: {df_all['humidity'].max():.2f}%")
    print(f"  Average: {df_all['humidity'].mean():.2f}%")
else:
    print("No data found")

print("\n" + "="*100)
print("REPORT COMPLETE")
print("="*100)

client.close()
