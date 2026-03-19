import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models_hw import ride_deserializer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-console',
    value_deserializer=ride_deserializer
)

print(f"Listening to {topic_name}...")

count = 0
num_trips_gt_5 = 0
for message in consumer:
    ride = message.value
    pickup_dt = datetime.fromtimestamp(ride.lpep_pickup_datetime / 1000)
    dropoff_dt = datetime.fromtimestamp(ride.lpep_dropoff_datetime / 1000)
    # print(f"Received: PU={ride.PULocationID}, DO={ride.DOLocationID}, "
    #       f"distance={ride.trip_distance}, amount=${ride.total_amount:.2f}, "
    #       f"pickup={pickup_dt}, dropoff={dropoff_dt}")
    if ride.trip_distance > 5:
        num_trips_gt_5 += 1
    count += 1
    print(f"Number of trips with trip_distance > 5: {num_trips_gt_5}")
    print(f"Number of trips: {count}")

print(f"\nNumber of trips with trip_distance > 5: {num_trips_gt_5}")

consumer.close()