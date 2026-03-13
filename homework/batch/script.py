import os
import urllib.request
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('hw_w6').getOrCreate()

print('Spark Version: ', spark.version)

data_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet'
local_file = 'yellow_tripdata_2025-11_raw.parquet'

if not os.path.exists(local_file):
    print(f'Downloading {data_url}...')
    urllib.request.urlretrieve(data_url, local_file)
    print('Download complete.')

df = spark.read.parquet(local_file)

output_dir = 'yellow_tripdata_2025-11'
df.repartition(4).write.mode('overwrite').parquet(output_dir, compression='gzip')

files = [f for f in os.listdir(output_dir) if f.endswith('.parquet')]
total_size = sum(os.path.getsize(os.path.join(output_dir, f)) for f in files)
print(f'Number of Parquet files: {len(files)}')
print(f'Average size of the Parquet files: {total_size / len(files) / 1024 / 1024:.2f} MB')

from pyspark.sql import functions as F

df = spark.read.parquet(output_dir)
print(df.head(5))
print(df.filter(df['tpep_pickup_datetime'].startswith('2025-11-15')).count())

# Question 4: Maximum trip duration (in hours)
df = df.withColumn(
    'trip_duration',
    (F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')) / 3600
)
df.agg(F.max('trip_duration').alias('max_trip_duration_minutes')).show()

# Question 6: Least frequent pickup location zone
lookup_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
lookup_file = 'taxi_zone_lookup.csv'

if not os.path.exists(lookup_file):
    print(f'Downloading {lookup_url}...')
    urllib.request.urlretrieve(lookup_url, lookup_file)
    print('Download complete.')

lookup_df = spark.read.csv(lookup_file, header=True, inferSchema=True)
lookup_df.createOrReplaceTempView('lookup_table')

df = df.join(lookup_df, df['PULocationID'] == lookup_df['LocationID'], 'left')

df.groupBy('Zone').count().orderBy('count', ascending=True).limit(5).show()