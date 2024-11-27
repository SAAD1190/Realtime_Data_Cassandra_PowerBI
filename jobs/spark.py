from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType
import requests
import json

# Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("SmartCityStreaming") \
#     .config("spark.cassandra.connection.host", "cassandra-node1") \
#     .config("spark.cassandra.connection.port", "9042") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("SmartCityStreaming") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
    .config("spark.cassandra.connection.host", "cassandra-node1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()


# Define schema for vehicle data (including the extra fields you requested)
vehicle_schema = StructType() \
    .add("id", StringType()) \
    .add("deviceId", StringType()) \
    .add("timestamp", StringType()) \
    .add("location", StringType()) \
    .add("speed", FloatType()) \
    .add("carModel", StringType()) \
    .add("fuelType", StringType()) \
    .add("fuelConsumption", FloatType()) \
    .add("tirePressure", FloatType()) \
    .add("alAlloyConcentration", FloatType()) \
    .add("oilTemperature", FloatType())

# Read vehicle data stream from Kafka topic
vehicle_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "vehicle_data") \
    .load()

# Parse the JSON data from Kafka messages
vehicle_stream_parsed = vehicle_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), vehicle_schema).alias("data")) \
    .select("data.*")

# Display vehicle stream data schema
vehicle_stream_parsed.printSchema()

# Start streaming query to process data (example: write to console for testing)
console_query = vehicle_stream_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Example: Calculate average speed by manufacturer, model, and fuel type
from pyspark.sql.functions import avg

avg_speed = vehicle_stream_parsed.groupBy("carModel", "fuelType") \
    .agg(avg("speed").alias("avg_speed"))

# Write the result to the console (you can modify this for other sinks like Power BI)
avg_speed_query = avg_speed.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Power BI API URL for your dataset (change it to match your dataset's URL)
power_bi_url = "https://api.powerbi.com/beta/93401443-f80a-4526-932b-487074bef423/datasets/5700211f-406a-4adb-b366-355d06df7aac/rows?experience=power-bi&key=7i1H67UHKjQFzU9%2FQDuglXZE6FGwhhwUBpqdqeKB5NygLyZ6niNDDtR0wNhNQRE8VXRS96dYakaWwL9OsAKbrQ%3D%3D"

def push_to_power_bi(data):
    headers = {"Content-Type": "application/json"}
    response = requests.post(power_bi_url, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
        print(f"Error pushing to Power BI: {response.status_code}, {response.text}")
    else:
        print("Data successfully pushed to Power BI")

def push_to_power_bi_batch(df, epoch_id):
    data = df.toJSON().collect()  # Convert the DataFrame to JSON
    for record in data:
        push_to_power_bi(json.loads(record))  # Push each record to Power BI

# Modify the query to push processed data to Power BI using foreachBatch
power_bi_query = vehicle_stream_parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(push_to_power_bi_batch) \
    .start()

# Write data to Cassandra
# def write_to_cassandra(df, epoch_id):
#     df.write \
#       .format("org.apache.spark.sql.cassandra") \
#       .mode("append") \
#       .options(table="vehicle_data", keyspace="datamasterylab_keyspace") \
#       .save()
    
def write_to_cassandra(df, epoch_id):
    print("Writing batch to Cassandra...")
    try:
        df.write \
          .format("org.apache.spark.sql.cassandra") \
          .mode("append") \
          .options(table="vehicle_data", keyspace="datamasterylab_keyspace") \
          .save()
        print("Batch written successfully.")
    except Exception as e:
        print(f"Error writing batch to Cassandra: {e}")


# Execute Cassandra batch write
cassandra_query = vehicle_stream_parsed.writeStream \
    .foreachBatch(write_to_cassandra) \
    .start()


# Example for setting a checkpoint location for fault tolerance
# cassandra_query = vehicle_stream_parsed.writeStream \
#     .foreachBatch(write_to_cassandra) \
#     .option("checkpointLocation", "/path/to/checkpoint") \
#     .start()

"This ensures that in case of failure, the streaming query can resume from the last checkpoint, preserving data consistency."

# Wait for termination of queries
console_query.awaitTermination()
avg_speed_query.awaitTermination()
power_bi_query.awaitTermination()

cassandra_query.awaitTermination()