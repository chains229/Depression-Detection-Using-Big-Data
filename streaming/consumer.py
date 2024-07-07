from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pymongo import MongoClient
from pyspark.ml import PipelineModel
import os

# Kafka Configuration
KAFKA_TOPIC = 'depression_detection'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# MongoDB Configuration
MONGO_URI = 'mongodb://localhost:27017'
MONGO_DB = 'local'
MONGO_COLLECTION = 'depression_posts'

# Load Pre-trained PySpark MLlib Model
model = PipelineModel.load("\model\modelPipeline")

# Initialize Spark Session
spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .appName("KafkaSparkMongoDB") \
    .getOrCreate()

# Schema for incoming Kafka data
schema = StructType([
    StructField("_c0", FloatType(), True),
    StructField("text", StringType(), True)
])

# Function to make predictions
def predict(df):
    predictions = model.transform(df)
    return predictions

# Kafka Consumer
def kafka_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: x.decode('utf-8')
    )

    for message in consumer:
        yield message.value

# Stream Data from Kafka, Process and Write to MongoDB
def process_stream():
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    # Make predictions
    df = predict(df)

    # Write to MongoDB
    def write_to_mongo(batch_df, batch_id):
        mongo_client = MongoClient(MONGO_URI)
        mongo_db = mongo_client[MONGO_DB]
        mongo_collection = mongo_db[MONGO_COLLECTION]
        records = batch_df.toJSON().map(lambda j: json.loads(j)).collect()
        if records:
            logging.info(f"Writing {len(records)} records to MongoDB")
            mongo_collection.insert_many(records)

    query = df.writeStream \
        .foreachBatch(write_to_mongo) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
    process_stream()
