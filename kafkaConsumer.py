from kafka import KafkaConsumer
from time import sleep
from json import loads, dump
import logging
from s3fs import S3FileSystem

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Kafka and S3 Configuration
KAFKA_TOPIC = 'demo'
BOOTSTRAP_SERVERS = [':9092']  # Replace IP
S3_BUCKET = "kafka-flight-analysis-bucket"
FILE_PREFIX = "kafka_analysis"

def initialize_consumer(topic, servers):
    """
    Initialize Kafka consumer.
    :param topic: Kafka topic to subscribe to.
    :param servers: List of Kafka broker addresses.
    :return: KafkaConsumer instance.
    """
    logging.info(f"Initializing Kafka consumer for topic: {topic}")
    return KafkaConsumer(
        topic,
        bootstrap_servers=servers,
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

def process_messages(consumer, bucket, prefix):
    """
    Process messages from Kafka and save them to S3.
    :param consumer: KafkaConsumer instance.
    :param bucket: S3 bucket name.
    :param prefix: File prefix for saved files.
    """
    s3 = S3FileSystem()
    logging.info(f"Connected to S3 bucket: {bucket}")

    for count, message in enumerate(consumer):
        try:
            file_path = f"s3://{bucket}/{prefix}_{count}.json"
            logging.info(f"Processing message {count}: {message.value}")
            
            with s3.open(file_path, 'w') as file:
                dump(message.value, file)
                logging.info(f"Message saved to {file_path}")
        except Exception as e:
            logging.error(f"Error processing message {count}: {e}")

if __name__ == "__main__":
    # Initialize Kafka consumer
    consumer = initialize_consumer(KAFKA_TOPIC, BOOTSTRAP_SERVERS)

    # Process messages and save them to S3
    try:
        process_messages(consumer, S3_BUCKET, FILE_PREFIX)
    except KeyboardInterrupt:
        logging.info("Consumer stopped by user.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        consumer.close()
        logging.info("Kafka consumer closed.")
