import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps
import logging

#import requests

def create_kafka_producer(bootstrap_servers):
    try:
        logging.info("Initializing Kafka Producer...")
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
        logging.info("Kafka Producer initialized successfully.")
        return producer
    except Exception as e:
        logging.error(f"Failed to create Kafka Producer: {e}")
        raise

def read_data(filepath):
    try:
        logging.info(f"Reading data from {filepath}...")
        df = pd.read_csv(filepath)
        logging.info(f"Data read successfully. Shape: {df.shape}")
        return df
    except FileNotFoundError:
        logging.error("File not found. Please check the file path.")
        raise
    except Exception as e:
        logging.error(f"Error reading data: {e}")
        raise

def stream_data(producer, topic, df, delay=1):
    logging.info("Starting data streaming...")
    try:
        while True:
            record = df.sample(1).to_dict(orient="records")[0]
            logging.debug(f"Sending record: {record}")
            producer.send(topic, value=record)
            sleep(delay)
    except KeyboardInterrupt:
        logging.info("Data streaming interrupted by user.")
    except Exception as e:
        logging.error(f"Error during data streaming: {e}")
    finally:
        producer.flush()
        logging.info("Flushed data from Kafka Producer. Exiting...")

if __name__ == "__main__":
    # Configuration
    KAFKA_SERVERS = [':9092']  # Replace with your IP
    TOPIC = 'stream'
    FILEPATH = r'C:\Users\jiachen1203\Desktop\CPSC531\airflights_data.csv'
    
    try:
        producer = create_kafka_producer(KAFKA_SERVERS)
        df = read_data(FILEPATH)
        stream_data(producer, TOPIC, df)
    except Exception as e:
        logging.error(f"Application error: {e}")


'''
read_data from api if you have one. 
def read_data(api_url):
    try:
        logging.info(f"Fetching data from {api_url}...")
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        logging.info(f"Data fetched successfully. Shape: {df.shape}")
        return df
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        raise
    except Exception as e:
        logging.error(f"Error processing API data: {e}")
        raise

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("producer.log"),
        logging.StreamHandler()
    ]
)
'''
