# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import json  # work with JSON data
from datetime import datetime  # work with timestamps
from kafka import KafkaConsumer  # Kafka Consumer from kafka-python

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("FOOD_TOPIC", "unknown_food_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id = os.getenv("FOOD_CONSUMER_GROUP", "default_group")
    logger.info(f"Consumer group: {group_id}")
    return group_id

#####################################
# Set up Consumer
#####################################

def create_kafka_consumer(topic: str, group_id: str):
    """
    Create a Kafka consumer for a specific topic and consumer group.
    Args:
        topic (str): Kafka topic to subscribe to.
        group_id (str): Consumer group id.
    Returns:
        KafkaConsumer: The consumer object.
    """
    try:
        consumer = KafkaConsumer(
            topic,
            group_id=group_id,
            bootstrap_servers=os.getenv("KAFKA_BROKER_URL", "localhost:9092"),
            auto_offset_reset='earliest',  # start reading at the earliest message
            enable_auto_commit=True,  # auto-commit offsets
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # Deserialize JSON messages
        )
        logger.info(f"Kafka consumer created successfully for topic '{topic}' and group '{group_id}'.")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        sys.exit(1)

#####################################
# Process Kafka Messages
#####################################

def process_message(message):
    """
    Process the received message.
    Args:
        message (dict): The received message.
    """
    try:
        logger.info(f"Processing message: {message}")
        # Extract the fields and process them
        timestamp = message.get("timestamp")
        food_item = message.get("food_item")
        quantity = message.get("quantity")
        
        # Print the message (or you can process it as per your needs)
        print(f"Received - Timestamp: {timestamp}, Food Item: {food_item}, Quantity: {quantity}")

    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Define main function for the consumer.
#####################################

def main():
    """
    Main entry point for the consumer.
    
    - Reads the Kafka topic name and consumer group id from environment variables.
    - Creates a Kafka consumer.
    - Starts consuming and processing messages.
    """

    logger.info("START consumer.")

    # Fetch .env content
    topic = get_kafka_topic()
    group_id = get_group_id()

    # Create the Kafka consumer
    consumer = create_kafka_consumer(topic, group_id)

    # Start consuming messages
    logger.info(f"Starting message consumption from topic '{topic}'...")

    try:
        for message in consumer:
            # Process each message
            process_message(message.value)  # message.value contains the actual data
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message consumption: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

    logger.info("END consumer.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
