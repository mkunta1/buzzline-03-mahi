"""
json_consumer_mahi.py

Consume JSON messages from a Kafka topic and process them.

Example serialized Kafka message:
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

Example JSON message (after deserialization) to be processed:
{
  "message": "I love Python!",
  "author": "Eve"
}
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting occurrences
from dotenv import load_dotenv  # Load environment variables

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer  # Assuming you have a utility function to create Kafka consumers
from utils.utils_logger import logger  # Assuming you have a custom logger setup

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

#####################################
# Set up Data Store to hold message counts
#####################################

# Initialize a dictionary to store message counts (you can track any kind of data here)
message_counts: defaultdict[str, int] = defaultdict(int)

#####################################
# Function to process a single message
#####################################

def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict = json.loads(message)

        # Ensure the processed JSON is logged for debugging
        logger.info(f"Processed JSON message: {message_dict}")

        # Example processing: Count occurrences of different authors
        author = message_dict.get("author", "unknown")
        logger.info(f"Message received from author: {author}")

        # Increment the count for the author
        message_counts[author] += 1

        # Log the updated counts
        logger.info(f"Updated message counts: {dict(message_counts)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Define Kafka Consumer Function
#####################################

def create_kafka_consumer(topic: str, group_id: str):
    """
    Create and return a Kafka consumer for the specified topic and group.

    Args:
        topic (str): The Kafka topic to subscribe to.
        group_id (str): The consumer group ID.

    Returns:
        KafkaConsumer: A configured Kafka consumer.
    """
    try:
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            topic,
            group_id=group_id,
            value_deserializer=lambda m: m.decode('utf-8'),  # Deserialize from byte to string
            bootstrap_servers=['localhost:9092'],  # Adjust to your Kafka server
            auto_offset_reset='earliest',  # Start from the earliest message if no offset exists
            enable_auto_commit=True,  # Commit offsets automatically
        )
        logger.info(f"Kafka consumer created for topic '{topic}' and group '{group_id}'.")
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer: {e}")
        raise

#####################################
# Main Function
#####################################

def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` function.
    - Consumes messages and processes them.
    """
    logger.info("START consumer.")

    # Fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()

    # Create the Kafka consumer
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Starting message consumption from topic '{topic}'...")
    try:
        # Consume messages continuously
        for message in consumer:
            # Deserialize the message value and process it
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")
        
    logger.info("END consumer.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
