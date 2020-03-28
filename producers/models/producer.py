"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://kafka0:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081/"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "schema.registry.url": SCHEMA_REGISTRY_URL
        }

        #Configure the AvroProducer
        self.schema_registry = CachedSchemaRegistryClient({"url": self.broker_properties["schema.registry.url"]})
        self.producer = AvroProducer(
            {"bootstrap.servers": self.broker_properties["bootstrap.servers"]},
            schema_registry = self.schema_registry,
            default_key_schema = self.key_schema,
            default_value_schema = self.value_schema)

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
            logger.info(f"created topic: {self.topic_name}")



    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        logger.info(f"creating producer topic: {self.topic_name}")
        admin = AdminClient({"bootstrap.servers": self.broker_properties["bootstrap.servers"]})
        admin.create_topics([NewTopic(self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)])
        logger.info(f"topic {self.topic_name} complete")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
        logger.info(f"producer {self.topic_name} closed")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
