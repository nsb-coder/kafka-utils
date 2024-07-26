import logging
import os
from typing import List

from confluent_kafka.admin import AdminClient, NewTopic

log = logging.getLogger(__name__)


def create_topics(topics: List):
    admin_client = AdminClient({"bootstrap.servers": os.environ["KAFKA_BROKERS"]})
    topic_list = []

    try:
        for topic in topics:
            topic, num_partitions, replication_factor, replica_assignment = topic
            topic_list.append(
                NewTopic(
                    topic=topic,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor,
                    replica_assignment=replica_assignment,
                )
            )
        admin_client.create_topics(topic_list)
        log.info("Topics created successfully!")

    except Exception as e:
        log.error(f"Failed to create topics! {e}")
