import logging
import os
import time
from base64 import b64decode
from pathlib import Path
from tempfile import mkdtemp
from typing import Any, Dict, Iterable, Tuple, Union

import orjson
from confluent_kafka import Producer
from .utils import chunks

log = logging.getLogger(__name__)


class KafkaProducer:
    BOOTSTRAP_ENV = None
    CERT_NAME = "public-kafka.crt"
    CERT_KEY_NAME = "public-kafka.key"
    HEARTBEAT = 300 * 60
    BATCH_SIZE = 1000

    def __init__(
        self,
        topic: str,
        cert_cache_path: Union[str, Path] = None,
        skip_ssl: bool = False,
        force_even_distribution: bool = False,
        additional_client_args: Dict = None,
    ):
        if cert_cache_path is None:
            self._cert_path = Path(mkdtemp())
        else:
            if isinstance(cert_cache_path, str):
                cert_cache_path = Path(cert_cache_path)
            self._cert_path = cert_cache_path
        self.topic = topic
        self.skip_ssl = skip_ssl
        self.force_even_distribution = force_even_distribution
        self._kwargs = additional_client_args or {}
        self._batch_count = 0
        self._message_count = 0
        self._num_partitions = -1
        self.__connection = None
        self._connection_last_used = 0

    @property
    def _connection(self) -> Producer:
        """
        Returns a connection or reconnects if it has been longer than heartbeat since connection was used

        :return: KafkaProducer connection
        """
        if (
            self.__connection is None
            or (time.time() - self._connection_last_used) > self.HEARTBEAT
        ):
            self._batch_count = 0
            self._message_count = 0
            if self.__connection:
                self.__connection = None
            self.__connection = self._build_connection()
            self._num_partitions = (
                self._get_partition_count() if self.force_even_distribution else -1
            )
            self._connection_last_used = time.time()
        return self.__connection

    def push_to_kafka(
        self,
        values: Iterable[Union[Tuple[str, Any], Any]],
        skip_serialization: bool = False,
        flush_interval: int = 1,
    ):
        """
        Pushes messages to Kafka in batches

        :param values: iterable of JSONable batches (if value is a Tuple, should be [key, value])
        :param skip_serialization: do not json dumps data
        :param flush_interval: how many batches to flush
        """
        for message_chunk in chunks(values, self.BATCH_SIZE):
            for message in message_chunk:
                key, value = message if isinstance(message, Tuple) else (None, message)
                if not skip_serialization:
                    value = orjson.dumps(value)
                # NOTE :: Partition should not be specified when providing a key
                partition = (
                    self._message_count % self._num_partitions
                    if self.force_even_distribution and not key
                    else -1
                )
                if partition >= 0:
                    self._connection.produce(
                        self.topic, value=value, key=key, partition=partition
                    )
                else:
                    # NOTE :: passing -1 or None for partition sends all messages to same topic so need to not pass anything
                    self._connection.produce(self.topic, value=value, key=key)
                if self.force_even_distribution and not key:
                    self._message_count += 1
                self._connection_last_used = time.time()
            self._batch_count += 1
            if self._batch_count % flush_interval == 0:
                self._connection.flush()
        # NOTE :: If flush_interval <= 1, it would've flushed before exiting the for loop
        if flush_interval > 1 and self._batch_count % flush_interval == 0:
            self._connection.flush()

    def _build_connection(self):
        """
        Builds the Producer connection

        :return: Producer
        """
        use_ssl = self._check_auth_files()
        ssl_certfile = (
            str(self._cert_path.joinpath(self.CERT_NAME).absolute())
            if use_ssl
            else None
        )
        ssl_keyfile = (
            str(self._cert_path.joinpath(self.CERT_KEY_NAME).absolute())
            if use_ssl
            else None
        )
        security_protocol = "SSL" if use_ssl else "PLAINTEXT"
        return Producer(
            {
                "bootstrap.servers": os.environ[f"{self.BOOTSTRAP_ENV}_BROKERS"],
                "ssl.certificate.location": ssl_certfile,
                "ssl.key.location": ssl_keyfile,
                "security.protocol": security_protocol,
                "compression.type": "snappy",
                **self._kwargs,
            }
        )

    def _get_partition_count(self) -> int:
        """
        Get the number of partitions for this topic to more evenly distribute messages

        :return: count
        """
        metadata = self.__connection.list_topics()
        topic = metadata.topics.get(self.topic)
        if not topic:
            raise Exception(
                f"Cannot find {self.topic} in order to get partition count!"
            )
        return len(topic.partitions)

    def _check_auth_files(self) -> bool:
        """
        Checks that environ variables have been saved as cached files on the system

        :return: True if SSL params exist and should be used to connect
        """
        if self.skip_ssl:
            return False

        if not self._cert_path.joinpath(self.CERT_NAME).exists():
            if (
                f"{self.BOOTSTRAP_ENV}_CERT" not in os.environ
                or f"{self.BOOTSTRAP_ENV}_KEY" not in os.environ
            ):
                log.warning(
                    "Attempting to connect without SSL when skip_ssl is false..."
                )
                return False

            with open(self._cert_path.joinpath(self.CERT_NAME), "wb") as outfile:
                outfile.write(b64decode(os.environ[f"{self.BOOTSTRAP_ENV}_CERT"]))
            with open(self._cert_path.joinpath(self.CERT_KEY_NAME), "wb") as outfile:
                outfile.write(b64decode(os.environ[f"{self.BOOTSTRAP_ENV}_KEY"]))
        return True
