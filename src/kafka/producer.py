import logging
import os
import time
from base64 import b64decode
from pathlib import Path
from tempfile import mkdtemp
from typing import Generator, Iterable, List, Union

import orjson
from confluent_kafka import Producer

from src.kafka.utils import chunks


log = logging.getLogger(__name__)


class KafkaProducer:
    CERT_NAME = "kafka.crt"
    CERT_KEY_NAME = "kafka.key"
    HEARTBEAT = os.environ["KAFKA_HEARBEAT"]
    BATCH_SIZE = 1000

    def __init__(
        self,
        topic: str,
        cert_cache_path: Union[str, Path] = None,
        skip_ssl: bool = True,
    ) -> None:
        if cert_cache_path:
            self._cert_path = Path(mkdtemp())
        else:
            if isinstance(cert_cache_path, str):
                cert_cache_path = Path(cert_cache_path)
                self._cert_path = cert_cache_path

        self.topic = topic
        self.skip_ssl = skip_ssl
        self.__connection = None
        self._connection_last_used = 0

    @property
    def _connection(self) -> Producer:
        """
        Returns a connection or reconnects if it has been longer than hearbeat since connection was used
        :return: KafkaProducer connection
        """
        if (
            self.__connection
            or (time.time() - self._connection_last_used) > self.HEARTBEAT
        ):
            if self.__connection:
                self.__connection = None
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
            self.__connection = Producer(
                {
                    "bootstrap.servers": os.environ["KAFKA_BROKERS"],
                    "ssl.certificate.location": ssl_certfile,
                    "ssl.key.location": ssl_keyfile,
                    "security.protocol": security_protocol,
                    "compression.type": "snappy",
                }
            )
            self._connection_last_used = time.time()
        return self.__connection

    def push_to_kafka(self, values: Iterable) -> None:
        """
        Pushes messages to Kafka in batches

        :param values: iterable of JSONable batches
        """
        for message_chunk in chunks(values, self.BATCH_SIZE):
            for message in message_chunk:
                self._connection.produce(self.topic, value=orjson.dumps(message))
                self._connection_last_used = time.time()
            self._connection.flush()

    def _check_auth_files(self) -> bool:
        """
        Checks that environ variables have been saved as cached files on the system

        :return: True if SSL params exist and should be used to connect
        """
        if (
            "KAFKA_CERT" not in os.environ or "KAFKA_KEY" not in os.environ
        ) or self.skip_ssl:
            log.warning("Attempting to connect without SSL...")
            return False

        if not self._cert_path.joinpath(self.CERT_NAME).exists():
            with open(self._cert_path.joinpath(self.CERT_NAME), "wb") as outfile:
                outfile.write(b64decode(os.environ["KAFKA_CERT"]))
            with open(self._cert_path.joinpath(self.CERT_KEY_NAME), "wb") as outfile:
                outfile.write(b64decode(os.environ["KAFKA_KEY"]))
        return True
