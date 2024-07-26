import logging
import os
import time
from base64 import b64decode
from pathlib import Path
from tempfile import mkdtemp
from typing import Dict, Union

from confluent_kafka.admin import AdminClient, ConsumerGroupDescription

log = logging.getLogger(__name__)


class KafkaAdmin:
    BOOTSTRAP_ENV = None
    CERT_NAME = "public-kafka.crt"
    CERT_KEY_NAME = "public-kafka.key"
    HEARTBEAT = 300 * 60
    BATCH_SIZE = 1000

    def __init__(
        self,
        cert_cache_path: Union[str, Path] = None,
        skip_ssl: bool = False,
        additional_client_args: Dict = None,
    ):
        if cert_cache_path is None:
            self._cert_path = Path(mkdtemp())
        else:
            if isinstance(cert_cache_path, str):
                cert_cache_path = Path(cert_cache_path)
            self._cert_path = cert_cache_path
        self.skip_ssl = skip_ssl
        self._kwargs = additional_client_args or {}
        self.__connection = None
        self._connection_last_used = 0

    @property
    def _connection(self) -> AdminClient:
        """
        Returns a connection or reconnects if it has been longer than heartbeat since connection was used

        :return: AdminClient connection
        """
        if (
            self.__connection is None
            or (time.time() - self._connection_last_used) > self.HEARTBEAT
        ):
            if self.__connection:
                self.__connection = None
            self.__connection = self._build_connection()
            self._connection_last_used = time.time()
        return self.__connection

    def get_consumer_status(self, group_id: str) -> ConsumerGroupDescription:
        """
        Gets the status of a consumer group

        :param group_id: id of group
        :return: ConsumerGroupDescription
        """
        return self._connection.describe_consumer_groups([group_id])[group_id].result()

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
        return AdminClient(
            {
                "bootstrap.servers": os.environ[f"{self.BOOTSTRAP_ENV}_BROKERS"],
                "ssl.certificate.location": ssl_certfile,
                "ssl.key.location": ssl_keyfile,
                "security.protocol": security_protocol,
                **self._kwargs,
            }
        )

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
