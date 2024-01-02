import os
import time
import orjson
import logging
from pathlib import Path
from tempfile import mkdtemp
from base64 import b64decode
from collections import defaultdict
from typing import Union, List, Generator, Dict
from inspector_gadget.exceptions import InvalidAction
from confluent_kafka import DeserializingConsumer, TopicPartition


log = logging.getLogger(__name__)
OFFSET_MANAGEMENTS = ["pre-yield", "post-yield"]


class PublicKafkaConsumer:
    CERT_NAME = "kafka.crt"
    CERT_KEY_NAME = "kafka.key"
    EXCEPTION_LIMIT = 3
    HEARTBEAT = os.environ["KAFKA_HEARBEAT"]

    def __init__(
        self,
        topics: Union[List[str], str],
        group_id: str,
        offset_reset: str = "latest",
        auto_commit: bool = True,
        cert_cache_path: Union[str, Path] = None,
        skip_ssl: bool = True,
        additional_client_args: Dict = None,
        error_limit: int = 3,
    ):
        if not isinstance(topics, List):
            topics = [topics]
        if cert_cache_path is None:
            self._cert_path = Path(mkdtemp())
        else:
            if isinstance(cert_cache_path, str):
                cert_cache_path = Path(cert_cache_path)
            self._cert_path = cert_cache_path
        self.topics = topics
        self.group_id = group_id
        self.offset_reset = offset_reset
        self.auto_commit = auto_commit
        self.skip_ssl = skip_ssl
        self._kwargs = additional_client_args or {}
        self.__connection = None
        self._connection_last_used = 0
        self._errors = 0
        self._error_limit = error_limit
        if not self.auto_commit:
            log.warning(
                "You will need to manage your own offsets when auto_commit is set to False"
            )

    @property
    def _connection(self) -> DeserializingConsumer:
        """
        Returns a connection or reconnects if it has been longer than heartbeat since connection was used

        NOTE :: The stream method handles committing messages!

        :return: KafkaConsumer connection
        """
        if (
            self.__connection is None
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
            self.__connection = DeserializingConsumer(
                {
                    "group.id": self.group_id,
                    "enable.auto.commit": False,
                    "auto.offset.reset": self.offset_reset,
                    "bootstrap.servers": os.environ["KAFKA_BROKERS"],
                    "value.deserializer": lambda v, _: orjson.loads(v.decode()),
                    "ssl.certificate.location": ssl_certfile,
                    "ssl.key.location": ssl_keyfile,
                    "security.protocol": security_protocol,
                    "enable.auto.offset.store": True,
                    # NOTE :: Make the internal heartbeat 2x the external
                    "max.poll.interval.ms": self.HEARTBEAT * 2000,
                    **self._kwargs,
                }
            )
            self.__connection.subscribe(self.topics)
            self._connection_last_used = time.time()
        return self.__connection

    def stream(
        self,
        batch_size: int = 500,
        offset_management: str = "pre-yield",
        commit_interval: int = 1,
    ) -> Generator[List[Dict], None, None]:
        """
        Stream results from subscribed topics in batches

        If topic(s) have no more messages, the stream will regularly return an empty batch.
        Consumers should handle this accordingly.

        for item_batch in consumer.stream() will keep yielding forever.
        if item_batch is empty, it should simply continue and wait for a next batch

        :param batch_size: max batch size to be returned
        :param offset_management:
          - pre-yield: kafka builds a batch of up to x size, and updates the offset once before yielding. if a failure
                  happens, it would only result in the error of 1 batch.
          - post-yield: the offset for each partition is updated AFTER the yield. that way an exception will result
                  in a retry. but be warned this would potentially cause duplicates unless batch size is 1.
        :param commit_interval: how many batches to yield before committing offsets
          only increase this if messages are okay to be missed/retried if a consumer restarts
        :return: generator of batches of messages up to the batch size
        """

        def _commit_offsets(is_before_yield, skip_batch_count):
            if not self.auto_commit:
                return
            # NOTE :: Need to allow for case to force commit regardless of current batch count
            if state["batches"] < commit_interval and not skip_batch_count:
                return
            if is_before_yield and offset_management == "post-yield":
                # NOTE :: This is to skip committing offsets before the yield when they will be committed after
                return
            elif not is_before_yield and offset_management == "pre-yield":
                # NOTE :: This is to skip committing offsets after the yield when they were already committed before
                return

            to_commit = []
            for topic_partition, max_last_message in state["max_offsets"].items():
                if max_last_message > 0:
                    log.info(f"Setting {topic_partition} to {max_last_message + 1}")
                    to_commit.append(
                        TopicPartition(
                            topic_partition[0], topic_partition[1], max_last_message + 1
                        )
                    )
            state["max_offsets"].clear()
            state["batches"] = 0
            if to_commit:
                self._connection_last_used = time.time()
                self._connection.commit(offsets=to_commit, asynchronous=False)

        def _batch_or_partial_yield(next_message):
            if next_message is None:
                if state["batch"]:
                    state["batches"] += 1
                    _commit_offsets(is_before_yield=True, skip_batch_count=False)
                    yield [r["value"] for r in state["batch"]]
                    _commit_offsets(is_before_yield=False, skip_batch_count=False)
                    state["batch"] = []
                elif state["batches"]:
                    # NOTE :: If we've run out of things to process, and offsets haven't been committed yet due to
                    #         a commit_interval higher than 1, we should commit offsets
                    _commit_offsets(is_before_yield=True, skip_batch_count=True)
                    _commit_offsets(is_before_yield=False, skip_batch_count=True)
                    yield []
                else:
                    # NOTE :: return an empty batch so consumers can decide what to do when topic is empty
                    yield []
            else:
                tp = (next_message.topic(), next_message.partition())
                if len(state["batch"]) >= batch_size:
                    state["batches"] += 1
                    _commit_offsets(is_before_yield=True, skip_batch_count=False)
                    yield [r["value"] for r in state["batch"]]
                    _commit_offsets(is_before_yield=False, skip_batch_count=False)
                    state["batch"] = []
                state["batch"].append(
                    {
                        "tp": tp,
                        "value": next_message.value(),
                        "offset": next_message.offset(),
                    }
                )
                if next_message.offset() > state["max_offsets"][tp]:
                    state["max_offsets"][tp] = next_message.offset()

        if commit_interval < 1:
            raise InvalidAction("Commit interval cannot be less than 1!")
        state = {
            "batch": [],
            "exception_count": 0,
            "max_offsets": defaultdict(int),
            "batches": 0,
        }
        if offset_management not in OFFSET_MANAGEMENTS:
            raise Exception(f"{offset_management} is not a valid option.")
        while True:
            try:
                msg_pack = self._connection.poll(timeout=5)
                if not msg_pack or msg_pack.error():
                    # NOTE :: This will yield a message batch even if it isn't full yet
                    if msg_pack and msg_pack.error():
                        self._errors += 1
                        if self._errors >= self._error_limit:
                            raise Exception(msg_pack.error())
                        else:
                            log.warning(
                                f"Consumer hit error {msg_pack.error()}. Resetting connection."
                            )
                        self.__connection = None
                    for batch in _batch_or_partial_yield(None):
                        if batch is not None:
                            yield batch
                    time.sleep(10)
                else:
                    self._connection_last_used = time.time()
                    self._errors = 0
                    for batch in _batch_or_partial_yield(msg_pack):
                        if batch is not None:
                            yield batch
                    state["exception_count"] = 0
            except Exception as e:
                state["exception_count"] += 1
                log.exception(e)
                if state["exception_count"] > self.EXCEPTION_LIMIT:
                    raise

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
