import json
import logging
import os
import time
from base64 import b64decode
from collections import defaultdict
from pathlib import Path
from tempfile import mkdtemp
from typing import Callable, Dict, Generator, List, Tuple, Union

import orjson
from confluent_kafka import DeserializingConsumer, TopicPartition
from inspector_gadget.exceptions import InvalidAction

log = logging.getLogger(__name__)
OFFSET_MANAGEMENTS = ["pre-yield", "post-yield"]


class KafkaConsumer:
    BOOTSTRAP_ENV = None
    CERT_NAME = "public-kafka.crt"
    CERT_KEY_NAME = "public-kafka.key"
    EXCEPTION_LIMIT = 3
    HEARTBEAT = 300 * 60

    def __init__(
        self,
        topics: Union[List[str], str],
        group_id: str,
        offset_reset: str = "latest",
        auto_commit: bool = True,
        cert_cache_path: Union[str, Path] = None,
        skip_ssl: bool = False,
        additional_client_args: Dict = None,
        error_limit: int = 3,
        serialize_as_json: bool = True,
        metadata_only: bool = False,
    ):
        if cert_cache_path is None:
            self._cert_path = Path(mkdtemp())
        else:
            if isinstance(cert_cache_path, str):
                cert_cache_path = Path(cert_cache_path)
            self._cert_path = cert_cache_path

        if not isinstance(topics, List):
            topics = [topics]
        self.topics = topics
        self.group_id = group_id
        self.offset_reset = offset_reset
        self.auto_commit = auto_commit
        self.serialize_as_json = serialize_as_json
        self.skip_ssl = skip_ssl
        self._metadata_only = metadata_only
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
            self.__connection = self._build_connection()
            if not self._metadata_only:
                self.__connection.subscribe(self.topics)
            self._connection_last_used = time.time()
        return self.__connection

    def stream(
        self,
        batch_size: int = 500,
        offset_management: str = "pre-yield",
        commit_interval: int = 1,
        message_filter: Callable[[Union[Dict, str]], bool] = None,
        max_filtered: int = 5000,
        include_topic: bool = False,
        include_timestamp: bool = False,
    ) -> Generator[
        List[
            Union[
                Union[Dict, str],
                Union[Union[Dict, str], str],
                Union[Union[Dict, str], int],
                Union[Union[Dict, str], str, int],
            ]
        ],
        None,
        None,
    ]:
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
        :param message_filter: pre-filter messages with a callable that takes a serialized value and returns True if
                               it should be returned
        :param max_filtered: number of filtered messages to skip before yielding a partial batch
        :param include_topic: includes topic with message making return signature Tuple[value, topic]
        :param include_timestamp: include the append timestamp of the message (in milliseconds)
        :return: generator of batches of messages up to the batch size
        """

        def _allow_message(message_value):
            if not message_filter:
                return True
            else:
                should_return = message_filter(message_value)
                if not should_return:
                    state["filtered_messages"] += 1
                    return False
                else:
                    return True

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

        def _yield():
            state["batches"] += 1
            _commit_offsets(is_before_yield=True, skip_batch_count=False)
            formatted_batch = []
            for r in state["batch"]:
                if include_topic and include_timestamp:
                    formatted_batch.append((r["value"], r["tp"][0], r["timestamp"]))
                elif include_topic:
                    formatted_batch.append((r["value"], r["tp"][0]))
                elif include_timestamp:
                    formatted_batch.append((r["value"], r["timestamp"]))
                else:
                    formatted_batch.append(r["value"])
            yield formatted_batch
            _commit_offsets(is_before_yield=False, skip_batch_count=False)
            state["batch"] = []
            state["filtered_messages"] = 0

        def _batch_or_partial_yield(next_message):
            if next_message is None:
                if state["batch"]:
                    yield from _yield()
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
                if (
                    len(state["batch"]) >= batch_size
                    or state["filtered_messages"] >= max_filtered
                ):
                    yield from _yield()

                message_value = next_message.value()
                if _allow_message(message_value):
                    state["batch"].append(
                        {
                            "tp": tp,
                            "value": message_value,
                            "offset": next_message.offset(),
                            "timestamp": next_message.timestamp()[1],
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
            "filtered_messages": 0,
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

    def get_group_lag(self) -> Tuple[int, Dict[str, Dict[int, int]]]:
        """
        Gets the consumer lag by topic -> partition: lag

        :return: total_lag, detailed
        """
        total_lag = 0
        details = defaultdict(lambda: defaultdict(int))
        for topic in self.topics:
            metadata = self._connection.list_topics(topic)
            partitions = [
                TopicPartition(topic, p) for p in metadata.topics[topic].partitions
            ]
            committed = self._connection.committed(partitions)
            for partition in committed:
                # Get the partitions low and high watermark offsets.
                (lo, hi) = self._connection.get_watermark_offsets(
                    partition, timeout=10, cached=False
                )
                lag = (hi - lo) if partition.offset < 0 else (hi - partition.offset)
                total_lag += lag
                details[partition.topic][partition.partition] += lag
        return total_lag, details

    def _build_connection(self):
        """
        Builds the Consumer connection

        :return: Consumer
        """

        def _deserialize(v, ctx):
            try:
                try:
                    return orjson.loads(v) if self.serialize_as_json else v.decode()
                except:
                    return (
                        json.loads(v.decode("utf-8"))
                        if self.serialize_as_json
                        else v.decode("utf-8")
                    )
            except:
                log.warning(
                    f"Kafka Consumer Serialization Error for Message: {v.decode()}"
                )
                raise

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
        return DeserializingConsumer(
            {
                "group.id": self.group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": self.offset_reset,
                "bootstrap.servers": os.environ[f"{self.BOOTSTRAP_ENV}_BROKERS"],
                "value.deserializer": _deserialize,
                "ssl.certificate.location": ssl_certfile,
                "ssl.key.location": ssl_keyfile,
                "security.protocol": security_protocol,
                "enable.auto.offset.store": True,
                # NOTE :: Make the internal heartbeat 2x the external
                "max.poll.interval.ms": self.HEARTBEAT * 2000,
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
