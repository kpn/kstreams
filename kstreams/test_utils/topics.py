import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from typing import ClassVar, DefaultDict, Dict, Optional, Tuple

from kstreams import ConsumerRecord

from . import test_clients


@dataclass
class Topic:
    name: str
    queue: asyncio.Queue
    total_partition_events: DefaultDict[int, int] = field(
        default_factory=lambda: defaultdict(lambda: -1)
    )
    total_events: int = 0
    # for now we assumed that 1 streams is connected to 1 topic
    consumer: Optional["test_clients.Consumer"] = None

    async def put(self, event: ConsumerRecord) -> None:
        await self.queue.put(event)

        # keep track of the amount of events per topic partition
        self.total_partition_events[event.partition] += 1
        self.total_events += 1

    async def get(self) -> ConsumerRecord:
        return await self.queue.get()

    def get_nowait(self) -> ConsumerRecord:
        return self.queue.get_nowait()

    def put_nowait(self, *, event: ConsumerRecord) -> None:
        return self.queue.put_nowait(event)

    def task_done(self) -> None:
        self.queue.task_done()

    async def join(self) -> None:
        await self.queue.join()

    def is_empty(self) -> bool:
        return self.queue.empty()

    def size(self) -> int:
        return self.queue.qsize()

    def offset(self, *, partition: int) -> int:
        return self.total_partition_events[partition]

    @property
    def consumed(self) -> bool:
        """
        We need to check if the Topic has a Consumer and if there are messages in it
        """
        return self.consumer is None or self.is_empty()

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return (
            f"Topic {self.name} with Consumer: {self.consumer}. "
            f"Messages in buffer: {self.queue.qsize()}"
        )


@dataclass
class TopicManager:
    # The queues will represent the kafka topics during testing
    # where the name is the topic name
    topics: ClassVar[Dict[str, Topic]] = {}

    @classmethod
    def get(cls, name: str) -> Topic:
        topic = cls.topics.get(name)

        if topic is not None:
            return topic
        raise ValueError(
            f"You might be trying to get the topic {name} outside the "
            "`client async context` or trying to get an event from an empty "
            f"topic {name}. Make sure that the code is inside the async context"
            "and the topic has events."
        )

    @classmethod
    def create(
        cls, name: str, consumer: Optional["test_clients.Consumer"] = None
    ) -> Topic:
        topic = Topic(name=name, queue=asyncio.Queue(), consumer=consumer)
        cls.topics[name] = topic
        return topic

    @classmethod
    def get_or_create(
        cls, name: str, consumer: Optional["test_clients.Consumer"] = None
    ) -> Tuple[Topic, bool]:
        """
        A convenience method for looking up Topic by name.
        If the topic does not exist a new one is created.

        Returns a tuple of (Topic, created), where Topic is the
        retrieved or created object and created is a boolean
        specifying whether a new Topic was created.
        """
        try:
            topic = cls.get(name)
            return topic, False
        except ValueError:
            topic = cls.create(name, consumer=consumer)
            return topic, True

    @classmethod
    def all_messages_consumed(cls) -> bool:
        """
        Check if all the messages has been consumed for ALL the topics
        """
        for topic in cls.topics.values():
            # if there is at least 1 topic with events we need to keep waiting
            if not topic.consumed:
                return False
        return True

    @classmethod
    async def join(cls) -> None:
        """
        Wait for all topic messages to be processed.
        Only topics that have a consumer assigned should be awaited.
        """
        await asyncio.gather(
            *[topic.join() for topic in cls.topics.values() if not topic.consumed]
        )

    @classmethod
    def clean(cls) -> None:
        cls.topics = {}
