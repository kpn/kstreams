from . import test_clients
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Optional

import asyncio


@dataclass
class Topic:
    name: str
    queue: asyncio.Queue
    total_messages: int = 0
    # for now we assumed that 1 streams is connected to 1 topic
    consumer: Optional["test_clients.Consumer"] = None

    async def put(self, event: Any) -> None:
        await self.queue.put(event)
        self.total_messages += 1

    async def get(self) -> Any:
        return await self.queue.get()

    def is_empty(self) -> bool:
        return self.queue.empty()

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
    def get_topic(cls, name: str) -> Optional[Topic]:
        return cls.topics.get(name)

    @classmethod
    def create_topic(
        cls, name: str, consumer: Optional["test_clients.Consumer"] = None
    ) -> Topic:
        topic = Topic(name=name, queue=asyncio.Queue(), consumer=consumer)
        cls.topics[name] = topic
        return topic

    @classmethod
    def get_or_create_topic(cls, name: str) -> Topic:
        """
        Add a new queue if does not exist and return it
        """
        topic = cls.get_topic(name)
        if topic is None:
            topic = cls.create_topic(name)
        return topic

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
