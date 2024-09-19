import logging
from confluent_kafka import Producer
from pydantic import BaseModel


CONNECTION_CONFIG = {
    'bootstrap.servers': '0.0.0.0:9092',
    'auto.offset.reset': 'smallest',
    'group.id': 'foo'
}



class BaseProducer:
    topic: str
    _producer: Producer

    def __init__(self, topic: str) -> None:
        """Creates a new producer for the specified topic.

        Args:
            topic (str): The topic of the producer.
        """
        self.topic = topic
        self._producer = Producer({'bootstrap.servers': '0.0.0.0:9092'})

    def _check_delivery(self, error: Exception | None, message: object) -> None:
        """Checks if a message was successfully delivered.

        Args:
            error (Exception | None): The error that occurred during message publishing.
            message (object): The message that was attempted to be sent.
        """
        if error is None:
            logging.info(f"Message produced: {message.value()}")
        else:
            logging.error(f"Failed to deliver message: {message.value()}: {str(error)}")
            raise error

    def publish_message(self, sub_topic: str, data: BaseModel) -> None:
        """Publishes a new message to the sub-topic with the provided data.

        Args:
            sub_topic (str): The sub-topic to publish the message.
            data (BaseModel): The data to be sent.
        """
        try:
            self._producer.produce(
                topic=f"{self.topic}.{sub_topic}",
                value=data.model_dump_json().encode("utf-8"),
                on_delivery=self._check_delivery
            )
            self._producer.poll(1)
        except Exception as publish_message_error:
            raise publish_message_error
