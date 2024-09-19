import asyncio
import logging
from functools import wraps
from typing import Callable

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.cimpl import KafkaException
from pydantic import BaseModel, validate_call


class OrderSchema(BaseModel):
    id: int
    name: str


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s -> %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

CONNECTION_CONFIG = {
    'bootstrap.servers': '0.0.0.0:9092',
    'auto.offset.reset': 'smallest',
    'group.id': 'foo'
}

admin_client = AdminClient(CONNECTION_CONFIG)


def create_topic(new_topic: str) -> None:
    """Cria um novo tópico no servidor Kafka.

    Args:
        new_topic (str): tópico a ser criado
    """
    created_topics = admin_client.create_topics([NewTopic(
        topic=new_topic,
        num_partitions=1,
        replication_factor=1
    )])

    try:
        success_creation = created_topics[new_topic]
        success_creation.result()
    except KafkaException as topic_creation_error:
        # fazer validação pelo codigo para fazer a mensagem
        logging.info(f"Topico já está criado: {topic_creation_error.args[0].str()}")
    except Exception as error:
        logging.error(f"Erro ao criar o tópico Kafka: {str(error)}")
    else:
        logging.info(f"Tópico Kafka criado com sucesso: {new_topic}")


# @validate_call
def kafka_consumer(topic: str, validation_schema: BaseModel) -> Callable:
    """_summary_

    Args:
        topic (str): _description_
        validation_schema (BaseModel): _description_

    Returns:
        Callable: _description_
    """
    try:
        create_topic(new_topic=topic)
    except Exception:
        pass

    consumer = Consumer(CONNECTION_CONFIG)
    consumer.subscribe([topic])

    def _decorator(consumer_func):
        @wraps(consumer_func)
        async def _wraps(*args, **kwargs):
            while True:
                message = consumer.poll(timeout=0.2)
                if message is None:
                    continue
                elif message.error():
                    print(message.error())
                elif message.value():
                    await consumer_func(message=message)
                else:
                    continue
        _wraps.__wrapped__ = consumer_func
        return _wraps
    return _decorator


@kafka_consumer(topic="store.order", validation_schema=OrderSchema)
async def order_consumer(message):
    print(str(message.value()))


@kafka_consumer(topic="store.payment", validation_schema=OrderSchema)
async def payment_consumer(message):
    print(str(message.value()))


async def start_consumers():
    await asyncio.gather(
        order_consumer(),
        payment_consumer(),
    )


asyncio.run(start_consumers())
