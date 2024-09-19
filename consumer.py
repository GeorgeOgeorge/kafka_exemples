import logging
from confluent_kafka import Consumer, Producer
from pydantic import BaseModel
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.cimpl import KafkaException

CONNECTION_CONFIG = {
    'bootstrap.servers': '0.0.0.0:9092',
    'auto.offset.reset': 'smallest',
    'group.id': 'foo'
}


class BaseConsumer:
    topic: str
    validation_schema: BaseModel
    _admin_client: AdminClient
    _consumer: Consumer

    def __init__(self, topic: str, validation_schema: BaseModel) -> None:
        self.topic = topic
        self.validation_schema = validation_schema
        self._admin_client = AdminClient(CONNECTION_CONFIG)
        self._create_topic()

    def _create_topic(self) -> None:
        """Cria um novo tópico no servidor Kafka."""
        created_topics = self._admin_client.create_topics([NewTopic(
            topic=self.topic,
            num_partitions=1,
            replication_factor=1
        )])

        try:
            success_creation = created_topics[self.topic]
            success_creation.result()
        except KafkaException as topic_creation_error:
            logging.info(f"Topico já está criado: {topic_creation_error.args[0].str()}")
        except Exception as error:
            logging.error(f"Erro ao criar o tópico Kafka: {str(error)}")
        else:
            logging.info(f"Tópico Kafka criado com sucesso: {self.topic}")

    async def start_consumer(self) -> None:
        """Consome as mensagens do tópico."""
        self._consumer = Consumer(CONNECTION_CONFIG)
        self._consumer.subscribe([self.topic])

        while True:
            message = self._consumer.poll(timeout=0.2)
            if message is None:
                continue
            elif message.error():
                logging.error(f"{self.topic} - erro ao ler mensagem: {message.error()}")
            elif message.value():
                try:
                    validated_message = self.validation_schema.model_validate_json(message.value())
                except Exception as validation_error:
                    logging.error(f"Erro ao validar schema da mensagem: {str(validation_error)}")
                else:
                    await self._message_handler(validated_message)

    async def _message_handler(self, message: BaseModel) -> None:
        """Manipula a mensagem recebida.

        Este método pode ser substituído por subclasses para implementar um comportamento específico.

        Args:
            message (BaseModel): A mensagem recebida.

        Raises:
            NotImplementedError: Se não for implementado por subclasses.
        """
        raise NotImplementedError("Please create a message handler function")


class OrderMessage(BaseModel):
    id: int
    item: str
    price: float


class OrderConsumer(BaseConsumer):
    topic = "city.order"

    def __init__(self) -> None:
        super().__init__(self.topic, OrderMessage)

    async def _message_handler(self, message: OrderMessage) -> None:
        logging.info(f"Nova mensagem de pedido recebida: {message.id}, {message.item}, {message.price}")
