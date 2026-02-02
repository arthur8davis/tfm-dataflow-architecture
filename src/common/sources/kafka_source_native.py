"""
Source nativo para leer desde Kafka usando confluent_kafka
Evita el uso de contenedores Docker que requiere ReadFromKafka de Beam
"""
import json
import apache_beam as beam
from confluent_kafka import Consumer, KafkaError, KafkaException
import logging
from typing import Dict, Any, Optional
import time

logger = logging.getLogger(__name__)


class KafkaConsumerDoFn(beam.DoFn):
    """
    DoFn que consume mensajes de Kafka usando confluent_kafka directamente.
    Se ejecuta como un source de Beam pero usa el cliente nativo de Kafka.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        consumer_config: Dict[str, Any],
        poll_timeout: float = 1.0,
        max_messages_per_batch: int = 1000,
        max_time_seconds: Optional[float] = None
    ):
        """
        Args:
            bootstrap_servers: Servidores de Kafka
            topic: Topic a consumir
            consumer_config: Configuración adicional del consumidor
            poll_timeout: Timeout para cada poll en segundos
            max_messages_per_batch: Máximo de mensajes por batch (para modo batch)
            max_time_seconds: Tiempo máximo de consumo en segundos (None = infinito para streaming)
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer_config = consumer_config
        self.poll_timeout = poll_timeout
        self.max_messages_per_batch = max_messages_per_batch
        self.max_time_seconds = max_time_seconds
        self.consumer = None

    def setup(self):
        """Inicializa el consumidor de Kafka"""
        # Propiedades que NO son válidas en confluent_kafka (son de Java client)
        invalid_properties = {
            'max.poll.records',  # Java only
            'max.poll.interval.ms',  # Use en confluent_kafka: max.poll.interval.ms está disponible
        }

        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.consumer_config.get('group.id', 'beam-consumer-group'),
            'auto.offset.reset': self.consumer_config.get('auto.offset.reset', 'earliest'),
            'enable.auto.commit': self.consumer_config.get('enable.auto.commit', 'false'),
        }

        # Agregar configuración adicional (filtrando propiedades inválidas)
        for key, value in self.consumer_config.items():
            if key not in config and key not in invalid_properties:
                # Convertir booleanos a strings si es necesario
                if isinstance(value, bool):
                    config[key] = str(value).lower()
                else:
                    config[key] = str(value)

        logger.info(f"Inicializando consumidor Kafka para topic: {self.topic}")
        self.consumer = Consumer(config)
        self.consumer.subscribe([self.topic])

    def process(self, element):
        """
        Procesa mensajes de Kafka.
        El 'element' es un trigger, no se usa realmente.

        Yields:
            dict: Mensaje parseado de Kafka
        """
        messages_consumed = 0
        start_time = time.time()

        try:
            while True:
                # Verificar límites
                if self.max_messages_per_batch and messages_consumed >= self.max_messages_per_batch:
                    logger.info(f"Alcanzado límite de mensajes: {messages_consumed}")
                    break

                if self.max_time_seconds:
                    elapsed = time.time() - start_time
                    if elapsed >= self.max_time_seconds:
                        logger.info(f"Alcanzado límite de tiempo: {elapsed:.2f}s")
                        break

                # Poll para obtener mensaje
                msg = self.consumer.poll(timeout=self.poll_timeout)

                if msg is None:
                    # No hay mensajes disponibles
                    if self.max_time_seconds is None:
                        # Modo streaming: continuar esperando
                        continue
                    else:
                        # Modo batch: si no hay mensajes, terminar
                        break

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Fin de la partición, continuar
                        logger.debug(f"Fin de partición {msg.partition()}")
                        if self.max_time_seconds:
                            break
                        continue
                    else:
                        raise KafkaException(msg.error())

                # Procesar mensaje
                try:
                    value = msg.value()
                    if value:
                        message = json.loads(value.decode('utf-8'))
                        messages_consumed += 1
                        yield message
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing JSON: {e}")
                    yield beam.pvalue.TaggedOutput('dlq', {
                        'error': str(e),
                        'raw_value': value.decode('utf-8') if value else None,
                        'error_type': 'parse_error',
                        'topic': self.topic,
                        'partition': msg.partition(),
                        'offset': msg.offset()
                    })
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    yield beam.pvalue.TaggedOutput('dlq', {
                        'error': str(e),
                        'raw_value': str(value),
                        'error_type': 'processing_error'
                    })

            # Commit después de procesar el batch
            self.consumer.commit(asynchronous=False)
            logger.info(f"Procesados {messages_consumed} mensajes de Kafka")

        except Exception as e:
            logger.error(f"Error en consumidor Kafka: {e}")
            raise

    def teardown(self):
        """Cierra el consumidor de Kafka"""
        if self.consumer:
            logger.info("Cerrando consumidor Kafka")
            self.consumer.close()


class ReadFromKafkaNative(beam.PTransform):
    """
    PTransform para leer de Kafka usando confluent_kafka nativo.
    Evita la necesidad de contenedores Docker.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        consumer_config: Dict[str, Any],
        max_messages: Optional[int] = None,
        max_time_seconds: Optional[float] = None
    ):
        """
        Args:
            bootstrap_servers: Servidores de Kafka
            topic: Topic a consumir
            consumer_config: Configuración del consumidor
            max_messages: Máximo de mensajes a leer (None = sin límite)
            max_time_seconds: Tiempo máximo de lectura en segundos
        """
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer_config = consumer_config
        self.max_messages = max_messages
        self.max_time_seconds = max_time_seconds

    def expand(self, pcoll):
        """Expande el PTransform"""
        return (
            pcoll
            | 'CreateTrigger' >> beam.Create([None])  # Trigger para iniciar
            | 'ConsumeKafka' >> beam.ParDo(
                KafkaConsumerDoFn(
                    bootstrap_servers=self.bootstrap_servers,
                    topic=self.topic,
                    consumer_config=self.consumer_config,
                    max_messages_per_batch=self.max_messages,
                    max_time_seconds=self.max_time_seconds
                )
            ).with_outputs('dlq', main='main')
        )


def create_kafka_source_native(
    bootstrap_servers: str,
    topic: str,
    consumer_config: Dict[str, Any],
    max_messages: Optional[int] = None,
    max_time_seconds: Optional[float] = None
) -> ReadFromKafkaNative:
    """
    Crea un source nativo de Kafka usando confluent_kafka.

    Args:
        bootstrap_servers: Servidores de Kafka
        topic: Topic a leer
        consumer_config: Configuración del consumidor
        max_messages: Máximo de mensajes (para modo batch)
        max_time_seconds: Tiempo máximo de lectura

    Returns:
        ReadFromKafkaNative: Transform de lectura nativa de Kafka
    """
    return ReadFromKafkaNative(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        consumer_config=consumer_config,
        max_messages=max_messages,
        max_time_seconds=max_time_seconds
    )
