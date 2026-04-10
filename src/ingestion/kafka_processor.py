"""
Procesador de ingesta que lee CSV/Parquet y envía a Kafka
"""
import json
import polars as pl
from pathlib import Path
from typing import Optional, List
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaProcessor:
    """Procesa archivos y los envía a Kafka"""

    def __init__(self, bootstrap_servers: str, producer_config: dict):
        """
        Inicializa el procesador de Kafka

        Args:
            bootstrap_servers: Servidores de Kafka
            producer_config: Configuración del productor
        """
        self.bootstrap_servers = bootstrap_servers
        config = {
            'bootstrap.servers': bootstrap_servers,
            **producer_config
        }
        self.producer = Producer(config)
        self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    def ensure_topic_exists(self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
        """Crea el topic si no existe"""
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            if topic_name not in metadata.topics:
                logger.info(f"Creating topic: {topic_name}")
                topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
                self.admin_client.create_topics([topic])
                logger.info(f"Topic {topic_name} created successfully")
            else:
                logger.info(f"Topic {topic_name} already exists")
        except Exception as e:
            logger.error(f"Error ensuring topic exists: {e}")

    def process_csv(self, file_path: str, topic: str, schema_name: str):
        """
        Procesa un archivo CSV y envía a Kafka

        Args:
            file_path: Ruta al archivo CSV
            topic: Topic de Kafka
            schema_name: Nombre del schema
        """
        logger.info(f"Processing CSV: {file_path} -> {topic}")

        # Leer CSV con Polars
        df = pl.read_csv(file_path)
        logger.info(f"Loaded {len(df)} records from {file_path}")

        # Enviar cada registro a Kafka
        for i, row in enumerate(df.iter_rows(named=True)):
            try:
                self._send_record(topic, schema_name, row)
            except BufferError:
                self.producer.flush()
                self._send_record(topic, schema_name, row)
            if i % 10000 == 0 and i > 0:
                self.producer.flush()

        # Flush para asegurar que todos los mensajes se envíen
        self.producer.flush()
        logger.info(f"Finished processing {file_path}")

    def process_parquet(self, file_path: str, topic: str, schema_name: str):
        """
        Procesa un archivo Parquet y envía a Kafka

        Args:
            file_path: Ruta al archivo Parquet
            topic: Topic de Kafka
            schema_name: Nombre del schema
        """
        logger.info(f"Processing Parquet: {file_path} -> {topic}")

        # Leer Parquet con Polars
        df = pl.read_parquet(file_path)
        logger.info(f"Loaded {len(df)} records from {file_path}")

        # Enviar cada registro a Kafka
        for row in df.iter_rows(named=True):
            self._send_record(topic, schema_name, row)

        # Flush para asegurar que todos los mensajes se envíen
        self.producer.flush()
        logger.info(f"Finished processing {file_path}")

    def process_directory(self, directory: str, topic: str, schema_name: str, file_pattern: str = "*"):
        """
        Procesa todos los archivos en un directorio

        Args:
            directory: Directorio con archivos
            topic: Topic de Kafka
            schema_name: Nombre del schema
            file_pattern: Patrón de archivos (ej: "*.csv")
        """
        path = Path(directory)

        # Procesar archivos CSV
        csv_files = list(path.glob("*.csv"))
        for file in csv_files:
            self.process_csv(str(file), topic, schema_name)

        # Procesar archivos Parquet
        parquet_files = list(path.glob("*.parquet"))
        for file in parquet_files:
            self.process_parquet(str(file), topic, schema_name)

        logger.info(f"Processed {len(csv_files)} CSV and {len(parquet_files)} Parquet files")

    def _send_record(self, topic: str, schema_name: str, record: dict):
        """Envía un registro a Kafka"""
        # Agregar metadata del schema
        message = {
            "schema": schema_name,
            "data": record
        }

        # Convertir a JSON
        value = json.dumps(message).encode('utf-8')
        key = schema_name.encode('utf-8')

        # Enviar a Kafka
        self.producer.produce(
            topic=topic,
            key=key,
            value=value,
            callback=self._delivery_callback
        )

        # Poll para procesar callbacks
        self.producer.poll(0)

    @staticmethod
    def _delivery_callback(err, msg):
        """Callback para confirmar entrega"""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def close(self):
        """Cierra el productor"""
        self.producer.flush()
        logger.info("KafkaProcessor closed")
