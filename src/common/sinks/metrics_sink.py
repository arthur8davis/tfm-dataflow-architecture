"""
Sink para escribir métricas descriptivas por ventana a MongoDB.
"""
import apache_beam as beam
from pymongo import MongoClient
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)


class MetricsSink(beam.DoFn):
    """Escribe métricas de ventana a una colección estándar en MongoDB."""

    def __init__(self, connection_string: str, database: str, collection: str):
        self.connection_string = connection_string
        self.database_name = database
        self.collection_name = collection
        self.client = None
        self.db = None

    def setup(self):
        self.client = MongoClient(self.connection_string)
        self.db = self.client[self.database_name]
        self._ensure_collection()

    def _ensure_collection(self):
        collection = self.db[self.collection_name]
        collection.create_index([('window_start', 1), ('schema', 1)])
        collection.create_index([('computed_at', -1)])
        logger.info(f"Metrics collection ready: {self.collection_name}")

    def process(self, metrics):
        if metrics is None:
            return

        try:
            collection = self.db[self.collection_name]

            # Convertir top_departments de lista de tuplas a dict para MongoDB
            if 'top_departments' in metrics:
                metrics['top_departments'] = [
                    {'department': dept, 'count': count}
                    for dept, count in metrics['top_departments']
                ]

            result = collection.insert_one(metrics)
            logger.info(
                f"Metric written: {self.collection_name} "
                f"window={metrics.get('window_start')} "
                f"total={metrics.get('total')} "
                f"id={result.inserted_id}"
            )
            yield metrics

        except Exception as e:
            logger.error(f"Error writing metrics: {e}")

    def teardown(self):
        if self.client:
            self.client.close()
