"""
Sink para escribir anomalías detectadas a MongoDB.

Sección 5.11.2 - Persiste resultados de Z-Score, IQR y CUSUM.
"""
import apache_beam as beam
from pymongo import MongoClient
import logging

logger = logging.getLogger(__name__)


class AnomaliesSink(beam.DoFn):
    """Escribe anomalías detectadas a MongoDB."""

    def __init__(self, connection_string: str, database: str, collection: str = 'anomalies'):
        self.connection_string = connection_string
        self.database_name = database
        self.collection_name = collection
        self.client = None

    def setup(self):
        self.client = MongoClient(self.connection_string)
        self.db = self.client[self.database_name]
        self._ensure_collection()

    def _ensure_collection(self):
        collection = self.db[self.collection_name]
        collection.create_index([('detected_at', -1)])
        collection.create_index([('method', 1), ('severity', 1)])
        collection.create_index([('schema', 1), ('window_start', 1)])
        logger.info(f"Anomalies collection ready: {self.collection_name}")

    def process(self, anomaly):
        if anomaly is None:
            return
        try:
            result = self.db[self.collection_name].insert_one(anomaly)
            logger.warning(
                f"Anomaly persisted: method={anomaly.get('method')} "
                f"severity={anomaly.get('severity')} id={result.inserted_id}"
            )
            yield anomaly
        except Exception as e:
            logger.error(f"Error writing anomaly: {e}")

    def teardown(self):
        if self.client:
            self.client.close()
