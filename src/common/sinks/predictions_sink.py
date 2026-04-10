"""
Sink para escribir predicciones a MongoDB.

Sección 5.12.3 - Persiste predicciones de modelos (Rt, forecast, tendencia).
"""
import apache_beam as beam
from pymongo import MongoClient
import logging

logger = logging.getLogger(__name__)


class PredictionsSink(beam.DoFn):
    """Escribe predicciones de modelos a MongoDB."""

    def __init__(self, connection_string: str, database: str, collection: str = 'predictions'):
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
        collection.create_index([('predicted_at', -1)])
        collection.create_index([('schema', 1), ('window_start', 1)])
        collection.create_index([('trend', 1)])
        logger.info(f"Predictions collection ready: {self.collection_name}")

    def process(self, prediction):
        if prediction is None:
            return
        try:
            result = self.db[self.collection_name].insert_one(prediction)
            logger.info(
                f"Prediction persisted: Rt={prediction.get('current_rt')} "
                f"trend={prediction.get('trend')} id={result.inserted_id}"
            )
            yield prediction
        except Exception as e:
            logger.error(f"Error writing prediction: {e}")

    def teardown(self):
        if self.client:
            self.client.close()
