"""
Sink para escribir a MongoDB
"""
import apache_beam as beam
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
from typing import List, Dict, Any
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)


class MongoDBSink(beam.DoFn):
    """Escribe batches a MongoDB con soporte para time series"""

    def __init__(self, connection_string: str, database: str, collection_config: dict):
        self.connection_string = connection_string
        self.database_name = database
        self.collection_config = collection_config
        self.client = None
        self.db = None

    def setup(self):
        """Inicializa conexión a MongoDB"""
        self.client = MongoClient(self.connection_string)
        self.db = self.client[self.database_name]

        # Crear colección de time series si no existe
        self._ensure_timeseries_collection()

    def _ensure_timeseries_collection(self):
        """Crea la colección de time series si no existe"""
        collection_name = self.collection_config.get('name')

        if collection_name not in self.db.list_collection_names():
            timeseries_config = self.collection_config.get('timeseries', {})

            try:
                self.db.create_collection(
                    collection_name,
                    timeseries={
                        'timeField': timeseries_config.get('timeField', 'timestamp'),
                        'metaField': timeseries_config.get('metaField', 'metadata'),
                        'granularity': timeseries_config.get('granularity', 'hours')
                    }
                )
                logger.info(f"Created time series collection: {collection_name}")
            except Exception as e:
                logger.warning(f"Collection might already exist: {e}")

    def process(self, batch):
        """
        Escribe un batch a MongoDB

        Args:
            batch: Lista de documentos o batch con metadata

        Yields:
            dict: Resultado de la escritura
        """
        try:
            collection_name = self.collection_config.get('name')
            collection = self.db[collection_name]

            # Extraer elementos del batch
            if isinstance(batch, dict) and 'elements' in batch:
                documents = batch['elements']
            elif isinstance(batch, list):
                documents = batch
            else:
                documents = [batch]

            # Preparar documentos para MongoDB
            mongo_docs = []
            for doc in documents:
                mongo_doc = self._prepare_document(doc)
                if mongo_doc:
                    mongo_docs.append(mongo_doc)

            if not mongo_docs:
                logger.warning("No documents to insert")
                return

            # Insertar batch
            result = collection.insert_many(mongo_docs, ordered=False)

            yield {
                'inserted_count': len(result.inserted_ids),
                'collection': collection_name,
                'success': True
            }

            logger.info(f"Inserted {len(result.inserted_ids)} documents to {collection_name}")

        except BulkWriteError as bwe:
            # Manejar errores parciales
            inserted = bwe.details.get('nInserted', 0)
            write_errors = bwe.details.get('writeErrors', [])
            logger.error(f"Bulk write error. Inserted: {inserted}, Errors: {len(write_errors)}")
            # Mostrar el primer error para diagnóstico
            if write_errors:
                first_error = write_errors[0]
                logger.error(f"First error: {first_error.get('errmsg', 'Unknown')}")

            yield {
                'inserted_count': inserted,
                'collection': collection_name,
                'success': False,
                'error': str(bwe)
            }

            # Enviar errores a DLQ
            for error in bwe.details.get('writeErrors', []):
                yield beam.pvalue.TaggedOutput('dlq', {
                    'error': error.get('errmsg', 'Unknown error'),
                    'document': str(error.get('op', {})),
                    'error_type': 'mongo_write_error'
                })

        except Exception as e:
            logger.error(f"Error writing to MongoDB: {e}")
            yield beam.pvalue.TaggedOutput('dlq', {
                'error': str(e),
                'batch_size': len(documents) if documents else 0,
                'error_type': 'mongo_connection_error'
            })

    @staticmethod
    def _prepare_document(doc: dict) -> dict:
        """Prepara un documento para MongoDB time series"""
        if 'data' not in doc:
            return None

        # Extraer datos y metadata
        data = doc.get('data', {})
        metadata = doc.get('metadata', {})
        timestamp = doc.get('timestamp')

        # Convertir timestamp a datetime para MongoDB time series
        if isinstance(timestamp, (int, float)):
            timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        elif timestamp is None:
            timestamp = datetime.now(tz=timezone.utc)

        # Crear documento para time series
        mongo_doc = {
            **data,
            'timestamp': timestamp,
            'metadata': metadata
        }

        return mongo_doc

    def teardown(self):
        """Cierra conexión a MongoDB"""
        if self.client:
            self.client.close()
