"""
Dead Letter Queue Sink para registros con errores
"""
import apache_beam as beam
from pymongo import MongoClient
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DLQSink(beam.DoFn):
    """Escribe registros con errores a la Dead Letter Queue"""

    def __init__(self, connection_string: str, database: str, dlq_collection: str = "dead_letter_queue"):
        self.connection_string = connection_string
        self.database_name = database
        self.dlq_collection_name = dlq_collection
        self.client = None
        self.db = None
        self.collection = None

    def setup(self):
        """Inicializa conexión a MongoDB"""
        self.client = MongoClient(self.connection_string)
        self.db = self.client[self.database_name]
        self.collection = self.db[self.dlq_collection_name]

        # Crear índices para búsquedas eficientes
        self.collection.create_index([("error_type", 1)])
        self.collection.create_index([("timestamp", -1)])
        self.collection.create_index([("schema", 1)])

    def process(self, element):
        """
        Escribe un registro de error a la DLQ

        Args:
            element: Registro con error

        Yields:
            dict: Resultado de la escritura
        """
        try:
            # Preparar documento DLQ
            dlq_doc = {
                'error': element.get('error', 'Unknown error'),
                'error_type': element.get('error_type', 'unknown'),
                'timestamp': datetime.utcnow(),
                'schema': element.get('schema', 'unknown'),
                'record': element.get('record', {}),
                'raw_value': element.get('raw_value'),
                'file_path': element.get('file_path'),
                'batch_size': element.get('batch_size'),
            }

            # Insertar en DLQ
            result = self.collection.insert_one(dlq_doc)

            logger.warning(f"Error record inserted to DLQ: {element.get('error_type', 'unknown')}")

            yield {
                'dlq_id': str(result.inserted_id),
                'error_type': element.get('error_type', 'unknown')
            }

        except Exception as e:
            logger.error(f"Error writing to DLQ: {e}")
            # Si falla la escritura a DLQ, loguear el error
            logger.error(f"Failed DLQ record: {element}")

    def teardown(self):
        """Cierra conexión a MongoDB"""
        if self.client:
            self.client.close()


class CombineDLQErrors(beam.PTransform):
    """Combina múltiples salidas DLQ en una sola"""

    def __init__(self, *dlq_outputs):
        self.dlq_outputs = dlq_outputs

    def expand(self, pipeline):
        """
        Combina todas las salidas DLQ

        Args:
            pipeline: Pipeline

        Returns:
            PCollection: DLQ combinada
        """
        # Filtrar None values
        valid_outputs = [output for output in self.dlq_outputs if output is not None]

        if not valid_outputs:
            return pipeline | "Create Empty DLQ" >> beam.Create([])

        if len(valid_outputs) == 1:
            return valid_outputs[0]

        # Combinar múltiples outputs
        return valid_outputs | "Flatten DLQ" >> beam.Flatten()
