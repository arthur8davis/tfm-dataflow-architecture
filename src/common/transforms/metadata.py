"""
Adición de metadata a los registros
"""
import apache_beam as beam
from datetime import datetime
import socket
import logging

logger = logging.getLogger(__name__)


class AddMetadata(beam.DoFn):
    """Agrega metadata a los registros"""

    def __init__(self, pipeline_version: str = "1.0.0", source_type: str = "unknown"):
        self.pipeline_version = pipeline_version
        self.source_type = source_type  # "kafka" o "storage"
        self.hostname = None

    def setup(self):
        """Inicializa metadata del worker"""
        self.hostname = socket.gethostname()

    def process(self, element, window=beam.DoFn.WindowParam):
        """
        Agrega metadata al registro

        Args:
            element: Registro
            window: Ventana actual

        Yields:
            dict: Registro con metadata
        """
        try:
            # Agregar metadata
            element['metadata'] = {
                'pipeline_version': self.pipeline_version,
                'source_type': self.source_type,  # "kafka" (streaming) o "storage" (batch)
                'processed_at': datetime.utcnow().isoformat(),
                'worker_host': self.hostname,
                'window_start': window.start.to_utc_datetime().isoformat(),
                'window_end': window.end.to_utc_datetime().isoformat(),
                'schema': element.get('schema', 'unknown')
            }

            yield element

        except Exception as e:
            logger.error(f"Error adding metadata: {e}")
            yield beam.pvalue.TaggedOutput('dlq', {
                'error': str(e),
                'record': str(element),
                'error_type': 'metadata_error'
            })
