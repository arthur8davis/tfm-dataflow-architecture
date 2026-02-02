"""
Asignación de timestamps
"""
import apache_beam as beam
from datetime import datetime
from apache_beam.transforms.window import TimestampedValue
import logging

logger = logging.getLogger(__name__)


class AssignTimestamp(beam.DoFn):
    """Asigna timestamps a los registros"""

    def __init__(self, timestamp_field: str = 'timestamp'):
        self.timestamp_field = timestamp_field

    def process(self, element):
        """
        Asigna timestamp a un registro

        Args:
            element: Registro

        Yields:
            TimestampedValue: Registro con timestamp
        """
        try:
            data = element.get('data', {})

            # Intentar obtener timestamp del registro
            timestamp = None
            if self.timestamp_field in data:
                timestamp = self._parse_timestamp(data[self.timestamp_field])

            # Si no hay timestamp, usar el actual
            if timestamp is None:
                timestamp = datetime.utcnow().timestamp()

            # Agregar timestamp al elemento
            element['timestamp'] = timestamp

            # Retornar con timestamp de Beam
            yield TimestampedValue(element, timestamp)

        except Exception as e:
            logger.error(f"Error assigning timestamp: {e}")
            yield beam.pvalue.TaggedOutput('dlq', {
                'error': str(e),
                'record': str(element),
                'error_type': 'timestamp_error'
            })

    @staticmethod
    def _parse_timestamp(value) -> float:
        """Parsea un timestamp de diferentes formatos"""
        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            try:
                # Intentar parsear ISO format
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                return dt.timestamp()
            except:
                try:
                    # Intentar otros formatos comunes
                    dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                    return dt.timestamp()
                except:
                    pass

        return None
