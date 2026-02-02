"""
Normalización de datos
"""
import apache_beam as beam
import logging

logger = logging.getLogger(__name__)


class NormalizeRecord(beam.DoFn):
    """Normaliza registros a un formato estándar"""

    def process(self, element):
        """
        Normaliza un registro

        Args:
            element: Registro de entrada

        Yields:
            dict: Registro normalizado
        """
        try:
            # Si el elemento ya tiene la estructura {schema, data}, mantenerla
            if isinstance(element, dict) and 'schema' in element and 'data' in element:
                normalized = element
            else:
                # Si es un registro simple, envolverlo
                normalized = {
                    'schema': element.get('schema', 'unknown'),
                    'data': element
                }

            # Normalizar valores nulos
            if 'data' in normalized:
                normalized['data'] = self._normalize_nulls(normalized['data'])

            # Convertir tipos de datos comunes
            if 'data' in normalized:
                normalized['data'] = self._normalize_types(normalized['data'])

            yield normalized

        except Exception as e:
            logger.error(f"Error normalizing record: {e}")
            yield beam.pvalue.TaggedOutput('dlq', {
                'error': str(e),
                'record': str(element),
                'error_type': 'normalization_error'
            })

    @staticmethod
    def _normalize_nulls(data: dict) -> dict:
        """Convierte valores nulos a None"""
        normalized = {}
        for key, value in data.items():
            if value in ['', 'NULL', 'null', 'NA', 'N/A', 'None']:
                normalized[key] = None
            else:
                normalized[key] = value
        return normalized

    @staticmethod
    def _normalize_types(data: dict) -> dict:
        """Normaliza tipos de datos"""
        normalized = {}
        for key, value in data.items():
            if value is None:
                normalized[key] = None
            elif isinstance(value, str):
                # Intentar convertir strings a números si es apropiado
                normalized[key] = value.strip()
            else:
                normalized[key] = value
        return normalized
