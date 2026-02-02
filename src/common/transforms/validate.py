"""
Validación de esquemas
"""
import apache_beam as beam
from src.common.utils.schema_loader import SchemaLoader
import logging

logger = logging.getLogger(__name__)


class ValidateSchema(beam.DoFn):
    """Valida registros contra su schema"""

    def __init__(self, schema_dir: str = "schemas"):
        self.schema_dir = schema_dir
        self.schema_loader = None

    def setup(self):
        """Inicializa el schema loader"""
        self.schema_loader = SchemaLoader(self.schema_dir)

    def process(self, element):
        """
        Valida un registro

        Args:
            element: Registro a validar

        Yields:
            dict: Registro válido o error
        """
        try:
            schema_name = element.get('schema', 'unknown')
            data = element.get('data', {})

            # Validar contra el schema
            is_valid, error_message = self.schema_loader.validate_record(schema_name, data)

            if is_valid:
                yield element
            else:
                logger.warning(f"Validation failed for schema {schema_name}: {error_message}")
                yield beam.pvalue.TaggedOutput('dlq', {
                    'error': error_message,
                    'record': element,
                    'error_type': 'validation_error',
                    'schema': schema_name
                })

        except Exception as e:
            logger.error(f"Error validating record: {e}")
            yield beam.pvalue.TaggedOutput('dlq', {
                'error': str(e),
                'record': str(element),
                'error_type': 'validation_exception'
            })
