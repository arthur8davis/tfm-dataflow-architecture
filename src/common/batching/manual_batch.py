"""
Batching manual de registros
"""
import apache_beam as beam
from typing import List
import logging

logger = logging.getLogger(__name__)


class ManualBatcher(beam.DoFn):
    """Agrupa registros manualmente en batches"""

    def __init__(self, batch_size: int = 100):
        self.batch_size = batch_size
        self.buffer = []

    def process(self, element):
        """
        Acumula elementos y emite batches

        Args:
            element: Elemento individual

        Yields:
            List[dict]: Batch de elementos
        """
        self.buffer.append(element)

        if len(self.buffer) >= self.batch_size:
            yield self.buffer.copy()
            self.buffer.clear()

    def finish_bundle(self):
        """Emite el batch restante al finalizar"""
        if self.buffer:
            yield beam.utils.windowed_value.WindowedValue(
                value=self.buffer.copy(),
                timestamp=0,
                windows=[beam.window.GlobalWindow()]
            )
            self.buffer.clear()


class GroupIntoBatches(beam.PTransform):
    """Transform que agrupa elementos en batches por ventana"""

    def __init__(self, batch_size: int = 100, batch_timeout_seconds: int = 30):
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds

    def expand(self, pcoll):
        """
        Expande la transformación

        Args:
            pcoll: PCollection de entrada

        Returns:
            PCollection: Batches de elementos
        """
        return (
            pcoll
            | "Add Keys" >> beam.Map(lambda x: (x.get('schema', 'unknown'), x))
            | "Group by Schema" >> beam.GroupIntoBatches(self.batch_size)
            | "Extract Values" >> beam.Map(lambda kv: list(kv[1]))
        )
