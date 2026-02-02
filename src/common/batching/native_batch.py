"""
Batching nativo usando GroupIntoBatches de Beam
"""
import apache_beam as beam
from apache_beam.transforms.util import BatchElements
import logging

logger = logging.getLogger(__name__)


class NativeBatcher(beam.PTransform):
    """Usa el batching nativo de Beam"""

    def __init__(self, batch_size: int = 100, max_buffering_duration_secs: int = 30):
        self.batch_size = batch_size
        self.max_buffering_duration_secs = max_buffering_duration_secs

    def expand(self, pcoll):
        """
        Expande la transformación usando BatchElements

        Args:
            pcoll: PCollection de entrada

        Returns:
            PCollection: Batches de elementos
        """
        return (
            pcoll
            | "Batch Elements" >> BatchElements(
                min_batch_size=self.batch_size,
                max_batch_size=self.batch_size,
                target_batch_duration_secs=self.max_buffering_duration_secs
            )
        )


class AddBatchMetadata(beam.DoFn):
    """Agrega metadata a los batches"""

    def process(self, batch, window=beam.DoFn.WindowParam):
        """
        Agrega metadata al batch

        Args:
            batch: Lista de elementos
            window: Ventana actual

        Yields:
            dict: Batch con metadata
        """
        try:
            yield {
                'batch_size': len(batch),
                'elements': batch,
                'window_start': window.start.to_utc_datetime().isoformat(),
                'window_end': window.end.to_utc_datetime().isoformat(),
                'schema': batch[0].get('schema', 'unknown') if batch else 'unknown'
            }
        except Exception as e:
            logger.error(f"Error adding batch metadata: {e}")
            yield beam.pvalue.TaggedOutput('dlq', {
                'error': str(e),
                'batch_size': len(batch) if batch else 0,
                'error_type': 'batch_metadata_error'
            })
