"""
Aplicación de ventanas temporales
"""
import apache_beam as beam
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode
import logging

logger = logging.getLogger(__name__)


def create_windowing_transform(window_size_seconds: int = 60,
                               allowed_lateness_seconds: int = 300,
                               trigger_type: str = "default"):
    """
    Crea una transformación de windowing

    Args:
        window_size_seconds: Tamaño de la ventana en segundos
        allowed_lateness_seconds: Latencia permitida en segundos
        trigger_type: Tipo de trigger ("default", "early", "late")

    Returns:
        WindowInto: Transform de windowing
    """

    # Crear ventana fija
    windowing = window.FixedWindows(window_size_seconds)

    # Seleccionar trigger
    if trigger_type == "early":
        # Trigger que emite temprano cada 30 segundos
        trigger = AfterWatermark(
            early=AfterProcessingTime(30),
            late=AfterProcessingTime(60)
        )
    elif trigger_type == "late":
        # Trigger que espera al watermark
        trigger = AfterWatermark(
            late=AfterProcessingTime(60)
        )
    else:
        # Trigger por defecto
        trigger = None

    # Crear WindowInto
    if trigger:
        return beam.WindowInto(
            windowing,
            trigger=trigger,
            accumulation_mode=AccumulationMode.DISCARDING,
            allowed_lateness=allowed_lateness_seconds
        )
    else:
        return beam.WindowInto(
            windowing,
            allowed_lateness=allowed_lateness_seconds
        )


class LogWindow(beam.DoFn):
    """DoFn para logging de ventanas (útil para debugging)"""

    def process(self, element, window=beam.DoFn.WindowParam):
        """
        Procesa elemento y logea información de la ventana

        Args:
            element: Elemento
            window: Ventana actual

        Yields:
            element: El mismo elemento
        """
        logger.debug(f"Element in window [{window.start}, {window.end}): {element.get('schema', 'unknown')}")
        yield element
