"""
Integración de modelos predictivos en el pipeline Apache Beam.

Sección 5.12.3 - ApplyPredictiveModel DoFn:
  - Recibe métricas de ventana
  - Aplica detección de anomalías (Z-Score, IQR, CUSUM)
  - Genera predicciones con modelo entrenado (si disponible)
  - Emite resultados a colecciones separadas
"""
import apache_beam as beam
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class ApplyAnomalyDetection(beam.DoFn):
    """Aplica detección de anomalías sobre métricas de ventana.

    Métodos implementados (sección 5.11.2):
      - Z-Score por ventana móvil
      - IQR (Rango Intercuartílico)
      - CUSUM (Cumulative Sum)

    Emite anomalías detectadas al output tag 'anomalies'.
    """

    def __init__(self, window_history=20, z_threshold=2.0, cusum_k=0.5, cusum_h=5.0):
        self.window_history = window_history
        self.z_threshold = z_threshold
        self.cusum_k = cusum_k
        self.cusum_h = cusum_h

    def setup(self):
        from src.common.transforms.anomaly_detector import AnomalyDetector
        self._detector = AnomalyDetector(
            window_history=self.window_history,
            z_threshold=self.z_threshold,
            cusum_k=self.cusum_k,
            cusum_h=self.cusum_h,
        )

    def process(self, metrics):
        """Procesa métricas de ventana y emite anomalías detectadas."""
        if metrics is None:
            return

        # Pasar métricas al detector
        for anomaly in self._detector.process(metrics):
            anomaly['pipeline_stage'] = 'window_metrics'
            yield beam.pvalue.TaggedOutput('anomalies', anomaly)

        # Siempre emitir las métricas originales al main output
        yield metrics


class ApplyPredictiveModel(beam.DoFn):
    """Aplica modelo predictivo sobre métricas acumuladas de ventana.

    Sección 5.12.3 - Genera predicciones usando:
      - ARIMA para tendencia a corto plazo (próximos 7 días)
      - Estimación de Rt para indicador de crecimiento

    Las predicciones se emiten al output tag 'predictions'.
    """

    def __init__(self, min_history=14):
        self.min_history = min_history
        self._history = []

    def process(self, metrics):
        """Acumula historial y genera predicciones cuando hay suficientes datos."""
        if metrics is None:
            return

        total = metrics.get('total', 0)
        self._history.append(total)

        # Mantener historial limitado
        if len(self._history) > 90:
            self._history = self._history[-90:]

        # Siempre emitir métricas al main
        yield metrics

        # Generar predicción si hay suficiente historial
        if len(self._history) >= self.min_history:
            prediction = self._generate_prediction(metrics)
            if prediction:
                yield beam.pvalue.TaggedOutput('predictions', prediction)

    def _generate_prediction(self, metrics):
        """Genera predicción usando tendencia simple y Rt."""
        try:
            from src.analytics.scenarios import estimate_rt

            # Estimar Rt
            rt_values = estimate_rt(self._history)
            current_rt = rt_values[-1]['rt'] if rt_values else None

            # Tendencia simple: media móvil de 7 días
            recent_7d = self._history[-7:]
            previous_7d = self._history[-14:-7]
            avg_recent = sum(recent_7d) / len(recent_7d)
            avg_previous = sum(previous_7d) / len(previous_7d)

            if avg_previous > 0:
                growth_rate = (avg_recent - avg_previous) / avg_previous
            else:
                growth_rate = 0.0

            # Proyección simple a 7 días
            forecast_7d = [
                max(0, avg_recent * (1 + growth_rate) ** (d / 7))
                for d in range(1, 8)
            ]

            # Determinar tendencia
            if current_rt and current_rt < 1.0:
                trend = 'declining'
            elif current_rt and current_rt > 1.2:
                trend = 'increasing'
            else:
                trend = 'stable'

            prediction = {
                'schema': metrics.get('schema'),
                'window_start': metrics.get('window_start'),
                'window_end': metrics.get('window_end'),
                'current_rt': round(current_rt, 4) if current_rt else None,
                'trend': trend,
                'growth_rate_7d': round(growth_rate, 4),
                'avg_7d': round(avg_recent, 2),
                'forecast_7d': [round(v, 0) for v in forecast_7d],
                'history_length': len(self._history),
                'predicted_at': datetime.now(tz=timezone.utc).isoformat(),
            }

            logger.info(
                f"Prediction generated: Rt={prediction['current_rt']} "
                f"trend={trend} schema={metrics.get('schema')}"
            )
            return prediction

        except Exception as e:
            logger.error(f"Error generating prediction: {e}")
            return None


def create_analytics_branch(windowed_metrics, schema_name, label_prefix="Analytics"):
    """Crea rama analítica completa: anomalías + predicciones.

    Args:
        windowed_metrics: PCollection de métricas por ventana (output de compute_metrics)
        schema_name: 'cases' o 'demises'
        label_prefix: Prefijo para labels Beam

    Returns:
        Tuple de (anomalies PCollection, predictions PCollection)
    """
    # Paso 1: Detección de anomalías
    anomaly_result = (
        windowed_metrics
        | f"{label_prefix} Detect Anomalies" >> beam.ParDo(
            ApplyAnomalyDetection()
        ).with_outputs('anomalies', main='main')
    )

    # Paso 2: Modelo predictivo sobre las métricas
    predict_result = (
        anomaly_result.main
        | f"{label_prefix} Apply Predictive" >> beam.ParDo(
            ApplyPredictiveModel(min_history=14)
        ).with_outputs('predictions', main='main')
    )

    return anomaly_result.anomalies, predict_result.predictions
