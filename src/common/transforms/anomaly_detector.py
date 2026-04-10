"""
Detección de anomalías en métricas por ventana temporal.

Sección 5.11.2 - Implementa tres métodos:
  - Z-Score por ventana móvil
  - IQR (Rango Intercuartílico)
  - CUSUM (Cumulative Sum)
"""
import apache_beam as beam
import math
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class AnomalyDetector(beam.DoFn):
    """Detecta anomalías en métricas por ventana usando Z-Score, IQR y CUSUM."""

    def __init__(self, window_history=20, z_threshold=2.0, cusum_k=0.5, cusum_h=5.0):
        self.window_history = window_history
        self.z_threshold = z_threshold
        self.cusum_k = cusum_k
        self.cusum_h = cusum_h
        self.history = []
        self.cusum_pos = 0.0
        self.cusum_neg = 0.0

    def process(self, metrics):
        if metrics is None:
            return

        count = metrics.get('total', 0)
        self.history.append(count)

        if len(self.history) > self.window_history:
            self.history = self.history[-self.window_history:]

        anomalies = []

        # Z-Score
        z_result = self._detect_zscore(count)
        if z_result:
            anomalies.append(z_result)

        # IQR
        iqr_result = self._detect_iqr(count)
        if iqr_result:
            anomalies.append(iqr_result)

        # CUSUM
        cusum_result = self._detect_cusum(count)
        if cusum_result:
            anomalies.append(cusum_result)

        for anomaly in anomalies:
            anomaly.update({
                'window_start': metrics.get('window_start'),
                'window_end': metrics.get('window_end'),
                'schema': metrics.get('schema'),
                'actual_value': count,
                'detected_at': datetime.now(tz=timezone.utc).isoformat(),
            })
            logger.warning(
                f"Anomaly detected: {anomaly['method']} | "
                f"schema={anomaly['schema']} value={count} "
                f"severity={anomaly['severity']}"
            )
            yield anomaly

    def _detect_zscore(self, current):
        """Z-Score: z = (x - μ) / σ. Anomalía si |z| > threshold."""
        if len(self.history) < 5:
            return None

        past = self.history[:-1]
        mean = sum(past) / len(past)
        variance = sum((x - mean) ** 2 for x in past) / len(past)
        std = math.sqrt(variance)

        if std == 0:
            return None

        z_score = (current - mean) / std

        if abs(z_score) > self.z_threshold:
            return {
                'method': 'zscore',
                'metric': 'case_count',
                'z_score': round(z_score, 4),
                'mean': round(mean, 2),
                'std': round(std, 2),
                'severity': 'HIGH' if abs(z_score) > 3 else 'MEDIUM',
            }
        return None

    def _detect_iqr(self, current):
        """IQR: outlier si x < Q1 - 1.5*IQR o x > Q3 + 1.5*IQR."""
        if len(self.history) < 8:
            return None

        sorted_vals = sorted(self.history[:-1])
        n = len(sorted_vals)
        q1 = sorted_vals[n // 4]
        q3 = sorted_vals[3 * n // 4]
        iqr = q3 - q1

        if iqr == 0:
            return None

        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        if current < lower or current > upper:
            return {
                'method': 'iqr',
                'metric': 'case_count',
                'q1': round(q1, 2),
                'q3': round(q3, 2),
                'iqr': round(iqr, 2),
                'lower_bound': round(lower, 2),
                'upper_bound': round(upper, 2),
                'severity': 'HIGH' if current > q3 + 3 * iqr else 'MEDIUM',
            }
        return None

    def _detect_cusum(self, current):
        """CUSUM: S_t = max(0, S_{t-1} + (x_t - μ₀) - k). Alerta si S_t > h."""
        if len(self.history) < 5:
            return None

        past = self.history[:-1]
        mean = sum(past) / len(past)
        std = math.sqrt(sum((x - mean) ** 2 for x in past) / len(past))

        if std == 0:
            return None

        k = self.cusum_k * std
        h = self.cusum_h * std

        self.cusum_pos = max(0, self.cusum_pos + (current - mean) - k)
        self.cusum_neg = max(0, self.cusum_neg - (current - mean) - k)

        if self.cusum_pos > h or self.cusum_neg > h:
            direction = 'increase' if self.cusum_pos > h else 'decrease'
            result = {
                'method': 'cusum',
                'metric': 'case_count',
                'cusum_pos': round(self.cusum_pos, 2),
                'cusum_neg': round(self.cusum_neg, 2),
                'threshold_h': round(h, 2),
                'direction': direction,
                'severity': 'HIGH',
            }
            # Reset después de alerta
            if self.cusum_pos > h:
                self.cusum_pos = 0.0
            if self.cusum_neg > h:
                self.cusum_neg = 0.0
            return result
        return None
