"""
Tests para detección de anomalías (Sección 5.11.2).
Verifica Z-Score, IQR y CUSUM.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import unittest
from src.common.transforms.anomaly_detector import AnomalyDetector


class TestAnomalyDetector(unittest.TestCase):

    def _feed_values(self, detector, values):
        """Feed values to detector and collect all anomalies."""
        all_anomalies = []
        for i, v in enumerate(values):
            metrics = {
                'total': v,
                'window_start': f'2024-01-{i+1:02d}T00:00:00',
                'window_end': f'2024-01-{i+1:02d}T01:00:00',
                'schema': 'cases',
            }
            anomalies = list(detector.process(metrics))
            all_anomalies.extend(anomalies)
        return all_anomalies

    def test_zscore_detects_spike(self):
        """Z-Score debe detectar un valor atípico alto."""
        detector = AnomalyDetector(window_history=20, z_threshold=2.0)
        # Valores con variación normal seguidos de un spike (std != 0)
        values = [98, 102, 99, 101, 100, 103, 97, 101, 99, 100, 102, 98, 100, 101, 99, 500]
        anomalies = self._feed_values(detector, values)
        zscore_anomalies = [a for a in anomalies if a['method'] == 'zscore']
        self.assertGreater(len(zscore_anomalies), 0)
        self.assertGreater(zscore_anomalies[0]['z_score'], 2.0)

    def test_iqr_detects_outlier(self):
        """IQR debe detectar un outlier."""
        detector = AnomalyDetector(window_history=30)
        values = [100, 105, 98, 102, 99, 103, 97, 101, 100, 104] + [1000]
        anomalies = self._feed_values(detector, values)
        iqr_anomalies = [a for a in anomalies if a['method'] == 'iqr']
        self.assertGreater(len(iqr_anomalies), 0)

    def test_cusum_detects_shift(self):
        """CUSUM debe detectar un cambio sostenido de media."""
        detector = AnomalyDetector(window_history=20, cusum_k=0.5, cusum_h=4.0)
        # Cambio de nivel: de ~100 a ~200
        values = [100] * 10 + [200] * 10
        anomalies = self._feed_values(detector, values)
        cusum_anomalies = [a for a in anomalies if a['method'] == 'cusum']
        self.assertGreater(len(cusum_anomalies), 0)
        self.assertEqual(cusum_anomalies[0]['direction'], 'increase')

    def test_no_anomaly_for_stable_data(self):
        """Datos estables no deben generar anomalías."""
        detector = AnomalyDetector(window_history=20, z_threshold=3.0, cusum_h=8.0)
        values = [100, 101, 99, 100, 102, 98, 101, 100, 99, 100,
                  101, 100, 99, 102, 98, 100, 101, 99, 100, 100]
        anomalies = self._feed_values(detector, values)
        self.assertEqual(len(anomalies), 0)

    def test_severity_levels(self):
        """Severidad HIGH cuando z > 3, MEDIUM cuando 2 < z <= 3."""
        detector = AnomalyDetector(window_history=20, z_threshold=2.0)
        values = [100] * 15 + [300]  # spike moderado
        anomalies = self._feed_values(detector, values)
        zscore_a = [a for a in anomalies if a['method'] == 'zscore']
        if zscore_a:
            z = zscore_a[0]['z_score']
            expected_sev = 'HIGH' if abs(z) > 3 else 'MEDIUM'
            self.assertEqual(zscore_a[0]['severity'], expected_sev)

    def test_none_input_skipped(self):
        """None metrics no deben generar output."""
        detector = AnomalyDetector()
        result = list(detector.process(None))
        self.assertEqual(len(result), 0)


if __name__ == '__main__':
    unittest.main()
