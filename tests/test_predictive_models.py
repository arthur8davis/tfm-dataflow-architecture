"""
Tests para modelos predictivos y feature engineering (Sección 5.12).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import unittest
import numpy as np
import pandas as pd


class TestFeatureEngineering(unittest.TestCase):

    def test_create_daily_series(self):
        from src.analytics.feature_engineering import create_daily_series
        df = pd.DataFrame({
            'fecha': pd.date_range('2024-01-01', periods=30, freq='D').repeat(10),
            'resultado': ['POSITIVO'] * 150 + ['NEGATIVO'] * 150,
        })
        daily = create_daily_series(df)
        self.assertEqual(len(daily), 30)
        self.assertIn('total', daily.columns)
        self.assertIn('positivos', daily.columns)
        self.assertEqual(daily['total'].sum(), 300)

    def test_add_derived_features(self):
        from src.analytics.feature_engineering import add_derived_features
        daily = pd.DataFrame({
            'ds': pd.date_range('2024-01-01', periods=14),
            'total': [100 + i for i in range(14)],
            'positivos': [50 + i for i in range(14)],
        })
        result = add_derived_features(daily)
        self.assertIn('dia_semana', result.columns)
        self.assertIn('mes', result.columns)
        self.assertIn('casos_7d_rolling', result.columns)
        self.assertIn('tasa_positividad_rolling', result.columns)
        self.assertIn('lag_1d', result.columns)
        self.assertIn('lag_7d', result.columns)

    def test_create_age_groups(self):
        from src.analytics.feature_engineering import create_age_groups
        ages = pd.Series([5, 25, 45, 65, 85])
        groups = create_age_groups(ages)
        self.assertEqual(list(groups), ['0-19', '20-39', '40-59', '60-79', '80+'])

    def test_prepare_classification_features(self):
        from src.analytics.feature_engineering import prepare_classification_features
        df = pd.DataFrame({
            'edad': [30, 50, 70],
            'sexo': ['MASCULINO', 'FEMENINO', 'MASCULINO'],
            'resultado': ['POSITIVO', 'NEGATIVO', 'POSITIVO'],
        })
        result = prepare_classification_features(df)
        self.assertIn('target', result.columns)
        self.assertIn('grupo_edad', result.columns)
        self.assertEqual(result['target'].sum(), 2)


class TestPredictiveModels(unittest.TestCase):

    def test_evaluate_classifier(self):
        from src.analytics.predictive_models import evaluate_classifier
        from sklearn.dummy import DummyClassifier
        X = np.random.rand(100, 3)
        y = np.random.randint(0, 2, 100)
        model = DummyClassifier(strategy='most_frequent')
        model.fit(X, y)
        metrics = evaluate_classifier(model, X, y)
        self.assertIn('accuracy', metrics)
        self.assertIn('precision', metrics)
        self.assertIn('recall', metrics)
        self.assertIn('f1', metrics)

    def test_isolation_forest(self):
        from src.analytics.predictive_models import train_isolation_forest
        X = np.random.rand(100, 3)
        model = train_isolation_forest(X, contamination=0.1)
        predictions = model.predict(X)
        self.assertEqual(len(predictions), 100)
        # -1 for anomaly, 1 for normal
        self.assertTrue(set(predictions).issubset({-1, 1}))

    def test_autoencoder_anomalies(self):
        from src.analytics.predictive_models import train_autoencoder, detect_autoencoder_anomalies
        X = np.random.rand(100, 5)
        model = train_autoencoder(X, encoding_dim=2, epochs=20)
        result = detect_autoencoder_anomalies(model, X)
        self.assertIn('anomaly_indices', result)
        self.assertIn('reconstruction_errors', result)
        self.assertIn('threshold', result)
        self.assertEqual(len(result['reconstruction_errors']), 100)

    def test_save_load_model(self):
        import tempfile, os
        from src.analytics.predictive_models import save_model, load_model
        from sklearn.dummy import DummyClassifier
        model = DummyClassifier(strategy='most_frequent')
        model.fit([[1], [2]], [0, 1])

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'model.pkl')
            save_model(model, path)
            loaded = load_model(path)
            self.assertEqual(loaded.predict([[1]])[0], model.predict([[1]])[0])


if __name__ == '__main__':
    unittest.main()
