"""
Genera datos de anomalías y predicciones a partir de las métricas existentes
en MongoDB, simulando lo que haría la rama analítica del pipeline.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pymongo import MongoClient
from datetime import datetime, timezone

from src.common.transforms.anomaly_detector import AnomalyDetector
from src.analytics.scenarios import estimate_rt

CONN = 'mongodb://admin:admin123@localhost:27017/?authSource=admin&directConnection=true'
DB_NAME = 'covid-db'


def main():
    client = MongoClient(CONN)
    db = client[DB_NAME]

    # --- Leer métricas existentes ---
    metrics_cases = list(db.metrics_cases.find({}, {'_id': 0}).sort('window_start', 1))
    print(f"Métricas cases encontradas: {len(metrics_cases)}")

    if not metrics_cases:
        print("No hay métricas. Ejecuta el pipeline primero.")
        return

    # --- Generar anomalías ---
    detector = AnomalyDetector(window_history=20, z_threshold=2.0, cusum_k=0.5, cusum_h=4.0)
    anomalies = []
    for m in metrics_cases:
        for anomaly in detector.process(m):
            anomaly['pipeline_stage'] = 'seed_analytics'
            anomalies.append(anomaly)

    print(f"Anomalías detectadas: {len(anomalies)}")
    if anomalies:
        db.anomalies_cases.drop()
        db.anomalies_cases.insert_many(anomalies)
        db.anomalies_cases.create_index([('detected_at', -1)])
        db.anomalies_cases.create_index([('method', 1), ('severity', 1)])
        print(f"  -> Insertadas en anomalies_cases")

    # --- Generar predicciones ---
    history = []
    predictions = []
    for m in metrics_cases:
        total = m.get('total', 0)
        history.append(total)

        if len(history) >= 14:
            # Rt
            rt_values = estimate_rt(history)
            current_rt = rt_values[-1]['rt'] if rt_values else None

            # Tendencia
            recent_7d = history[-7:]
            previous_7d = history[-14:-7]
            avg_recent = sum(recent_7d) / len(recent_7d)
            avg_previous = sum(previous_7d) / len(previous_7d)
            growth_rate = (avg_recent - avg_previous) / avg_previous if avg_previous > 0 else 0.0

            # Forecast 7d
            forecast_7d = [
                max(0, avg_recent * (1 + growth_rate) ** (d / 7))
                for d in range(1, 8)
            ]

            if current_rt and current_rt < 1.0:
                trend = 'declining'
            elif current_rt and current_rt > 1.2:
                trend = 'increasing'
            else:
                trend = 'stable'

            prediction = {
                'schema': 'cases',
                'window_start': m.get('window_start'),
                'window_end': m.get('window_end'),
                'current_rt': round(current_rt, 4) if current_rt else None,
                'trend': trend,
                'growth_rate_7d': round(growth_rate, 4),
                'avg_7d': round(avg_recent, 2),
                'forecast_7d': [round(v, 0) for v in forecast_7d],
                'history_length': len(history),
                'predicted_at': m.get('computed_at', datetime.now(tz=timezone.utc).isoformat()),
            }
            predictions.append(prediction)

    print(f"Predicciones generadas: {len(predictions)}")
    if predictions:
        db.predictions_cases.drop()
        db.predictions_cases.insert_many(predictions)
        db.predictions_cases.create_index([('predicted_at', -1)])
        db.predictions_cases.create_index([('schema', 1)])
        print(f"  -> Insertadas en predictions_cases")

    # Crear colecciones vacías para demises (si hay métricas)
    metrics_demises = list(db.metrics_demises.find({}, {'_id': 0}).sort('window_start', 1))
    if not db.list_collection_names(filter={'name': 'anomalies_demises'}):
        db.create_collection('anomalies_demises')
    if not db.list_collection_names(filter={'name': 'predictions_demises'}):
        db.create_collection('predictions_demises')

    print(f"\nResumen final:")
    print(f"  anomalies_cases:    {db.anomalies_cases.count_documents({})} docs")
    print(f"  predictions_cases:  {db.predictions_cases.count_documents({})} docs")
    print(f"  anomalies_demises:  {db.anomalies_demises.count_documents({})} docs")
    print(f"  predictions_demises:{db.predictions_demises.count_documents({})} docs")

    client.close()
    print("\nListo. Recarga http://localhost:5006/metricas para ver los datos.")


if __name__ == '__main__':
    main()
