"""
Consultas de predicciones generadas por el pipeline.
Sección 5.12.3 - Rt, forecast, tendencia
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services import db


def get_predictions(schema='cases', limit=50):
    """Obtiene predicciones recientes, ordenadas por fecha descendente."""
    if db is None:
        return []

    collection_name = f'predictions_{schema}'
    results = []
    for doc in db[collection_name].find(
        {}, {'_id': 0}
    ).sort('predicted_at', -1).limit(limit):
        results.append(doc)
    return results


def get_predictions_latest(schema='cases'):
    """Obtiene la predicción más reciente."""
    if db is None:
        return None

    collection_name = f'predictions_{schema}'
    doc = db[collection_name].find_one(
        {}, {'_id': 0}, sort=[('predicted_at', -1)]
    )
    return doc


def get_predictions_summary():
    """Resumen de predicciones: último Rt y tendencia por schema."""
    if db is None:
        return {}

    result = {}
    for schema in ['cases', 'demises']:
        latest = get_predictions_latest(schema)
        if latest:
            result[schema] = {
                'current_rt': latest.get('current_rt'),
                'trend': latest.get('trend'),
                'growth_rate_7d': latest.get('growth_rate_7d'),
                'avg_7d': latest.get('avg_7d'),
                'forecast_7d': latest.get('forecast_7d'),
                'predicted_at': latest.get('predicted_at'),
            }
    return result
