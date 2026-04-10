"""
Consultas de anomalías detectadas por el pipeline.
Sección 5.11.2 - Z-Score, IQR, CUSUM
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services import db


def get_anomalies(schema='cases', limit=100):
    """Obtiene anomalías detectadas, ordenadas por fecha descendente."""
    if db is None:
        return []

    collection_name = f'anomalies_{schema}'
    results = []
    for doc in db[collection_name].find(
        {}, {'_id': 0}
    ).sort('detected_at', -1).limit(limit):
        results.append(doc)
    return results


def get_anomalies_summary():
    """Resumen de anomalías por método y severidad."""
    if db is None:
        return {}

    result = {}
    for schema in ['cases', 'demises']:
        collection_name = f'anomalies_{schema}'
        pipeline = [
            {'$group': {
                '_id': {'method': '$method', 'severity': '$severity'},
                'count': {'$sum': 1},
                'last_detected': {'$max': '$detected_at'},
            }},
            {'$sort': {'count': -1}}
        ]
        agg_results = []
        for doc in db[collection_name].aggregate(pipeline):
            agg_results.append({
                'method': doc['_id']['method'],
                'severity': doc['_id']['severity'],
                'count': doc['count'],
                'last_detected': doc['last_detected'],
            })
        result[schema] = {
            'total': sum(a['count'] for a in agg_results),
            'by_method': agg_results,
        }
    return result
