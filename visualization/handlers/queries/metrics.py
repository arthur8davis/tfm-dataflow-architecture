"""
Consultas de métricas descriptivas por ventana temporal.
Lee de las colecciones metrics_cases y metrics_demises generadas por el pipeline.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services import db


def get_metrics_cases():
    """Obtiene las métricas por ventana del schema cases, ordenadas por ventana."""
    if db is None:
        return []

    results = []
    for doc in db.metrics_cases.find({}, {'_id': 0}).sort('window_start', 1):
        # Convertir top_departments si viene como lista de dicts
        top_depts = doc.get('top_departments', [])
        if top_depts and isinstance(top_depts[0], dict):
            doc['top_departments'] = top_depts
        elif top_depts and isinstance(top_depts[0], (list, tuple)):
            doc['top_departments'] = [
                {'department': d[0], 'count': d[1]} for d in top_depts
            ]
        results.append(doc)
    return results


def get_metrics_demises():
    """Obtiene las métricas por ventana del schema demises, ordenadas por ventana."""
    if db is None:
        return []

    results = []
    for doc in db.metrics_demises.find({}, {'_id': 0}).sort('window_start', 1):
        top_depts = doc.get('top_departments', [])
        if top_depts and isinstance(top_depts[0], dict):
            doc['top_departments'] = top_depts
        elif top_depts and isinstance(top_depts[0], (list, tuple)):
            doc['top_departments'] = [
                {'department': d[0], 'count': d[1]} for d in top_depts
            ]
        results.append(doc)
    return results


def get_metrics_summary():
    """Obtiene un resumen agregado de las métricas (última ventana y promedios)."""
    if db is None:
        return {}

    result = {}

    # Última métrica de cases
    last_cases = db.metrics_cases.find_one(
        {}, {'_id': 0}, sort=[('window_start', -1)]
    )
    if last_cases:
        top_depts = last_cases.get('top_departments', [])
        if top_depts and isinstance(top_depts[0], dict):
            last_cases['top_departments'] = top_depts
        elif top_depts and isinstance(top_depts[0], (list, tuple)):
            last_cases['top_departments'] = [
                {'department': d[0], 'count': d[1]} for d in top_depts
            ]
        result['cases_latest'] = last_cases

    # Promedios de cases
    pipeline_cases = [
        {'$group': {
            '_id': None,
            'total_windows': {'$sum': 1},
            'total_records': {'$sum': '$total'},
            'avg_positivity': {'$avg': '$positivity_rate'},
            'avg_age': {'$avg': '$avg_age'},
            'avg_male_ratio': {'$avg': '$male_ratio'},
        }}
    ]
    for agg in db.metrics_cases.aggregate(pipeline_cases):
        agg.pop('_id', None)
        result['cases_aggregated'] = agg

    # Última métrica de demises
    last_demises = db.metrics_demises.find_one(
        {}, {'_id': 0}, sort=[('window_start', -1)]
    )
    if last_demises:
        top_depts = last_demises.get('top_departments', [])
        if top_depts and isinstance(top_depts[0], dict):
            last_demises['top_departments'] = top_depts
        elif top_depts and isinstance(top_depts[0], (list, tuple)):
            last_demises['top_departments'] = [
                {'department': d[0], 'count': d[1]} for d in top_depts
            ]
        result['demises_latest'] = last_demises

    # Promedios de demises
    pipeline_demises = [
        {'$group': {
            '_id': None,
            'total_windows': {'$sum': 1},
            'total_records': {'$sum': '$total'},
            'avg_age': {'$avg': '$avg_age'},
            'avg_male_ratio': {'$avg': '$male_ratio'},
        }}
    ]
    for agg in db.metrics_demises.aggregate(pipeline_demises):
        agg.pop('_id', None)
        result['demises_aggregated'] = agg

    return result
