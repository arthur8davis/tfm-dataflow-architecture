"""
Consultas de fallecidos COVID-19
"""
import sys
from pathlib import Path

# Agregar paths para imports
VISUALIZATION_DIR = Path(__file__).parent.parent.parent
ROOT_DIR = VISUALIZATION_DIR.parent
sys.path.insert(0, str(VISUALIZATION_DIR))
sys.path.insert(0, str(ROOT_DIR))

from src.common.data.ubigeo_coords import get_coords_from_ubigeo
from services import db
from config import DEPARTAMENTO_COORDS


def get_demises_by_department():
    """Obtiene fallecidos agrupados por departamento."""
    if db is None:
        return []

    pipeline = [
        {"$match": {"sexo": {"$nin": [None, "", "Sin especificar"]}}},
        {
            "$group": {
                "_id": "$departamento",
                "total": {"$sum": 1},
                "lat": {"$max": "$latitud"},
                "lon": {"$max": "$longitud"}
            }
        },
        {"$sort": {"total": -1}},
        {"$limit": 25},
    ]

    results = []
    for r in db.demises.aggregate(pipeline):
        dept = r["_id"] or "Sin especificar"
        lat = r.get("lat")
        lon = r.get("lon")

        if lat is None or lon is None:
            coords = DEPARTAMENTO_COORDS.get(dept.upper(), {"lat": None, "lon": None})
            lat = coords["lat"]
            lon = coords["lon"]

        results.append({
            "departamento": dept,
            "total": r["total"],
            "lat": lat,
            "lon": lon
        })
    return results


def get_demises_by_sex(departamento: str = None):
    """Obtiene distribución de fallecidos por sexo."""
    if db is None:
        return []

    match_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}

    if departamento and departamento != "TODOS":
        match_filter["departamento"] = departamento

    pipeline = [
        {"$match": match_filter},
        {"$group": {"_id": "$sexo", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}}
    ]
    return [{"sexo": r["_id"], "total": r["total"]} for r in db.demises.aggregate(pipeline)]


def get_demises_by_date(departamento: str = None, sexo: str = None):
    """Obtiene fallecidos por fecha."""
    if db is None:
        return []

    match_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}

    if departamento and departamento != "TODOS":
        match_filter["departamento"] = departamento

    if sexo and sexo != "TODOS":
        match_filter["sexo"] = sexo

    pipeline = [
        {"$match": match_filter},
        {"$group": {"_id": "$fecha_fallecimiento", "total": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
        {"$limit": 100},
    ]
    return [{"fecha": r["_id"], "total": r["total"]} for r in db.demises.aggregate(pipeline) if r["_id"]]


def get_demises_heatmap_data():
    """Obtiene datos de fallecidos para el mapa de calor."""
    if db is None:
        return []

    pipeline = [
        {"$match": {"sexo": {"$nin": [None, "", "Sin especificar"]}}},
        {
            "$group": {
                "_id": {
                    "departamento": "$departamento",
                    "provincia": "$provincia",
                    "distrito": "$distrito"
                },
                "total": {"$sum": 1},
                "lat": {"$max": "$latitud"},
                "lon": {"$max": "$longitud"}
            }
        },
        {"$sort": {"total": -1}},
        {"$limit": 200}
    ]

    results = []
    for r in db.demises.aggregate(pipeline):
        departamento = r["_id"].get("departamento") or "Sin especificar"
        provincia = r["_id"].get("provincia") or ""
        distrito = r["_id"].get("distrito") or ""

        lat = r.get("lat")
        lon = r.get("lon")

        if lat is None or lon is None:
            coords = get_coords_from_ubigeo(departamento, provincia, distrito)
            if coords:
                lat = coords["lat"]
                lon = coords["lon"]

        if lat is not None and lon is not None:
            results.append({
                "departamento": departamento,
                "provincia": provincia,
                "distrito": distrito,
                "total": r["total"],
                "lat": lat,
                "lon": lon
            })

    return results


def get_filtered_demises_by_department(sexo: str = None):
    """Obtiene fallecidos por departamento filtrado por sexo."""
    if db is None:
        return []

    match_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}
    if sexo and sexo != "TODOS":
        match_filter["sexo"] = sexo

    pipeline = [
        {"$match": match_filter},
        {
            "$group": {
                "_id": "$departamento",
                "total": {"$sum": 1},
                "lat": {"$max": "$latitud"},
                "lon": {"$max": "$longitud"}
            }
        },
        {"$sort": {"total": -1}},
        {"$limit": 25},
    ]

    results = []
    for r in db.demises.aggregate(pipeline):
        dept = r["_id"] or "Sin especificar"
        lat = r.get("lat")
        lon = r.get("lon")

        if lat is None or lon is None:
            coords = DEPARTAMENTO_COORDS.get(dept.upper(), {"lat": None, "lon": None})
            lat = coords["lat"]
            lon = coords["lon"]

        results.append({
            "departamento": dept,
            "total": r["total"],
            "lat": lat,
            "lon": lon
        })
    return results


def get_filtered_demises_heatmap(sexo: str = None, departamento: str = None):
    """Obtiene datos de heatmap de fallecidos filtrados."""
    if db is None:
        return []

    match_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}
    if sexo and sexo != "TODOS":
        match_filter["sexo"] = sexo
    if departamento and departamento != "TODOS":
        match_filter["departamento"] = departamento

    pipeline = [
        {"$match": match_filter},
        {
            "$group": {
                "_id": {
                    "departamento": "$departamento",
                    "provincia": "$provincia",
                    "distrito": "$distrito"
                },
                "total": {"$sum": 1},
                "lat": {"$max": "$latitud"},
                "lon": {"$max": "$longitud"}
            }
        },
        {"$sort": {"total": -1}},
        {"$limit": 200}
    ]

    results = []
    for r in db.demises.aggregate(pipeline):
        departamento_r = r["_id"].get("departamento") or "Sin especificar"
        provincia = r["_id"].get("provincia") or ""
        distrito = r["_id"].get("distrito") or ""

        lat = r.get("lat")
        lon = r.get("lon")

        if lat is None or lon is None:
            coords = get_coords_from_ubigeo(departamento_r, provincia, distrito)
            if coords:
                lat = coords["lat"]
                lon = coords["lon"]

        if lat is not None and lon is not None:
            results.append({
                "departamento": departamento_r,
                "provincia": provincia,
                "distrito": distrito,
                "total": r["total"],
                "lat": lat,
                "lon": lon
            })

    return results
