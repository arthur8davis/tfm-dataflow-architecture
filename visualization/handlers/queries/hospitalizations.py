"""
Consultas de hospitalizaciones COVID-19
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


def get_hospitalizations_by_department():
    """Obtiene hospitalizaciones agrupadas por departamento."""
    if db is None:
        return []

    pipeline = [
        {"$match": {"sexo": {"$nin": [None, ""]}}},
        {
            "$group": {
                "_id": "$dep_domicilio",
                "total": {"$sum": 1}
            }
        },
        {"$sort": {"total": -1}},
        {"$limit": 25},
    ]

    results = []
    for r in db.hospitalizations.aggregate(pipeline):
        dept = r["_id"] or "Sin especificar"
        coords = DEPARTAMENTO_COORDS.get(dept.upper(), {"lat": None, "lon": None})
        results.append({
            "departamento": dept,
            "total": r["total"],
            "lat": coords["lat"],
            "lon": coords["lon"]
        })
    return results


def get_hospitalizations_heatmap_data():
    """Obtiene datos de hospitalizaciones para el mapa de calor."""
    if db is None:
        return []

    pipeline = [
        {"$match": {"sexo": {"$nin": [None, ""]}}},
        {
            "$group": {
                "_id": {
                    "departamento": "$dep_domicilio",
                    "provincia": "$prov_domicilio",
                    "distrito": "$dist_domicilio"
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
    for r in db.hospitalizations.aggregate(pipeline):
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


def get_filtered_hospitalizations_by_department(sexo: str = None):
    """Obtiene hospitalizaciones por departamento filtrado por sexo."""
    if db is None:
        return []

    match_filter = {"sexo": {"$nin": [None, ""]}}
    if sexo and sexo != "TODOS":
        match_filter["sexo"] = "M" if sexo == "MASCULINO" else "F"

    pipeline = [
        {"$match": match_filter},
        {
            "$group": {
                "_id": "$dep_domicilio",
                "total": {"$sum": 1}
            }
        },
        {"$sort": {"total": -1}},
        {"$limit": 25},
    ]

    results = []
    for r in db.hospitalizations.aggregate(pipeline):
        dept = r["_id"] or "Sin especificar"
        coords = DEPARTAMENTO_COORDS.get(dept.upper(), {"lat": None, "lon": None})
        results.append({
            "departamento": dept,
            "total": r["total"],
            "lat": coords["lat"],
            "lon": coords["lon"]
        })
    return results


def get_filtered_hospitalizations_heatmap(sexo: str = None, departamento: str = None):
    """Obtiene datos de heatmap de hospitalizaciones filtrados."""
    if db is None:
        return []

    match_filter = {"sexo": {"$nin": [None, ""]}}
    if sexo and sexo != "TODOS":
        match_filter["sexo"] = "M" if sexo == "MASCULINO" else "F"
    if departamento and departamento != "TODOS":
        match_filter["dep_domicilio"] = departamento

    pipeline = [
        {"$match": match_filter},
        {
            "$group": {
                "_id": {
                    "departamento": "$dep_domicilio",
                    "provincia": "$prov_domicilio",
                    "distrito": "$dist_domicilio"
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
    for r in db.hospitalizations.aggregate(pipeline):
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
