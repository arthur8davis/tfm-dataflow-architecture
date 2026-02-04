"""
Consultas de casos COVID-19
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


def get_cases_by_department():
    """Obtiene casos agrupados por departamento."""
    if db is None:
        return []

    pipeline = [
        {"$match": {"sexo": {"$nin": [None, "", "Sin especificar"]}}},
        {
            "$group": {
                "_id": "$departamento_paciente",
                "total": {"$sum": 1},
                "positivos": {"$sum": {"$cond": [{"$eq": ["$resultado", "POSITIVO"]}, 1, 0]}}
            }
        },
        {"$sort": {"total": -1}},
        {"$limit": 25},
    ]

    results = []
    for r in db.cases.aggregate(pipeline):
        dept = r["_id"] or "Sin especificar"
        coords = DEPARTAMENTO_COORDS.get(dept.upper(), {"lat": None, "lon": None})
        results.append({
            "departamento": dept,
            "total": r["total"],
            "positivos": r["positivos"],
            "lat": coords["lat"],
            "lon": coords["lon"]
        })
    return results


def get_cases_by_sex(departamento: str = None):
    """Obtiene distribución por sexo - solo casos positivos."""
    if db is None:
        return []

    match_filter = {
        "sexo": {"$nin": [None, "", "Sin especificar"]},
        "resultado": "POSITIVO"
    }

    if departamento and departamento != "TODOS":
        match_filter["departamento_paciente"] = departamento

    pipeline = [
        {"$match": match_filter},
        {"$group": {"_id": "$sexo", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}}
    ]
    return [{"sexo": r["_id"], "total": r["total"]} for r in db.cases.aggregate(pipeline)]


def get_cases_by_date(departamento: str = None, sexo: str = None):
    """Obtiene casos confirmados por fecha."""
    if db is None:
        return []

    match_filter = {
        "sexo": {"$nin": [None, "", "Sin especificar"]},
        "resultado": "POSITIVO"
    }

    if departamento and departamento != "TODOS":
        match_filter["departamento_paciente"] = departamento

    if sexo and sexo != "TODOS":
        match_filter["sexo"] = sexo

    pipeline = [
        {"$match": match_filter},
        {"$group": {"_id": "$fecha_muestra", "total": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
        {"$limit": 100},
    ]
    return [
        {"fecha": r["_id"], "total": r["total"], "positivos": r["total"]}
        for r in db.cases.aggregate(pipeline) if r["_id"]
    ]


def get_cases_by_age(departamento: str = None):
    """Obtiene casos positivos agrupados por grupo de edad."""
    if db is None:
        return []

    match_filter = {
        "sexo": {"$nin": [None, "", "Sin especificar"]},
        "resultado": "POSITIVO"
    }

    if departamento and departamento != "TODOS":
        match_filter["departamento_paciente"] = departamento

    pipeline = [
        {"$match": match_filter},
        {
            "$bucket": {
                "groupBy": "$edad",
                "boundaries": [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
                "default": "100+",
                "output": {
                    "total": {"$sum": 1}
                }
            }
        }
    ]

    def label(r):
        return f"{r['_id']}-{r['_id']+9}" if isinstance(r["_id"], int) else r["_id"]

    return [
        {"grupo": label(r), "total": r["total"]}
        for r in db.cases.aggregate(pipeline)
    ]


def get_heatmap_data():
    """Obtiene datos para el mapa de calor de casos."""
    if db is None:
        return []

    pipeline = [
        {"$match": {"sexo": {"$nin": [None, "", "Sin especificar"]}}},
        {
            "$group": {
                "_id": {
                    "departamento": "$departamento_muestra",
                    "provincia": "$provincia_muestra",
                    "distrito": "$distrito_muestra"
                },
                "total": {"$sum": 1},
                "positivos": {"$sum": {"$cond": [{"$eq": ["$resultado", "POSITIVO"]}, 1, 0]}},
                "lat": {"$max": "$latitud"},
                "lon": {"$max": "$longitud"}
            }
        },
        {"$sort": {"total": -1}},
        {"$limit": 200}
    ]

    results = []
    for r in db.cases.aggregate(pipeline):
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
                "positivos": r["positivos"],
                "lat": lat,
                "lon": lon
            })

    return results


def get_filtered_cases_by_department(sexo: str = None):
    """Obtiene casos por departamento filtrado por sexo."""
    if db is None:
        return []

    match_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}
    if sexo and sexo != "TODOS":
        match_filter["sexo"] = sexo

    pipeline = [
        {"$match": match_filter},
        {
            "$group": {
                "_id": "$departamento_paciente",
                "total": {"$sum": 1},
                "positivos": {"$sum": {"$cond": [{"$eq": ["$resultado", "POSITIVO"]}, 1, 0]}}
            }
        },
        {"$sort": {"total": -1}},
        {"$limit": 25},
    ]

    results = []
    for r in db.cases.aggregate(pipeline):
        dept = r["_id"] or "Sin especificar"
        coords = DEPARTAMENTO_COORDS.get(dept.upper(), {"lat": None, "lon": None})
        results.append({
            "departamento": dept,
            "total": r["total"],
            "positivos": r["positivos"],
            "lat": coords["lat"],
            "lon": coords["lon"]
        })
    return results


def get_filtered_heatmap(sexo: str = None, departamento: str = None):
    """Obtiene datos de heatmap filtrados por sexo y/o departamento."""
    if db is None:
        return []

    match_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}
    if sexo and sexo != "TODOS":
        match_filter["sexo"] = sexo
    if departamento and departamento != "TODOS":
        match_filter["departamento_muestra"] = departamento

    pipeline = [
        {"$match": match_filter},
        {
            "$group": {
                "_id": {
                    "departamento": "$departamento_muestra",
                    "provincia": "$provincia_muestra",
                    "distrito": "$distrito_muestra"
                },
                "total": {"$sum": 1},
                "positivos": {"$sum": {"$cond": [{"$eq": ["$resultado", "POSITIVO"]}, 1, 0]}},
                "lat": {"$max": "$latitud"},
                "lon": {"$max": "$longitud"}
            }
        },
        {"$sort": {"total": -1}},
        {"$limit": 200}
    ]

    results = []
    for r in db.cases.aggregate(pipeline):
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
                "positivos": r["positivos"],
                "lat": lat,
                "lon": lon
            })

    return results
