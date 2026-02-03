"""
Consultas de resumen del dashboard COVID-19
"""
import sys
from pathlib import Path
from datetime import datetime

# Agregar path para imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services import db


def get_summary_data():
    """Obtiene datos de resumen general."""
    if db is None:
        return {
            "cases_total": 0,
            "demises_total": 0,
            "cases_positive": 0,
            "hospitalizations_total": 0,
            "last_updated": datetime.now().isoformat()
        }

    sex_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}
    hosp_filter = {"sexo": {"$nin": [None, ""]}}

    return {
        "cases_total": db.cases.count_documents(sex_filter),
        "demises_total": db.demises.count_documents(sex_filter),
        "hospitalizations_total": db.hospitalizations.count_documents(hosp_filter),
        "cases_positive": db.cases.count_documents({"resultado": "POSITIVO", **sex_filter}),
        "last_updated": datetime.now().isoformat(),
    }


def get_filtered_summary(departamento: str = None, sexo: str = None):
    """Obtiene resumen filtrado por departamento y/o sexo."""
    if db is None:
        return {
            "cases_total": 0,
            "demises_total": 0,
            "cases_positive": 0,
            "hospitalizations_total": 0,
            "last_updated": datetime.now().isoformat()
        }

    base_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}

    if sexo and sexo != "TODOS":
        base_filter["sexo"] = sexo

    cases_filter = dict(base_filter)
    if departamento and departamento != "TODOS":
        cases_filter["departamento_paciente"] = departamento

    demises_filter = dict(base_filter)
    if departamento and departamento != "TODOS":
        demises_filter["departamento"] = departamento

    hosp_base = {"sexo": {"$nin": [None, ""]}}
    if sexo and sexo != "TODOS":
        hosp_base["sexo"] = "M" if sexo == "MASCULINO" else "F"
    hosp_filter = dict(hosp_base)
    if departamento and departamento != "TODOS":
        hosp_filter["dep_domicilio"] = departamento

    return {
        "cases_total": db.cases.count_documents(cases_filter),
        "demises_total": db.demises.count_documents(demises_filter),
        "hospitalizations_total": db.hospitalizations.count_documents(hosp_filter),
        "cases_positive": db.cases.count_documents({**cases_filter, "resultado": "POSITIVO"}),
        "last_updated": datetime.now().isoformat(),
    }


def get_count_by_date(collection_name: str, date_field: str, date: str,
                      extra_filter: dict = None, date_as_int: bool = True):
    """Obtiene conteo de documentos filtrados por fecha específica."""
    if db is None:
        return 0

    match_filter = {}
    if extra_filter:
        match_filter.update(extra_filter)

    if date:
        match_filter[date_field] = int(date) if date_as_int else date

    return db[collection_name].count_documents(match_filter)


def get_dept_count_by_date(collection_name: str, dept_field: str, date_field: str,
                           date: str, count_field: str = None, date_as_int: bool = True):
    """Obtiene conteo por departamento filtrado por fecha específica."""
    if db is None:
        return []

    match_filter = {}
    if date:
        match_filter[date_field] = int(date) if date_as_int else date

    pipeline = [
        {"$match": match_filter} if match_filter else {"$match": {}},
        {"$group": {"_id": f"${dept_field}", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}},
        {"$limit": 25}
    ]

    if count_field:
        pipeline[1]["$group"]["positivos"] = {
            "$sum": {"$cond": [{"$eq": ["$resultado", "POSITIVO"]}, 1, 0]}
        }

    return [
        {
            "departamento": r["_id"],
            "total": r["total"],
            "positivos": r.get("positivos", r["total"])
        }
        for r in db[collection_name].aggregate(pipeline) if r["_id"]
    ]
