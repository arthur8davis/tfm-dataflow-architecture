"""
COVID-19 Dashboard - Flask + Socket.IO
- Emite actualizaciones en tiempo real usando MongoDB Change Streams
- Debounce con intervalo configurable
"""

import os
import time
import threading
import subprocess
import signal
from datetime import datetime
from flask import Flask, jsonify, render_template
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import sys
from pathlib import Path

# Agregar path para imports del proyecto
ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR))
from src.common.data.ubigeo_coords import get_coords_from_ubigeo

# -----------------------------------------------------------------------------
# Configuración Socket.IO (eventlet opcional)
# -----------------------------------------------------------------------------
ASYNC_MODE = "threading"
if os.getenv("USE_EVENTLET", "0") == "1":
    try:
        import warnings
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="eventlet")
        import eventlet
        eventlet.monkey_patch()
        ASYNC_MODE = "eventlet"
    except ImportError:
        ASYNC_MODE = "threading"

app = Flask(__name__)
app.config["SECRET_KEY"] = "covid-dashboard-secret"
CORS(app)
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode=ASYNC_MODE,
    ping_timeout=60,
    ping_interval=25,
    logger=False,
    engineio_logger=False,
)

# -----------------------------------------------------------------------------
# Configuración MongoDB
# -----------------------------------------------------------------------------
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:admin123@localhost:27017/?authSource=admin&directConnection=true",
)
DATABASE = os.getenv("MONGO_DB", "covid-db")

mongo_client = None
db = None
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=1000)
    mongo_client.admin.command("ping")
    db = mongo_client[DATABASE]
    print(f"[Mongo] Conectado a {MONGO_URI}")
except Exception as e:
    print(f"[Mongo] No se pudo conectar: {e}")

# -----------------------------------------------------------------------------
# Cache y debounce
# -----------------------------------------------------------------------------
REFRESH_EVERY = float(os.getenv("REFRESH_EVERY", "3.0"))  # Intervalo de polling
EVENT_INTERVAL = float(os.getenv("CHANGE_EVENT_INTERVAL", "2.0"))  # ignorar ráfagas más cortas
_dirty_cases = False
_dirty_demises = False
_refresh_lock = threading.Lock()
_interval_lock = threading.Lock()
_refresh_interval = REFRESH_EVERY  # valor editable en caliente

# Polling: último conteo conocido para detectar cambios
_last_cases_count = 0
_last_demises_count = 0

_cache_summary = None
_cache_cases_department = None
_cache_cases_sex = None
_cache_cases_timeline = None
_cache_cases_age = None
_cache_demises_dept = None
_cache_demises_sex = None
_cache_ts = None
_cache_lock = threading.Lock()

# -----------------------------------------------------------------------------
# Sistema de Alertas por Umbral
# -----------------------------------------------------------------------------
_alerts_global_date = None  # Fecha global para todas las alertas
_alerts_config = {
    "cases_total": {"enabled": True, "threshold": 1000000, "name": "Total de Casos"},
    "cases_positive": {"enabled": True, "threshold": 1000, "name": "Casos Positivos"},
    "demises_total": {"enabled": True, "threshold": 1000, "name": "Total Fallecidos"},
    "cases_by_dept": {"enabled": True, "threshold": 2000, "name": "Casos Confirmados por Departamento"},
    "demises_by_dept": {"enabled": True, "threshold": 1000, "name": "Fallecidos por Departamento"},
    "hospitalizations_total": {"enabled": True, "threshold": 500, "name": "Total Hospitalizaciones"},
}
_alerts_lock = threading.Lock()
_active_alerts = []  # Lista de alertas activas
_alerts_history = []  # Historial de alertas (últimas 50)
MAX_ALERTS_HISTORY = 50

# -----------------------------------------------------------------------------
# Funciones de Alertas
# -----------------------------------------------------------------------------

def check_threshold_alert(metric_key: str, current_value: int, context: str = None):
    """Verifica si un valor supera el umbral configurado y emite alerta si corresponde."""
    with _alerts_lock:
        config = _alerts_config.get(metric_key)
        if not config or not config.get("enabled"):
            return None

        threshold = config.get("threshold", 0)
        if current_value >= threshold:
            # Para alertas por departamento, usar contexto como parte del identificador
            alert_key = f"{metric_key}_{context}" if context else metric_key

            # Formatear fecha global si existe
            date_display = None
            if _alerts_global_date:
                date_display = f"{_alerts_global_date[:4]}-{_alerts_global_date[4:6]}-{_alerts_global_date[6:]}"

            alert = {
                "id": f"{alert_key}_{int(datetime.now().timestamp())}",
                "metric": metric_key,
                "name": config.get("name", metric_key),
                "threshold": threshold,
                "current_value": current_value,
                "context": context,
                "date": date_display,
                "timestamp": datetime.now().isoformat(),
                "level": "warning" if current_value < threshold * 1.5 else "critical"
            }

            # Evitar alertas duplicadas (misma métrica+contexto)
            existing = [a for a in _active_alerts
                       if a["metric"] == metric_key and a.get("context") == context]
            if not existing:
                _active_alerts.append(alert)
                _alerts_history.insert(0, alert)
                if len(_alerts_history) > MAX_ALERTS_HISTORY:
                    _alerts_history.pop()
                return alert
            else:
                # Actualizar valor de alerta existente
                for a in _active_alerts:
                    if a["metric"] == metric_key and a.get("context") == context:
                        a["current_value"] = current_value
                        a["timestamp"] = datetime.now().isoformat()
                        a["date"] = date_display
                        a["level"] = "warning" if current_value < threshold * 1.5 else "critical"
    return None


def emit_alert(alert: dict):
    """Emite una alerta a todos los clientes conectados."""
    if alert:
        socketio.emit('threshold_alert', alert)
        print(f"[Alerta] {alert['name']}: {alert['current_value']:,} >= {alert['threshold']:,} ({alert['level']})")


def clear_alert(metric_key: str):
    """Elimina una alerta activa cuando el valor baja del umbral."""
    with _alerts_lock:
        global _active_alerts
        _active_alerts = [a for a in _active_alerts if a["metric"] != metric_key]


def get_count_by_date(collection_name: str, date_field: str, date: str, extra_filter: dict = None, date_as_int: bool = True):
    """Obtiene conteo de documentos filtrados por fecha específica."""
    if db is None:
        return 0

    match_filter = {}
    if extra_filter:
        match_filter.update(extra_filter)

    if date:
        # Convertir a int si es necesario (casos y fallecidos usan int)
        match_filter[date_field] = int(date) if date_as_int else date

    return db[collection_name].count_documents(match_filter)


def get_dept_count_by_date(collection_name: str, dept_field: str, date_field: str, date: str, count_field: str = None, date_as_int: bool = True):
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

    # Para casos, también contar positivos
    if count_field:
        pipeline[1]["$group"]["positivos"] = {"$sum": {"$cond": [{"$eq": ["$resultado", "POSITIVO"]}, 1, 0]}}

    return [{"departamento": r["_id"], "total": r["total"], "positivos": r.get("positivos", r["total"])}
            for r in db[collection_name].aggregate(pipeline) if r["_id"]]


def check_all_thresholds(summary: dict, dept_data: list = None, demises_dept: list = None):
    """Verifica todos los umbrales y emite alertas correspondientes."""
    alerts_to_emit = []
    global_date = _alerts_global_date

    # Total casos
    if global_date:
        count = get_count_by_date("cases", "fecha_muestra", global_date,
                                   {"sexo": {"$nin": [None, "", "Sin especificar"]}})
        alert = check_threshold_alert("cases_total", count)
    else:
        alert = check_threshold_alert("cases_total", summary.get("cases_total", 0))
    if alert:
        alerts_to_emit.append(alert)

    # Casos positivos
    if global_date:
        count = get_count_by_date("cases", "fecha_muestra", global_date,
                                   {"sexo": {"$nin": [None, "", "Sin especificar"]}, "resultado": "POSITIVO"})
        alert = check_threshold_alert("cases_positive", count)
    else:
        alert = check_threshold_alert("cases_positive", summary.get("cases_positive", 0))
    if alert:
        alerts_to_emit.append(alert)

    # Fallecidos
    if global_date:
        count = get_count_by_date("demises", "fecha_fallecimiento", global_date,
                                   {"sexo": {"$nin": [None, "", "Sin especificar"]}})
        alert = check_threshold_alert("demises_total", count)
    else:
        alert = check_threshold_alert("demises_total", summary.get("demises_total", 0))
    if alert:
        alerts_to_emit.append(alert)

    # Hospitalizaciones (formato fecha: dd/mm/yyyy)
    if global_date:
        # Convertir YYYYMMDD a dd/mm/yyyy
        hosp_date_fmt = f"{global_date[6:8]}/{global_date[4:6]}/{global_date[0:4]}"
        count = get_count_by_date("hospitalizations", "fecha_ingreso_hosp", hosp_date_fmt,
                                   {"sexo": {"$nin": [None, ""]}}, date_as_int=False)
        alert = check_threshold_alert("hospitalizations_total", count)
    else:
        alert = check_threshold_alert("hospitalizations_total", summary.get("hospitalizations_total", 0))
    if alert:
        alerts_to_emit.append(alert)

    # Casos por departamento
    if global_date:
        filtered_dept = get_dept_count_by_date("cases", "departamento_paciente", "fecha_muestra",
                                                global_date, count_field="positivos")
        for dept in filtered_dept[:5]:
            alert = check_threshold_alert("cases_by_dept", dept.get("positivos", 0), context=dept.get("departamento"))
            if alert:
                alerts_to_emit.append(alert)
    elif dept_data:
        for dept in dept_data[:5]:
            alert = check_threshold_alert("cases_by_dept", dept.get("positivos", 0), context=dept.get("departamento"))
            if alert:
                alerts_to_emit.append(alert)

    # Fallecidos por departamento
    if global_date:
        filtered_dept = get_dept_count_by_date("demises", "departamento", "fecha_fallecimiento", global_date)
        for dept in filtered_dept[:5]:
            alert = check_threshold_alert("demises_by_dept", dept.get("total", 0), context=dept.get("departamento"))
            if alert:
                alerts_to_emit.append(alert)
    elif demises_dept:
        for dept in demises_dept[:5]:
            alert = check_threshold_alert("demises_by_dept", dept.get("total", 0), context=dept.get("departamento"))
            if alert:
                alerts_to_emit.append(alert)

    # Emitir todas las alertas
    for alert in alerts_to_emit:
        emit_alert(alert)

    return alerts_to_emit


# -----------------------------------------------------------------------------
# Consultas
# -----------------------------------------------------------------------------

def get_summary_data():
    if db is None:
        return {"cases_total": 0, "demises_total": 0, "cases_positive": 0, "hospitalizations_total": 0, "last_updated": datetime.now().isoformat()}
    # Excluir registros sin sexo definido para que los totales coincidan con los filtros
    sex_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}
    # Hospitalizaciones usa formato M/F para sexo
    hosp_filter = {"sexo": {"$nin": [None, ""]}}
    return {
        "cases_total": db.cases.count_documents(sex_filter),
        "demises_total": db.demises.count_documents(sex_filter),
        "hospitalizations_total": db.hospitalizations.count_documents(hosp_filter),
        "cases_positive": db.cases.count_documents({"resultado": "POSITIVO", **sex_filter}),
        "last_updated": datetime.now().isoformat(),
    }


def get_hospitalizations_by_department():
    """Obtiene hospitalizaciones agrupadas por departamento"""
    if db is None:
        return []
    pipeline = [
        {"$match": {"sexo": {"$nin": [None, ""]}}},
        {"$group": {
            "_id": "$dep_domicilio",
            "total": {"$sum": 1}
        }},
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
    """Obtiene datos de hospitalizaciones para el mapa de calor"""
    if db is None:
        return []
    pipeline = [
        {"$match": {"sexo": {"$nin": [None, ""]}}},
        {"$group": {
            "_id": {
                "departamento": "$dep_domicilio",
                "provincia": "$prov_domicilio",
                "distrito": "$dist_domicilio"
            },
            "total": {"$sum": 1},
            "lat": {"$max": "$latitud"},
            "lon": {"$max": "$longitud"}
        }},
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


# Coordenadas de departamentos de Perú (centroide)
DEPARTAMENTO_COORDS = {
    "AMAZONAS": {"lat": -5.9146, "lon": -78.1219},
    "ANCASH": {"lat": -9.5293, "lon": -77.5282},
    "APURIMAC": {"lat": -13.6339, "lon": -72.8814},
    "AREQUIPA": {"lat": -16.3988, "lon": -71.5350},
    "AYACUCHO": {"lat": -13.1639, "lon": -74.2236},
    "CAJAMARCA": {"lat": -7.1619, "lon": -78.5128},
    "CALLAO": {"lat": -12.0565, "lon": -77.1181},
    "CUSCO": {"lat": -13.5226, "lon": -71.9674},
    "HUANCAVELICA": {"lat": -12.7864, "lon": -74.9764},
    "HUANUCO": {"lat": -9.9306, "lon": -76.2422},
    "ICA": {"lat": -14.0678, "lon": -75.7286},
    "JUNIN": {"lat": -11.7580, "lon": -75.1412},
    "LA LIBERTAD": {"lat": -8.1150, "lon": -79.0300},
    "LAMBAYEQUE": {"lat": -6.7014, "lon": -79.9060},
    "LIMA": {"lat": -12.0464, "lon": -77.0428},
    "LORETO": {"lat": -3.7491, "lon": -73.2538},
    "MADRE DE DIOS": {"lat": -12.5933, "lon": -69.1891},
    "MOQUEGUA": {"lat": -17.1960, "lon": -70.1356},
    "PASCO": {"lat": -10.6801, "lon": -75.9958},
    "PIURA": {"lat": -5.1945, "lon": -80.6328},
    "PUNO": {"lat": -15.8402, "lon": -70.0219},
    "SAN MARTIN": {"lat": -6.4855, "lon": -76.3606},
    "TACNA": {"lat": -18.0146, "lon": -70.2536},
    "TUMBES": {"lat": -3.5667, "lon": -80.4515},
    "UCAYALI": {"lat": -8.1116, "lon": -74.0089},
}


def get_cases_by_department():
    if db is None:
        return []
    pipeline = [
        # Excluir registros sin sexo definido para que los totales coincidan
        {"$match": {"sexo": {"$nin": [None, "", "Sin especificar"]}}},
        {"$group": {"_id": "$departamento_paciente", "total": {"$sum": 1}, "positivos": {"$sum": {"$cond": [{"$eq": ["$resultado", "POSITIVO"]}, 1, 0]}}}},
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
    """Obtiene distribución por sexo - solo casos positivos, filtrado por departamento opcional"""
    if db is None:
        return []

    # Filtro base: excluir sin sexo y solo positivos
    match_filter = {
        "sexo": {"$nin": [None, "", "Sin especificar"]},
        "resultado": "POSITIVO"
    }

    # Filtrar por departamento si está especificado
    if departamento and departamento != "TODOS":
        match_filter["departamento_paciente"] = departamento

    pipeline = [
        {"$match": match_filter},
        {"$group": {"_id": "$sexo", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}}
    ]
    return [{"sexo": r["_id"], "total": r["total"]} for r in db.cases.aggregate(pipeline)]


def get_demises_by_sex(departamento: str = None):
    """Obtiene distribución de fallecidos por sexo, filtrado por departamento opcional"""
    if db is None:
        return []

    # Filtro base: excluir sin sexo
    match_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}

    # Filtrar por departamento si está especificado
    if departamento and departamento != "TODOS":
        match_filter["departamento"] = departamento

    pipeline = [
        {"$match": match_filter},
        {"$group": {"_id": "$sexo", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}}
    ]
    return [{"sexo": r["_id"], "total": r["total"]} for r in db.demises.aggregate(pipeline)]


def get_cases_by_date(departamento: str = None, sexo: str = None):
    """Obtiene casos confirmados por fecha, filtrado por departamento y/o sexo"""
    if db is None:
        return []

    # Filtro base: excluir sin sexo y solo positivos
    match_filter = {
        "sexo": {"$nin": [None, "", "Sin especificar"]},
        "resultado": "POSITIVO"
    }

    # Filtrar por departamento si está especificado
    if departamento and departamento != "TODOS":
        match_filter["departamento_paciente"] = departamento

    # Filtrar por sexo si está especificado
    if sexo and sexo != "TODOS":
        match_filter["sexo"] = sexo

    pipeline = [
        {"$match": match_filter},
        {"$group": {"_id": "$fecha_muestra", "total": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
        {"$limit": 100},
    ]
    return [{"fecha": r["_id"], "total": r["total"], "positivos": r["total"]} for r in db.cases.aggregate(pipeline) if r["_id"]]


def get_demises_by_date(departamento: str = None, sexo: str = None):
    """Obtiene fallecidos por fecha, filtrado por departamento y/o sexo"""
    if db is None:
        return []

    # Filtro base: excluir sin sexo definido
    match_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}

    # Filtrar por departamento si está especificado
    if departamento and departamento != "TODOS":
        match_filter["departamento"] = departamento

    # Filtrar por sexo si está especificado
    if sexo and sexo != "TODOS":
        match_filter["sexo"] = sexo

    pipeline = [
        {"$match": match_filter},
        {"$group": {"_id": "$fecha_fallecimiento", "total": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
        {"$limit": 100},
    ]
    return [{"fecha": r["_id"], "total": r["total"]} for r in db.demises.aggregate(pipeline) if r["_id"]]


def get_cases_by_age():
    if db is None:
        return []
    pipeline = [
        # Excluir registros sin sexo definido
        {"$match": {"sexo": {"$nin": [None, "", "Sin especificar"]}}},
        {"$bucket": {
            "groupBy": "$edad",
            "boundaries": [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
            "default": "100+",
            "output": {
                "total": {"$sum": 1},
                "positivos": {"$sum": {"$cond": [{"$eq": ["$resultado", "POSITIVO"]}, 1, 0]}}
            }
        }}
    ]
    def label(r):
        return f"{r['_id']}-{r['_id']+9}" if isinstance(r["_id"], int) else r["_id"]
    return [{"grupo": label(r), "total": r["total"], "positivos": r["positivos"]} for r in db.cases.aggregate(pipeline)]


def get_demises_by_department():
    if db is None:
        return []
    pipeline = [
        # Excluir registros sin sexo definido
        {"$match": {"sexo": {"$nin": [None, "", "Sin especificar"]}}},
        {"$group": {
            "_id": "$departamento",
            "total": {"$sum": 1},
            "lat": {"$max": "$latitud"},
            "lon": {"$max": "$longitud"}
        }},
        {"$sort": {"total": -1}},
        {"$limit": 25},
    ]
    results = []
    for r in db.demises.aggregate(pipeline):
        dept = r["_id"] or "Sin especificar"
        lat = r.get("lat")
        lon = r.get("lon")
        # Fallback a coordenadas del diccionario
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


def get_demises_heatmap_data():
    """Obtiene datos de fallecidos para el mapa de calor con coordenadas precisas"""
    if db is None:
        return []

    pipeline = [
        # Excluir registros sin sexo definido
        {"$match": {"sexo": {"$nin": [None, "", "Sin especificar"]}}},
        {"$group": {
            "_id": {
                "departamento": "$departamento",
                "provincia": "$provincia",
                "distrito": "$distrito"
            },
            "total": {"$sum": 1},
            "lat": {"$max": "$latitud"},
            "lon": {"$max": "$longitud"}
        }},
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


# -----------------------------------------------------------------------------
# Consultas con filtros combinados (departamento + sexo)
# -----------------------------------------------------------------------------

def get_filtered_summary(departamento: str = None, sexo: str = None):
    """Obtiene resumen filtrado por departamento y/o sexo"""
    if db is None:
        return {"cases_total": 0, "demises_total": 0, "cases_positive": 0, "hospitalizations_total": 0, "last_updated": datetime.now().isoformat()}

    # Filtro base: excluir sin sexo definido
    base_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}

    # Agregar filtro de sexo si está especificado
    if sexo and sexo != "TODOS":
        base_filter["sexo"] = sexo

    # Filtro para casos (usa departamento_paciente)
    cases_filter = dict(base_filter)
    if departamento and departamento != "TODOS":
        cases_filter["departamento_paciente"] = departamento

    # Filtro para fallecidos (usa departamento)
    demises_filter = dict(base_filter)
    if departamento and departamento != "TODOS":
        demises_filter["departamento"] = departamento

    # Filtro para hospitalizaciones (usa dep_domicilio y sexo M/F)
    hosp_base = {"sexo": {"$nin": [None, ""]}}
    if sexo and sexo != "TODOS":
        # Convertir MASCULINO/FEMENINO a M/F
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


def get_filtered_cases_by_department(sexo: str = None):
    """Obtiene casos por departamento, opcionalmente filtrado por sexo"""
    if db is None:
        return []
    
    

    match_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}
    if sexo and sexo != "TODOS":
        match_filter["sexo"] = sexo

    pipeline = [
        {"$match": match_filter},
        {"$group": {"_id": "$departamento_paciente", "total": {"$sum": 1}, "positivos": {"$sum": {"$cond": [{"$eq": ["$resultado", "POSITIVO"]}, 1, 0]}}}},
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


def get_filtered_demises_by_department(sexo: str = None):
    """Obtiene fallecidos por departamento, opcionalmente filtrado por sexo"""
    if db is None:
        return []

    match_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}
    if sexo and sexo != "TODOS":
        match_filter["sexo"] = sexo

    pipeline = [
        {"$match": match_filter},
        {"$group": {
            "_id": "$departamento",
            "total": {"$sum": 1},
            "lat": {"$max": "$latitud"},
            "lon": {"$max": "$longitud"}
        }},
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


def get_filtered_heatmap(sexo: str = None, departamento: str = None):
    """Obtiene datos de heatmap filtrados por sexo y/o departamento"""
    if db is None:
        return []

    match_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}
    if sexo and sexo != "TODOS":
        match_filter["sexo"] = sexo
    if departamento and departamento != "TODOS":
        match_filter["departamento_muestra"] = departamento

    pipeline = [
        {"$match": match_filter},
        {"$group": {
            "_id": {
                "departamento": "$departamento_muestra",
                "provincia": "$provincia_muestra",
                "distrito": "$distrito_muestra"
            },
            "total": {"$sum": 1},
            "positivos": {"$sum": {"$cond": [{"$eq": ["$resultado", "POSITIVO"]}, 1, 0]}},
            "lat": {"$max": "$latitud"},
            "lon": {"$max": "$longitud"}
        }},
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


def get_filtered_demises_heatmap(sexo: str = None, departamento: str = None):
    """Obtiene datos de heatmap de fallecidos filtrados por sexo y/o departamento"""
    if db is None:
        return []

    match_filter = {"sexo": {"$nin": [None, "", "Sin especificar"]}}
    if sexo and sexo != "TODOS":
        match_filter["sexo"] = sexo
    if departamento and departamento != "TODOS":
        match_filter["departamento"] = departamento

    pipeline = [
        {"$match": match_filter},
        {"$group": {
            "_id": {
                "departamento": "$departamento",
                "provincia": "$provincia",
                "distrito": "$distrito"
            },
            "total": {"$sum": 1},
            "lat": {"$max": "$latitud"},
            "lon": {"$max": "$longitud"}
        }},
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


def get_filtered_hospitalizations_by_department(sexo: str = None):
    """Obtiene hospitalizaciones por departamento, filtrado por sexo"""
    if db is None:
        return []

    match_filter = {"sexo": {"$nin": [None, ""]}}
    if sexo and sexo != "TODOS":
        # Convertir MASCULINO/FEMENINO a M/F
        match_filter["sexo"] = "M" if sexo == "MASCULINO" else "F"

    pipeline = [
        {"$match": match_filter},
        {"$group": {
            "_id": "$dep_domicilio",
            "total": {"$sum": 1}
        }},
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
    """Obtiene datos de heatmap de hospitalizaciones filtrados"""
    if db is None:
        return []

    match_filter = {"sexo": {"$nin": [None, ""]}}
    if sexo and sexo != "TODOS":
        match_filter["sexo"] = "M" if sexo == "MASCULINO" else "F"
    if departamento and departamento != "TODOS":
        match_filter["dep_domicilio"] = departamento

    pipeline = [
        {"$match": match_filter},
        {"$group": {
            "_id": {
                "departamento": "$dep_domicilio",
                "provincia": "$prov_domicilio",
                "distrito": "$dist_domicilio"
            },
            "total": {"$sum": 1},
            "lat": {"$max": "$latitud"},
            "lon": {"$max": "$longitud"}
        }},
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


def get_heatmap_data():
    """Obtiene datos para el mapa de calor agrupados por ubicación de muestra con coordenadas precisas"""
    if db is None:
        return []

    # Agrupar por departamento/provincia/distrito de muestra para mayor precisión
    pipeline = [
        # Excluir registros sin sexo definido
        {"$match": {"sexo": {"$nin": [None, "", "Sin especificar"]}}},
        {"$group": {
            "_id": {
                "departamento": "$departamento_muestra",
                "provincia": "$provincia_muestra",
                "distrito": "$distrito_muestra"
            },
            "total": {"$sum": 1},
            "positivos": {"$sum": {"$cond": [{"$eq": ["$resultado", "POSITIVO"]}, 1, 0]}},
            # Usar $max para obtener valores no nulos (toma el valor más alto, ignorando nulls)
            "lat": {"$max": "$latitud"},
            "lon": {"$max": "$longitud"}
        }},
        {"$sort": {"total": -1}},
        {"$limit": 200}
    ]

    results = []
    for r in db.cases.aggregate(pipeline):
        departamento = r["_id"].get("departamento") or "Sin especificar"
        provincia = r["_id"].get("provincia") or ""
        distrito = r["_id"].get("distrito") or ""

        # Usar coordenadas guardadas en MongoDB
        lat = r.get("lat")
        lon = r.get("lon")

        # Si no hay coordenadas guardadas, buscar por distrito/provincia/departamento
        if lat is None or lon is None:
            coords = get_coords_from_ubigeo(departamento, provincia, distrito)
            if coords:
                lat = coords["lat"]
                lon = coords["lon"]

        # Debug: mostrar qué se está obteniendo
        if distrito:
            print(f"[Heatmap] {departamento}|{provincia}|{distrito}: lat={lat}, lon={lon}, total={r['total']}")

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

# -----------------------------------------------------------------------------
# Refresh / cache
# -----------------------------------------------------------------------------

def refresh_cases():
    try:
        summary = get_summary_data()
        dept = get_cases_by_department()
        sex = get_cases_by_sex()
        timeline = get_cases_by_date()
        age = get_cases_by_age()
        with _cache_lock:
            global _cache_summary, _cache_cases_department, _cache_cases_sex, _cache_cases_timeline, _cache_cases_age, _cache_ts
            _cache_summary = summary
            _cache_cases_department = dept
            _cache_cases_sex = sex
            _cache_cases_timeline = timeline
            _cache_cases_age = age
            _cache_ts = datetime.now().isoformat()
        socketio.emit('update_summary', summary)
        socketio.emit('update_department', dept)
        socketio.emit('update_sex', sex)
        socketio.emit('update_timeline', timeline)
        socketio.emit('update_age', age)
        socketio.emit('update_heatmap', get_heatmap_data())
        print(f"[Refresh Cases] Emitidos datos - Total: {summary.get('cases_total', 0)}")
        # Verificar umbrales de alertas (solo globales para evitar sobrecarga)
        try:
            check_all_thresholds(summary, dept_data=dept[:5] if dept else None)  # Limitar a top 5
            with _alerts_lock:
                socketio.emit('active_alerts', list(_active_alerts))
        except Exception as e:
            print(f"[Refresh Cases] Error en alertas: {e}")
    except Exception as e:
        print(f"[Refresh Cases] Error: {e}")


def refresh_demises():
    try:
        summary = get_summary_data()
        dem_dept = get_demises_by_department()
        dem_sex = get_demises_by_sex()
        with _cache_lock:
            global _cache_summary, _cache_demises_dept, _cache_demises_sex, _cache_ts
            _cache_summary = summary
            _cache_demises_dept = dem_dept
            _cache_demises_sex = dem_sex
            _cache_ts = datetime.now().isoformat()
        socketio.emit('update_summary', summary)
        socketio.emit('update_demises_dept', dem_dept)
        socketio.emit('update_demises_sex', dem_sex)
        socketio.emit('update_demises_heatmap', get_demises_heatmap_data())
        print(f"[Refresh Demises] Emitidos datos - Total: {summary.get('demises_total', 0)}")
        # Verificar umbrales de alertas
        try:
            check_all_thresholds(summary, demises_dept=dem_dept[:5] if dem_dept else None)
            with _alerts_lock:
                socketio.emit('active_alerts', list(_active_alerts))
        except Exception as e:
            print(f"[Refresh Demises] Error en alertas: {e}")
    except Exception as e:
        print(f"[Refresh Demises] Error: {e}")


def mark_dirty(collection_name: str):
    global _dirty_cases, _dirty_demises
    with _refresh_lock:
        if collection_name == "cases":
            _dirty_cases = True
            print(f"[mark_dirty] cases marcado como dirty")
        elif collection_name == "demises":
            _dirty_demises = True
            print(f"[mark_dirty] demises marcado como dirty")


def refresh_loop():
    """
    Loop de polling para detectar cambios en colecciones time-series.
    Compara conteos para detectar nuevos documentos.
    """
    global _last_cases_count, _last_demises_count
    print(f"[Polling] Iniciado con intervalo {_refresh_interval}s")

    # Inicializar conteos
    if db is not None:
        try:
            _last_cases_count = db.cases.estimated_document_count()
            _last_demises_count = db.demises.estimated_document_count()
            print(f"[Polling] Conteo inicial - Cases: {_last_cases_count}, Demises: {_last_demises_count}")
        except Exception as e:
            print(f"[Polling] Error obteniendo conteo inicial: {e}")

    while True:
        with _interval_lock:
            interval = _refresh_interval
        time.sleep(interval)

        if db is None:
            continue

        try:
            # Verificar cambios por polling (comparar conteos)
            current_cases = db.cases.estimated_document_count()
            current_demises = db.demises.estimated_document_count()

            cases_changed = current_cases != _last_cases_count
            demises_changed = current_demises != _last_demises_count

            if cases_changed:
                diff = current_cases - _last_cases_count
                print(f"[Polling] Detectados {diff:+d} casos nuevos (total: {current_cases})")
                _last_cases_count = current_cases
                socketio.emit('data_changed', {
                    'collection': 'cases',
                    'operation': 'insert',
                    'timestamp': datetime.now().isoformat(),
                    'count': current_cases
                })
                refresh_cases()

            if demises_changed:
                diff = current_demises - _last_demises_count
                print(f"[Polling] Detectados {diff:+d} fallecidos nuevos (total: {current_demises})")
                _last_demises_count = current_demises
                socketio.emit('data_changed', {
                    'collection': 'demises',
                    'operation': 'insert',
                    'timestamp': datetime.now().isoformat(),
                    'count': current_demises
                })
                refresh_demises()

        except Exception as e:
            print(f"[Polling] Error: {e}")

# -----------------------------------------------------------------------------
# Change Streams
# -----------------------------------------------------------------------------

def watch_collection(collection_name):
    if db is None:
        print(f"[ChangeStream] Mongo no disponible; {collection_name} en espera 5s")
        time.sleep(5)
        socketio.start_background_task(watch_collection, collection_name)
        return
    collection = db[collection_name]
    print(f"[ChangeStream] Escuchando cambios en '{collection_name}'...")
    last_seen = 0.0
    try:
        with collection.watch([
            {'$match': {'operationType': {'$in': ['insert', 'update', 'delete']}}}
        ]) as stream:
            for change in stream:
                now = time.time()
                if now - last_seen < EVENT_INTERVAL:
                    continue  # ignora ráfagas; solo uno cada EVENT_INTERVAL
                last_seen = now
                operation = change['operationType']
                socketio.emit('data_changed', {
                    'collection': collection_name,
                    'operation': operation,
                    'timestamp': datetime.fromtimestamp(now).isoformat()
                })
                mark_dirty(collection_name)
    except Exception as e:
        msg = str(e)
        print(f"[ChangeStream] Error en {collection_name}: {msg}")
        # No reintentar si es una colección time series/view
        if 'CommandNotSupportedOnView' in msg or '166' in msg:
            print(f"[ChangeStream] {collection_name} parece timeseries/view; se detiene watch.")
            return
        time.sleep(2)
        socketio.start_background_task(watch_collection, collection_name)


def start_change_streams():
    # Change Streams no funcionan con colecciones time-series
    # Usamos polling en su lugar
    print("[Info] Colecciones time-series detectadas - usando polling en lugar de Change Streams")
    socketio.start_background_task(refresh_loop)
    print(f"[Polling] Loop iniciado cada {_refresh_interval}s")

# -----------------------------------------------------------------------------
# WebSocket Events
# -----------------------------------------------------------------------------

@socketio.on('connect')
def handle_connect(auth=None):
    print('[WebSocket] Cliente conectado')
    if db is None:
        emit('error', {'message': 'MongoDB no disponible'})
        return
    with _cache_lock:
        summary = _cache_summary or get_summary_data()
        dept = _cache_cases_department or get_cases_by_department()
        sex = _cache_cases_sex or get_cases_by_sex()
        timeline = _cache_cases_timeline or get_cases_by_date()
        age = _cache_cases_age or get_cases_by_age()
        dem_dept = _cache_demises_dept or get_demises_by_department()
        dem_sex = _cache_demises_sex or get_demises_by_sex()
    hosp_dept = get_hospitalizations_by_department()
    emit('update_summary', summary)
    emit('update_department', dept)
    emit('update_sex', sex)
    emit('update_timeline', timeline)
    emit('update_age', age)
    emit('update_demises_dept', dem_dept)
    emit('update_demises_sex', dem_sex)
    emit('update_demises_timeline', get_demises_by_date())
    emit('update_heatmap', get_heatmap_data())
    emit('update_demises_heatmap', get_demises_heatmap_data())
    emit('update_hospitalizations_dept', hosp_dept)
    emit('update_hospitalizations_heatmap', get_hospitalizations_heatmap_data())

    # Verificar umbrales con datos actuales
    check_all_thresholds(summary, dept_data=dept, demises_dept=dem_dept)

    # Enviar configuración y alertas activas
    with _alerts_lock:
        emit('alerts_config', {"config": _alerts_config, "global_date": _alerts_global_date})
        emit('active_alerts', _active_alerts)


@socketio.on('disconnect')
def handle_disconnect():
    print('[WebSocket] Cliente desconectado')


@socketio.on('request_refresh')
def handle_refresh():
    summary = get_summary_data()
    dept = get_cases_by_department()
    dem_dept = get_demises_by_department()
    hosp_dept = get_hospitalizations_by_department()

    emit('update_summary', summary)
    emit('update_department', dept)
    emit('update_sex', get_cases_by_sex())
    emit('update_timeline', get_cases_by_date())
    emit('update_age', get_cases_by_age())
    emit('update_demises_dept', dem_dept)
    emit('update_demises_sex', get_demises_by_sex())
    emit('update_heatmap', get_heatmap_data())
    emit('update_demises_heatmap', get_demises_heatmap_data())
    emit('update_hospitalizations_dept', hosp_dept)
    emit('update_hospitalizations_heatmap', get_hospitalizations_heatmap_data())

    # Verificar umbrales y enviar alertas actualizadas
    check_all_thresholds(summary, dept_data=dept, demises_dept=dem_dept)
    with _alerts_lock:
        emit('active_alerts', _active_alerts)


@socketio.on('set_refresh_interval')
def handle_set_refresh_interval(data):
    """
    Permite ajustar el intervalo de refresco en caliente.
    data: {"seconds": <float>}
    """
    global _refresh_interval
    try:
        seconds = float(data.get("seconds"))
        if seconds < 0.1 or seconds > 60:
            raise ValueError("Intervalo fuera de rango (0.1s - 60s)")
        with _interval_lock:
            _refresh_interval = seconds
        emit('refresh_interval', {'seconds': seconds})
        print(f"[Refresh] Intervalo actualizado a {seconds}s vía websocket")
    except Exception as e:
        emit('error', {'message': f'No se pudo cambiar intervalo: {e}'})


@socketio.on('get_alerts_config')
def handle_get_alerts_config():
    """Devuelve la configuración actual de umbrales."""
    with _alerts_lock:
        emit('alerts_config', {"config": _alerts_config, "global_date": _alerts_global_date})


@socketio.on('update_alert_threshold')
def handle_update_alert_threshold(data):
    """
    Actualiza un umbral de alerta.
    data: {"metric": "cases_total", "threshold": 100000, "enabled": true}
    """
    try:
        metric = data.get("metric")
        if metric not in _alerts_config:
            emit('error', {'message': f'Métrica desconocida: {metric}'})
            return

        with _alerts_lock:
            if "threshold" in data:
                _alerts_config[metric]["threshold"] = int(data["threshold"])
            if "enabled" in data:
                _alerts_config[metric]["enabled"] = bool(data["enabled"])

        emit('alerts_config', {"config": _alerts_config, "global_date": _alerts_global_date})
        socketio.emit('alerts_config_updated', {"config": _alerts_config, "global_date": _alerts_global_date})
        print(f"[Alertas] Umbral actualizado: {metric} = {_alerts_config[metric]}")
    except Exception as e:
        emit('error', {'message': f'Error actualizando umbral: {e}'})


@socketio.on('set_alerts_global_date')
def handle_set_alerts_global_date(data):
    """
    Establece la fecha global para todas las alertas.
    data: {"date": "20211029"} o {"date": null}
    """
    global _alerts_global_date
    try:
        with _alerts_lock:
            _alerts_global_date = data.get("date") if data.get("date") else None

        # Limpiar alertas activas al cambiar fecha
        global _active_alerts
        _active_alerts = []

        emit('alerts_config', {"config": _alerts_config, "global_date": _alerts_global_date})
        socketio.emit('alerts_global_date_updated', {"global_date": _alerts_global_date})
        print(f"[Alertas] Fecha global actualizada: {_alerts_global_date}")

        # Re-evaluar alertas con nueva fecha
        summary = get_summary_data()
        dept = get_cases_by_department()
        dem_dept = get_demises_by_department()
        check_all_thresholds(summary, dept_data=dept, demises_dept=dem_dept)
        with _alerts_lock:
            emit('active_alerts', list(_active_alerts))
    except Exception as e:
        emit('error', {'message': f'Error actualizando fecha global: {e}'})


@socketio.on('get_active_alerts')
def handle_get_active_alerts():
    """Devuelve las alertas activas."""
    with _alerts_lock:
        emit('active_alerts', _active_alerts)


@socketio.on('get_alerts_history')
def handle_get_alerts_history():
    """Devuelve el historial de alertas."""
    with _alerts_lock:
        emit('alerts_history', _alerts_history)


@socketio.on('dismiss_alert')
def handle_dismiss_alert(data):
    """Descarta una alerta activa."""
    alert_id = data.get("alert_id")
    with _alerts_lock:
        global _active_alerts
        _active_alerts = [a for a in _active_alerts if a["id"] != alert_id]
    emit('active_alerts', _active_alerts)
    socketio.emit('alert_dismissed', {"alert_id": alert_id})


@socketio.on('clear_all_alerts')
def handle_clear_all_alerts():
    """Limpia todas las alertas activas."""
    global _active_alerts
    with _alerts_lock:
        _active_alerts = []
    emit('active_alerts', [])
    socketio.emit('all_alerts_cleared')
    print("[Alertas] Todas las alertas han sido limpiadas")


@socketio.on('request_filtered_data')
def handle_filtered_data(data):
    """
    Solicita datos filtrados por departamento y/o sexo.
    data: {"departamento": "LIMA", "sexo": "MASCULINO"}
    """
    departamento = data.get("departamento", "TODOS")
    sexo = data.get("sexo", "TODOS")

    print(f"[WebSocket] Solicitando datos filtrados: dept={departamento}, sexo={sexo}")

    # Obtener datos filtrados
    summary = get_filtered_summary(departamento, sexo)
    dept_data = get_filtered_cases_by_department(sexo)
    demises_dept = get_filtered_demises_by_department(sexo)
    hosp_dept = get_filtered_hospitalizations_by_department(sexo)
    heatmap = get_filtered_heatmap(sexo, departamento)
    demises_heatmap = get_filtered_demises_heatmap(sexo, departamento)
    hosp_heatmap = get_filtered_hospitalizations_heatmap(sexo, departamento)

    # Distribución por sexo filtrada por departamento (solo positivos)
    sex_data = get_cases_by_sex(departamento)
    demises_sex_data = get_demises_by_sex(departamento)

    # Timeline filtrado por departamento y sexo
    timeline_data = get_cases_by_date(departamento, sexo)
    demises_timeline_data = get_demises_by_date(departamento, sexo)

    # Emitir datos filtrados
    emit('filtered_summary', summary)
    emit('filtered_department', dept_data)
    emit('filtered_demises_dept', demises_dept)
    emit('filtered_hospitalizations_dept', hosp_dept)
    emit('filtered_heatmap', heatmap)
    emit('filtered_demises_heatmap', demises_heatmap)
    emit('filtered_hospitalizations_heatmap', hosp_heatmap)
    emit('filtered_sex', sex_data)
    emit('filtered_demises_sex', demises_sex_data)
    emit('filtered_timeline', timeline_data)
    emit('filtered_demises_timeline', demises_timeline_data)

# -----------------------------------------------------------------------------
# REST Endpoints
# -----------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/summary")
def api_summary():
    return jsonify(get_summary_data())

@app.route("/api/cases/by-department")
def api_cases_department():
    return jsonify(get_cases_by_department())

@app.route("/api/cases/by-date")
def api_cases_date():
    return jsonify(get_cases_by_date())

@app.route("/api/cases/by-age-group")
def api_cases_age():
    return jsonify(get_cases_by_age())

@app.route("/api/cases/by-sex")
def api_cases_sex():
    return jsonify(get_cases_by_sex())

@app.route("/api/demises/by-department")
def api_demises_department():
    return jsonify(get_demises_by_department())

@app.route("/api/demises/by-sex")
def api_demises_sex():
    return jsonify(get_demises_by_sex())

@app.route("/api/heatmap")
def api_heatmap():
    return jsonify(get_heatmap_data())

@app.route("/api/heatmap/demises")
def api_heatmap_demises():
    return jsonify(get_demises_heatmap_data())


@app.route("/api/heatmap/hospitalizations")
def api_heatmap_hospitalizations():
    return jsonify(get_hospitalizations_heatmap_data())

@app.route("/api/refresh-interval", methods=["GET"])
def api_refresh_interval():
    with _interval_lock:
        interval = _refresh_interval
    return jsonify({"refresh_interval_seconds": interval})


@app.route("/api/alerts/config", methods=["GET"])
def api_alerts_config():
    """Obtiene la configuración de umbrales de alertas."""
    with _alerts_lock:
        return jsonify(_alerts_config)


@app.route("/api/alerts/config", methods=["POST"])
def api_update_alerts_config():
    """Actualiza la configuración de umbrales de alertas."""
    from flask import request
    data = request.get_json()
    metric = data.get("metric")
    if metric not in _alerts_config:
        return jsonify({"error": f"Métrica desconocida: {metric}"}), 400

    with _alerts_lock:
        if "threshold" in data:
            _alerts_config[metric]["threshold"] = int(data["threshold"])
        if "enabled" in data:
            _alerts_config[metric]["enabled"] = bool(data["enabled"])

    socketio.emit('alerts_config_updated', _alerts_config)
    return jsonify(_alerts_config)


@app.route("/api/alerts/active", methods=["GET"])
def api_active_alerts():
    """Obtiene las alertas activas."""
    with _alerts_lock:
        return jsonify(_active_alerts)


@app.route("/api/alerts/history", methods=["GET"])
def api_alerts_history():
    """Obtiene el historial de alertas."""
    with _alerts_lock:
        return jsonify(_alerts_history)

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 50)
    print("COVID-19 Dashboard - Real-time con WebSockets")
    print("=" * 50)

    start_change_streams()

    PORT = int(os.getenv("PORT", "5006"))

    def free_port(port: int):
        """Si el puerto está ocupado, mata procesos que lo usan (solo del usuario actual)."""
        try:
            pids = (
                subprocess.check_output(
                    ["lsof", "-tiTCP:%d" % port, "-sTCP:LISTEN"],
                    stderr=subprocess.DEVNULL,
                )
                .decode()
                .strip()
                .splitlines()
            )
            for pid in pids:
                if pid:
                    try:
                        os.kill(int(pid), signal.SIGTERM)
                    except ProcessLookupError:
                        pass
            # pequeña espera a que liberen
            time.sleep(0.5)
        except subprocess.CalledProcessError:
            # lsof devuelve non-zero si no hay procesos; lo ignoramos
            pass

    free_port(PORT)

    socketio.run(app, host="0.0.0.0", port=PORT, debug=True, use_reloader=False, allow_unsafe_werkzeug=True)
