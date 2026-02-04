"""
Sistema de Alertas por Umbral - COVID-19 Dashboard
"""
import sys
import threading
from pathlib import Path
from datetime import datetime

# Agregar path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from handlers.queries.summary import get_count_by_date, get_dept_count_by_date

# Configuración de alertas
alerts_global_date = None
alerts_config = {
    "cases_total": {"enabled": True, "threshold": 1000000, "name": "Total de Casos"},
    "cases_positive": {"enabled": True, "threshold": 1000, "name": "Casos Positivos"},
    "demises_total": {"enabled": True, "threshold": 1000, "name": "Total Fallecidos"},
    "cases_by_dept": {"enabled": True, "threshold": 2000, "name": "Casos Confirmados por Departamento"},
    "demises_by_dept": {"enabled": True, "threshold": 1000, "name": "Fallecidos por Departamento"},
    "hospitalizations_total": {"enabled": True, "threshold": 500, "name": "Total Hospitalizaciones"},
}
alerts_lock = threading.Lock()
active_alerts = []
alerts_history = []
MAX_ALERTS_HISTORY = 50


def get_alerts_global_date():
    """Obtiene la fecha global de alertas."""
    return alerts_global_date


def set_alerts_global_date(date):
    """Establece la fecha global de alertas."""
    global alerts_global_date, active_alerts
    with alerts_lock:
        alerts_global_date = date
        active_alerts = []


def get_alerts_config():
    """Obtiene la configuración de alertas."""
    with alerts_lock:
        return dict(alerts_config)


def update_alert_config(metric: str, threshold: int = None, enabled: bool = None):
    """Actualiza la configuración de una alerta."""
    with alerts_lock:
        if metric not in alerts_config:
            return False
        if threshold is not None:
            alerts_config[metric]["threshold"] = threshold
        if enabled is not None:
            alerts_config[metric]["enabled"] = enabled
        return True


def get_active_alerts():
    """Obtiene las alertas activas."""
    with alerts_lock:
        return list(active_alerts)


def get_alerts_history_list():
    """Obtiene el historial de alertas."""
    with alerts_lock:
        return list(alerts_history)


def dismiss_alert(alert_id: str):
    """Descarta una alerta activa."""
    global active_alerts
    with alerts_lock:
        active_alerts = [a for a in active_alerts if a["id"] != alert_id]


def clear_all_alerts():
    """Limpia todas las alertas activas."""
    global active_alerts
    with alerts_lock:
        active_alerts = []


def check_threshold_alert(metric_key: str, current_value: int, context: str = None):
    """Verifica si un valor supera el umbral configurado."""
    with alerts_lock:
        config = alerts_config.get(metric_key)
        if not config or not config.get("enabled"):
            return None

        threshold = config.get("threshold", 0)
        if current_value >= threshold:
            alert_key = f"{metric_key}_{context}" if context else metric_key

            date_display = None
            if alerts_global_date:
                date_display = f"{alerts_global_date[:4]}-{alerts_global_date[4:6]}-{alerts_global_date[6:]}"

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

            existing = [a for a in active_alerts
                       if a["metric"] == metric_key and a.get("context") == context]
            if not existing:
                active_alerts.append(alert)
                alerts_history.insert(0, alert)
                if len(alerts_history) > MAX_ALERTS_HISTORY:
                    alerts_history.pop()
                return alert
            else:
                for a in active_alerts:
                    if a["metric"] == metric_key and a.get("context") == context:
                        a["current_value"] = current_value
                        a["timestamp"] = datetime.now().isoformat()
                        a["date"] = date_display
                        a["level"] = "warning" if current_value < threshold * 1.5 else "critical"
    return None


def clear_alert(metric_key: str):
    """Elimina una alerta activa cuando el valor baja del umbral."""
    global active_alerts
    with alerts_lock:
        active_alerts = [a for a in active_alerts if a["metric"] != metric_key]


def check_all_thresholds(summary: dict, dept_data: list = None, demises_dept: list = None,
                         emit_callback=None):
    """Verifica todos los umbrales y emite alertas correspondientes."""
    alerts_to_emit = []
    global_date = alerts_global_date

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

    # Hospitalizaciones
    if global_date:
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
            alert = check_threshold_alert("cases_by_dept", dept.get("positivos", 0),
                                          context=dept.get("departamento"))
            if alert:
                alerts_to_emit.append(alert)
    elif dept_data:
        for dept in dept_data[:5]:
            alert = check_threshold_alert("cases_by_dept", dept.get("positivos", 0),
                                          context=dept.get("departamento"))
            if alert:
                alerts_to_emit.append(alert)

    # Fallecidos por departamento
    if global_date:
        filtered_dept = get_dept_count_by_date("demises", "departamento", "fecha_fallecimiento", global_date)
        for dept in filtered_dept[:5]:
            alert = check_threshold_alert("demises_by_dept", dept.get("total", 0),
                                          context=dept.get("departamento"))
            if alert:
                alerts_to_emit.append(alert)
    elif demises_dept:
        for dept in demises_dept[:5]:
            alert = check_threshold_alert("demises_by_dept", dept.get("total", 0),
                                          context=dept.get("departamento"))
            if alert:
                alerts_to_emit.append(alert)

    # Emitir alertas si hay callback
    if emit_callback:
        for alert in alerts_to_emit:
            emit_callback(alert)

    return alerts_to_emit
