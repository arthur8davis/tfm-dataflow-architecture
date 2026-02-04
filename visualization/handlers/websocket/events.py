"""
Eventos WebSocket - COVID-19 Dashboard
"""
import sys
from pathlib import Path

# Agregar path para imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flask_socketio import emit

from handlers.queries.summary import get_summary_data, get_filtered_summary
from handlers.queries.cases import (
    get_cases_by_department,
    get_cases_by_sex,
    get_cases_by_date,
    get_cases_by_age,
    get_heatmap_data,
    get_filtered_cases_by_department,
    get_filtered_heatmap,
)
from handlers.queries.demises import (
    get_demises_by_department,
    get_demises_by_sex,
    get_demises_by_date,
    get_demises_heatmap_data,
    get_filtered_demises_by_department,
    get_filtered_demises_heatmap,
)
from handlers.queries.hospitalizations import (
    get_hospitalizations_by_department,
    get_hospitalizations_heatmap_data,
    get_filtered_hospitalizations_by_department,
    get_filtered_hospitalizations_heatmap,
)
from handlers.alerts import (
    get_alerts_config,
    get_alerts_global_date,
    set_alerts_global_date,
    get_active_alerts,
    get_alerts_history_list,
    update_alert_config,
    dismiss_alert,
    clear_all_alerts,
    check_all_thresholds,
    alerts_config,
    alerts_lock,
    active_alerts,
)
from services import db


def register_websocket_handlers(socketio, refresh_interval_lock, get_refresh_interval, set_refresh_interval):
    """Registra los handlers de WebSocket en la instancia de SocketIO."""

    @socketio.on('connect')
    def handle_connect(auth=None):
        print('[WebSocket] Cliente conectado')
        if db is None:
            emit('error', {'message': 'MongoDB no disponible'})
            return

        summary = get_summary_data()
        dept = get_cases_by_department()
        sex = get_cases_by_sex()
        timeline = get_cases_by_date()
        age = get_cases_by_age()
        dem_dept = get_demises_by_department()
        dem_sex = get_demises_by_sex()
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

        check_all_thresholds(summary, dept_data=dept, demises_dept=dem_dept)

        emit('alerts_config', {"config": get_alerts_config(), "global_date": get_alerts_global_date()})
        emit('active_alerts', get_active_alerts())

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

        check_all_thresholds(summary, dept_data=dept, demises_dept=dem_dept)
        emit('active_alerts', get_active_alerts())

    @socketio.on('set_refresh_interval')
    def handle_set_refresh_interval(data):
        """Permite ajustar el intervalo de refresco en caliente."""
        try:
            seconds = float(data.get("seconds"))
            if seconds < 0.1 or seconds > 60:
                raise ValueError("Intervalo fuera de rango (0.1s - 60s)")
            set_refresh_interval(seconds)
            emit('refresh_interval', {'seconds': seconds})
            print(f"[Refresh] Intervalo actualizado a {seconds}s vía websocket")
        except Exception as e:
            emit('error', {'message': f'No se pudo cambiar intervalo: {e}'})

    @socketio.on('get_alerts_config')
    def handle_get_alerts_config():
        """Devuelve la configuración actual de umbrales."""
        emit('alerts_config', {"config": get_alerts_config(), "global_date": get_alerts_global_date()})

    @socketio.on('update_alert_threshold')
    def handle_update_alert_threshold(data):
        """Actualiza un umbral de alerta."""
        try:
            metric = data.get("metric")
            if metric not in alerts_config:
                emit('error', {'message': f'Métrica desconocida: {metric}'})
                return

            update_alert_config(
                metric,
                threshold=int(data["threshold"]) if "threshold" in data else None,
                enabled=bool(data["enabled"]) if "enabled" in data else None
            )

            config_data = {"config": get_alerts_config(), "global_date": get_alerts_global_date()}
            emit('alerts_config', config_data)
            socketio.emit('alerts_config_updated', config_data)
            print(f"[Alertas] Umbral actualizado: {metric}")
        except Exception as e:
            emit('error', {'message': f'Error actualizando umbral: {e}'})

    @socketio.on('set_alerts_global_date')
    def handle_set_alerts_global_date(data):
        """Establece la fecha global para todas las alertas."""
        try:
            date_value = data.get("date") if data.get("date") else None
            set_alerts_global_date(date_value)

            config_data = {"config": get_alerts_config(), "global_date": get_alerts_global_date()}
            emit('alerts_config', config_data)
            socketio.emit('alerts_global_date_updated', {"global_date": get_alerts_global_date()})
            print(f"[Alertas] Fecha global actualizada: {get_alerts_global_date()}")

            summary = get_summary_data()
            dept = get_cases_by_department()
            dem_dept = get_demises_by_department()
            check_all_thresholds(summary, dept_data=dept, demises_dept=dem_dept)
            emit('active_alerts', get_active_alerts())
        except Exception as e:
            emit('error', {'message': f'Error actualizando fecha global: {e}'})

    @socketio.on('get_active_alerts')
    def handle_get_active_alerts():
        """Devuelve las alertas activas."""
        emit('active_alerts', get_active_alerts())

    @socketio.on('get_alerts_history')
    def handle_get_alerts_history():
        """Devuelve el historial de alertas."""
        emit('alerts_history', get_alerts_history_list())

    @socketio.on('dismiss_alert')
    def handle_dismiss_alert(data):
        """Descarta una alerta activa."""
        alert_id = data.get("alert_id")
        dismiss_alert(alert_id)
        emit('active_alerts', get_active_alerts())
        socketio.emit('alert_dismissed', {"alert_id": alert_id})

    @socketio.on('clear_all_alerts')
    def handle_clear_all_alerts():
        """Limpia todas las alertas activas."""
        clear_all_alerts()
        emit('active_alerts', [])
        socketio.emit('all_alerts_cleared')
        print("[Alertas] Todas las alertas han sido limpiadas")

    @socketio.on('request_filtered_data')
    def handle_filtered_data(data):
        """Solicita datos filtrados por departamento y/o sexo."""
        departamento = data.get("departamento", "TODOS")
        sexo = data.get("sexo", "TODOS")

        print(f"[WebSocket] Solicitando datos filtrados: dept={departamento}, sexo={sexo}")

        summary = get_filtered_summary(departamento, sexo)
        dept_data = get_filtered_cases_by_department(sexo)
        demises_dept = get_filtered_demises_by_department(sexo)
        hosp_dept = get_filtered_hospitalizations_by_department(sexo)
        heatmap = get_filtered_heatmap(sexo, departamento)
        demises_heatmap = get_filtered_demises_heatmap(sexo, departamento)
        hosp_heatmap = get_filtered_hospitalizations_heatmap(sexo, departamento)
        sex_data = get_cases_by_sex(departamento)
        demises_sex_data = get_demises_by_sex(departamento)
        timeline_data = get_cases_by_date(departamento, sexo)
        demises_timeline_data = get_demises_by_date(departamento, sexo)
        age_data = get_cases_by_age(departamento)

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
        emit('filtered_age', age_data)
