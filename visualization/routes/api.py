"""
Rutas REST API - COVID-19 Dashboard
"""
import sys
from pathlib import Path

# Agregar path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from flask import Blueprint, jsonify, render_template, request

from handlers.queries.summary import get_summary_data
from handlers.queries.cases import (
    get_cases_by_department,
    get_cases_by_date,
    get_cases_by_age,
    get_cases_by_sex,
    get_heatmap_data,
)
from handlers.queries.demises import (
    get_demises_by_department,
    get_demises_by_sex,
    get_demises_heatmap_data,
)
from handlers.queries.hospitalizations import get_hospitalizations_heatmap_data
from handlers.alerts import (
    get_alerts_config,
    get_active_alerts,
    get_alerts_history_list,
    update_alert_config,
    alerts_config,
)

api_bp = Blueprint('api', __name__)


@api_bp.route("/")
def index():
    return render_template("index.html")


@api_bp.route("/api/summary")
def api_summary():
    return jsonify(get_summary_data())


@api_bp.route("/api/cases/by-department")
def api_cases_department():
    return jsonify(get_cases_by_department())


@api_bp.route("/api/cases/by-date")
def api_cases_date():
    return jsonify(get_cases_by_date())


@api_bp.route("/api/cases/by-age-group")
def api_cases_age():
    return jsonify(get_cases_by_age())


@api_bp.route("/api/cases/by-sex")
def api_cases_sex():
    return jsonify(get_cases_by_sex())


@api_bp.route("/api/demises/by-department")
def api_demises_department():
    return jsonify(get_demises_by_department())


@api_bp.route("/api/demises/by-sex")
def api_demises_sex():
    return jsonify(get_demises_by_sex())


@api_bp.route("/api/heatmap")
def api_heatmap():
    return jsonify(get_heatmap_data())


@api_bp.route("/api/heatmap/demises")
def api_heatmap_demises():
    return jsonify(get_demises_heatmap_data())


@api_bp.route("/api/heatmap/hospitalizations")
def api_heatmap_hospitalizations():
    return jsonify(get_hospitalizations_heatmap_data())


@api_bp.route("/api/alerts/config", methods=["GET"])
def api_alerts_config_get():
    """Obtiene la configuración de umbrales de alertas."""
    return jsonify(get_alerts_config())


@api_bp.route("/api/alerts/config", methods=["POST"])
def api_alerts_config_update():
    """Actualiza la configuración de umbrales de alertas."""
    data = request.get_json()
    metric = data.get("metric")
    if metric not in alerts_config:
        return jsonify({"error": f"Métrica desconocida: {metric}"}), 400

    update_alert_config(
        metric,
        threshold=int(data["threshold"]) if "threshold" in data else None,
        enabled=bool(data["enabled"]) if "enabled" in data else None
    )
    return jsonify(get_alerts_config())


@api_bp.route("/api/alerts/active", methods=["GET"])
def api_active_alerts():
    """Obtiene las alertas activas."""
    return jsonify(get_active_alerts())


@api_bp.route("/api/alerts/history", methods=["GET"])
def api_alerts_history():
    """Obtiene el historial de alertas."""
    return jsonify(get_alerts_history_list())
