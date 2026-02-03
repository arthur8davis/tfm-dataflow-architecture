"""
COVID-19 Dashboard - Flask + Socket.IO
- Emite actualizaciones en tiempo real usando polling
- Arquitectura modular con handlers y services separados
"""

import os
import sys
import time
import threading
import subprocess
import signal
from datetime import datetime
from pathlib import Path

# Agregar path para imports del proyecto
ROOT_DIR = Path(__file__).parent
sys.path.insert(0, str(ROOT_DIR))
sys.path.insert(0, str(ROOT_DIR.parent))

from flask import Flask
from flask_socketio import SocketIO
from flask_cors import CORS

from config import REFRESH_EVERY, PORT, SECRET_KEY
from services import db
from routes.api import api_bp
from handlers.websocket.events import register_websocket_handlers
from handlers.queries.summary import get_summary_data
from handlers.queries.cases import (
    get_cases_by_department,
    get_cases_by_sex,
    get_cases_by_date,
    get_cases_by_age,
    get_heatmap_data,
)
from handlers.queries.demises import (
    get_demises_by_department,
    get_demises_by_sex,
    get_demises_heatmap_data,
)
from handlers.alerts import check_all_thresholds, get_active_alerts, alerts_lock

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
app.config["SECRET_KEY"] = SECRET_KEY
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

# Registrar Blueprint de rutas API
app.register_blueprint(api_bp)

# -----------------------------------------------------------------------------
# Control de intervalo de refresco
# -----------------------------------------------------------------------------
_interval_lock = threading.Lock()
_refresh_interval = REFRESH_EVERY

def get_refresh_interval():
    with _interval_lock:
        return _refresh_interval

def set_refresh_interval(seconds):
    global _refresh_interval
    with _interval_lock:
        _refresh_interval = seconds

# Registrar handlers de WebSocket
register_websocket_handlers(socketio, _interval_lock, get_refresh_interval, set_refresh_interval)

# -----------------------------------------------------------------------------
# Polling: último conteo conocido para detectar cambios
# -----------------------------------------------------------------------------
_last_cases_count = 0
_last_demises_count = 0

# -----------------------------------------------------------------------------
# Refresh / cache
# -----------------------------------------------------------------------------
_cache_lock = threading.Lock()
_cache_summary = None
_cache_cases_department = None
_cache_cases_sex = None
_cache_cases_timeline = None
_cache_cases_age = None
_cache_demises_dept = None
_cache_demises_sex = None
_cache_ts = None


def refresh_cases():
    """Refresca datos de casos y emite a clientes."""
    try:
        summary = get_summary_data()
        dept = get_cases_by_department()
        sex = get_cases_by_sex()
        timeline = get_cases_by_date()
        age = get_cases_by_age()

        with _cache_lock:
            global _cache_summary, _cache_cases_department, _cache_cases_sex
            global _cache_cases_timeline, _cache_cases_age, _cache_ts
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

        try:
            check_all_thresholds(summary, dept_data=dept[:5] if dept else None)
            with alerts_lock:
                socketio.emit('active_alerts', get_active_alerts())
        except Exception as e:
            print(f"[Refresh Cases] Error en alertas: {e}")
    except Exception as e:
        print(f"[Refresh Cases] Error: {e}")


def refresh_demises():
    """Refresca datos de fallecidos y emite a clientes."""
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

        try:
            check_all_thresholds(summary, demises_dept=dem_dept[:5] if dem_dept else None)
            with alerts_lock:
                socketio.emit('active_alerts', get_active_alerts())
        except Exception as e:
            print(f"[Refresh Demises] Error en alertas: {e}")
    except Exception as e:
        print(f"[Refresh Demises] Error: {e}")


def refresh_loop():
    """Loop de polling para detectar cambios en colecciones time-series."""
    global _last_cases_count, _last_demises_count

    print(f"[Polling] Iniciado con intervalo {_refresh_interval}s")

    if db is not None:
        try:
            _last_cases_count = db.cases.estimated_document_count()
            _last_demises_count = db.demises.estimated_document_count()
            print(f"[Polling] Conteo inicial - Cases: {_last_cases_count}, Demises: {_last_demises_count}")
        except Exception as e:
            print(f"[Polling] Error obteniendo conteo inicial: {e}")

    while True:
        interval = get_refresh_interval()
        time.sleep(interval)

        if db is None:
            continue

        try:
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


def start_polling():
    """Inicia el loop de polling en background."""
    print("[Info] Usando polling para detectar cambios en colecciones time-series")
    socketio.start_background_task(refresh_loop)
    print(f"[Polling] Loop iniciado cada {_refresh_interval}s")


# -----------------------------------------------------------------------------
# Endpoint adicional para intervalo de refresco
# -----------------------------------------------------------------------------
@app.route("/api/refresh-interval", methods=["GET"])
def api_refresh_interval():
    from flask import jsonify
    return jsonify({"refresh_interval_seconds": get_refresh_interval()})


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 50)
    print("COVID-19 Dashboard - Real-time con WebSockets")
    print("=" * 50)

    start_polling()

    def free_port(port: int):
        """Si el puerto está ocupado, mata procesos que lo usan."""
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
            time.sleep(0.5)
        except subprocess.CalledProcessError:
            pass

    free_port(PORT)

    socketio.run(app, host="0.0.0.0", port=PORT, debug=True, use_reloader=False, allow_unsafe_werkzeug=True)
