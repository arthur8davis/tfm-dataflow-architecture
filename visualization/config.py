"""
Configuración del Dashboard COVID-19
"""
import os

# MongoDB
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:admin123@localhost:27017/?authSource=admin&directConnection=true",
)
DATABASE = os.getenv("MONGO_DB", "covid-db")

# Polling
REFRESH_EVERY = float(os.getenv("REFRESH_EVERY", "3.0"))
EVENT_INTERVAL = float(os.getenv("CHANGE_EVENT_INTERVAL", "2.0"))

# Server
PORT = int(os.getenv("PORT", "5006"))
SECRET_KEY = "covid-dashboard-secret"

# Coordenadas de departamentos de Perú
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
