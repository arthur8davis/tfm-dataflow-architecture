"""
Servicio de conexión a MongoDB
"""
import sys
from pathlib import Path

# Agregar path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pymongo import MongoClient
from config import MONGO_URI, DATABASE

mongo_client = None
db = None

try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=1000)
    mongo_client.admin.command("ping")
    db = mongo_client[DATABASE]
    print(f"[Mongo] Conectado a {MONGO_URI}")
except Exception as e:
    print(f"[Mongo] No se pudo conectar: {e}")
