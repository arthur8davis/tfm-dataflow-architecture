"""
Cargador de configuración desde archivos YAML
"""
import yaml
from pathlib import Path
from typing import Dict, Any


class ConfigLoader:
    """Carga y gestiona archivos de configuración"""

    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self._configs: Dict[str, Any] = {}

    def load_kafka_config(self) -> Dict[str, Any]:
        """Carga configuración de Kafka"""
        if "kafka" not in self._configs:
            config_path = self.config_dir / "kafka_config.yaml"
            self._configs["kafka"] = self._load_yaml(config_path)
        return self._configs["kafka"]

    def load_mongo_config(self) -> Dict[str, Any]:
        """Carga configuración de MongoDB"""
        if "mongo" not in self._configs:
            config_path = self.config_dir / "mongo_config.yaml"
            self._configs["mongo"] = self._load_yaml(config_path)
        return self._configs["mongo"]

    def load_beam_config(self) -> Dict[str, Any]:
        """Carga configuración de Apache Beam"""
        if "beam" not in self._configs:
            config_path = self.config_dir / "beam_config.yaml"
            self._configs["beam"] = self._load_yaml(config_path)
        return self._configs["beam"]

    def get_schema_config(self, schema_name: str) -> Dict[str, Any]:
        """Obtiene configuración específica de un schema"""
        beam_config = self.load_beam_config()
        kafka_config = self.load_kafka_config()
        mongo_config = self.load_mongo_config()

        return {
            "beam": beam_config["beam"]["schemas"].get(schema_name, {}),
            "kafka_topic": kafka_config["kafka"]["topics"].get(schema_name),
            "mongo_collection": mongo_config["mongodb"]["collections"].get(schema_name, {})
        }

    @staticmethod
    def _load_yaml(file_path: Path) -> Dict[str, Any]:
        """Carga un archivo YAML"""
        if not file_path.exists():
            raise FileNotFoundError(f"Config file not found: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
