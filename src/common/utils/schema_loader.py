"""
Cargador de esquemas JSON para validación
"""
import json
from pathlib import Path
from typing import Dict, Any, List


class SchemaLoader:
    """Carga y gestiona esquemas de datos"""

    def __init__(self, schema_dir: str = "schemas"):
        self.schema_dir = Path(schema_dir)
        self._schemas: Dict[str, Dict[str, Any]] = {}

    def load_schema(self, schema_name: str) -> Dict[str, Any]:
        """Carga un esquema específico por nombre"""
        if schema_name not in self._schemas:
            schema_path = self.schema_dir / f"{schema_name}.json"
            self._schemas[schema_name] = self._load_json(schema_path)
        return self._schemas[schema_name]

    def get_available_schemas(self) -> List[str]:
        """Obtiene lista de schemas disponibles"""
        return [f.stem for f in self.schema_dir.glob("*.json")]

    def validate_record(self, schema_name: str, record: Dict[str, Any]) -> tuple[bool, str]:
        """
        Valida un registro contra su schema

        Returns:
            tuple: (is_valid, error_message)
        """
        schema = self.load_schema(schema_name)
        required_fields = schema.get("required_fields", [])
        field_types = schema.get("field_types", {})

        # Validar campos requeridos
        for field in required_fields:
            if field not in record:
                return False, f"Missing required field: {field}"

        # Validar tipos de datos (ignorar valores None en campos opcionales)
        optional_fields = set(schema.get("optional_fields", []))
        for field, expected_type in field_types.items():
            if field in record:
                value = record[field]
                # Permitir None en campos opcionales
                if value is None and field in optional_fields:
                    continue
                if not self._check_type(value, expected_type):
                    return False, f"Invalid type for field {field}: expected {expected_type}, got {type(value).__name__}"

        return True, ""

    @staticmethod
    def _load_json(file_path: Path) -> Dict[str, Any]:
        """Carga un archivo JSON"""
        if not file_path.exists():
            raise FileNotFoundError(f"Schema file not found: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    @staticmethod
    def _check_type(value: Any, expected_type: str) -> bool:
        """Verifica el tipo de un valor"""
        type_mapping = {
            "string": str,
            "integer": int,
            "float": float,
            "boolean": bool,
            "number": (int, float)
        }

        expected_python_type = type_mapping.get(expected_type.lower())
        if expected_python_type is None:
            return True

        return isinstance(value, expected_python_type)
