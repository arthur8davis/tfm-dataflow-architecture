# Esquema de datos esperado
# Por ahora usamos diccionarios, pero dejamos este archivo como placeholder para Pydantic 
# o lógica de validación futura.

EXPECTED_SCHEMA = {
    "uuid": int,
    "fecha_muestra": int,
    "edad": int,
    "sexo": str,
    "institucion": str,
    "ubigeo_paciente": int,
    "departamento_paciente": str,
    "provincia_paciente": str,
    "distrito_paciente": str,
    "departamento_muestra": str,
    "provincia_muestra": str,
    "distrito_muestra": str,
    "tipo_muestra": str,
    "resultado": str
}

def validate_case(record: dict) -> bool:
    """Valida que el registro tenga los campos mínimos requeridos"""
    required_keys = ["uuid", "resultado", "fecha_muestra"]
    return all(k in record for k in required_keys)
