#!/bin/bash
# Ejemplo completo de ejecución de múltiples schemas

set -e  # Detener en caso de error

echo "=========================================="
echo "  DEMO: Procesamiento Multi-Schema"
echo "=========================================="
echo ""

# Colores para output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Función de logging
log() {
    echo -e "${GREEN}[$(date +'%H:%M:%S')]${NC} $1"
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# 1. Verificar servicios
log "Verificando servicios Docker..."
if ! docker-compose ps | grep -q "Up"; then
    log_warn "Servicios no están corriendo. Iniciando..."
    docker-compose up -d
    log "Esperando 10 segundos para que los servicios inicien..."
    sleep 10
else
    log_info "Servicios ya están corriendo"
fi

echo ""
echo "=========================================="
echo "  PASO 1: Listar Schemas Disponibles"
echo "=========================================="
python orchestrator.py --list

echo ""
echo "=========================================="
echo "  PASO 2: Ingesta de Datos"
echo "=========================================="

log "Ingesta individual de CASES..."
python orchestrator.py --ingest cases
echo ""

log "Ingesta individual de DEATHS..."
python orchestrator.py --ingest deaths
echo ""

log_info "Ingesta completada para ambos schemas"

echo ""
echo "=========================================="
echo "  PASO 3: Ejecución de Pipelines"
echo "=========================================="

log_warn "Esta demo ejecuta los pipelines de manera secuencial"
log_warn "En producción, usa --parallel para ejecución simultánea"
echo ""

log "Ejecutando pipeline de CASES..."
echo "Presiona Ctrl+C después de unos segundos (es un pipeline de streaming)..."
timeout 30 python orchestrator.py --pipeline cases || true
echo ""

log "Ejecutando pipeline de DEATHS..."
echo "Presiona Ctrl+C después de unos segundos (es un pipeline de streaming)..."
timeout 30 python orchestrator.py --pipeline deaths || true
echo ""

echo ""
echo "=========================================="
echo "  DEMO COMPLETADA"
echo "=========================================="
echo ""
log_info "Servicios de monitoreo:"
echo "  - Kafka UI:      http://localhost:8080"
echo "  - Mongo Express: http://localhost:8083"
echo ""
log_info "Verificar datos en MongoDB:"
echo "  docker exec -it \$(docker-compose ps -q mongodb) mongosh -u root -p example"
echo "  > use covid-db"
echo "  > db.cases.countDocuments()"
echo "  > db.deaths.countDocuments()"
echo "  > db.dead_letter_queue.countDocuments()"
echo ""
log_info "Para ejecutar pipelines en paralelo:"
echo "  python orchestrator.py --pipeline cases deaths --parallel"
echo ""
