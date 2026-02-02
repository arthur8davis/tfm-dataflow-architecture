#!/bin/bash
# Script para verificar que la estructura del proyecto es correcta

set -e

echo "🔍 Verificando estructura del proyecto..."
echo ""

# Colores
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

success() {
    echo -e "${GREEN}✓${NC} $1"
}

error() {
    echo -e "${RED}✗${NC} $1"
}

warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

ERRORS=0

# 1. Verificar que NO existan carpetas legacy
echo "1. Verificando que NO existan carpetas legacy..."

if [ -d "config" ]; then
    error "La carpeta 'config/' todavía existe (debería estar eliminada)"
    ERRORS=$((ERRORS + 1))
else
    success "Carpeta 'config/' eliminada correctamente"
fi

if [ -d "schemas" ]; then
    error "La carpeta 'schemas/' todavía existe (debería estar eliminada)"
    ERRORS=$((ERRORS + 1))
else
    success "Carpeta 'schemas/' eliminada correctamente"
fi

if [ -d "src/pipeline" ]; then
    error "La carpeta 'src/pipeline/' todavía existe (debería estar eliminada)"
    ERRORS=$((ERRORS + 1))
else
    success "Carpeta 'src/pipeline/' eliminada correctamente"
fi

if [ -d "src/utils" ]; then
    error "La carpeta 'src/utils/' todavía existe (debería estar eliminada)"
    ERRORS=$((ERRORS + 1))
else
    success "Carpeta 'src/utils/' eliminada correctamente"
fi

echo ""

# 2. Verificar que existan las carpetas necesarias
echo "2. Verificando carpetas necesarias..."

REQUIRED_DIRS=(
    "pipelines/cases"
    "pipelines/deaths"
    "src/common/batching"
    "src/common/sinks"
    "src/common/sources"
    "src/common/transforms"
    "src/common/utils"
    "src/ingestion"
    "datasets/cases"
    "datasets/deaths"
)

for dir in "${REQUIRED_DIRS[@]}"; do
    if [ -d "$dir" ]; then
        success "Carpeta '$dir' existe"
    else
        error "Carpeta '$dir' NO existe"
        ERRORS=$((ERRORS + 1))
    fi
done

echo ""

# 3. Verificar archivos de pipelines
echo "3. Verificando archivos de pipelines..."

PIPELINE_FILES=(
    "pipelines/cases/config.yaml"
    "pipelines/cases/schema.json"
    "pipelines/cases/pipeline.py"
    "pipelines/cases/ingestion.py"
    "pipelines/deaths/config.yaml"
    "pipelines/deaths/schema.json"
    "pipelines/deaths/pipeline.py"
    "pipelines/deaths/ingestion.py"
)

for file in "${PIPELINE_FILES[@]}"; do
    if [ -f "$file" ]; then
        success "Archivo '$file' existe"
    else
        error "Archivo '$file' NO existe"
        ERRORS=$((ERRORS + 1))
    fi
done

echo ""

# 4. Verificar archivos principales
echo "4. Verificando archivos principales..."

MAIN_FILES=(
    "orchestrator.py"
    "docker-compose.yaml"
    "requirements.txt"
    "README_NEW.md"
    "QUICKSTART.md"
    "ARCHITECTURE.md"
)

for file in "${MAIN_FILES[@]}"; do
    if [ -f "$file" ]; then
        success "Archivo '$file' existe"
    else
        error "Archivo '$file' NO existe"
        ERRORS=$((ERRORS + 1))
    fi
done

echo ""

# 5. Verificar que NO existan scripts legacy
echo "5. Verificando que NO existan scripts legacy..."

LEGACY_FILES=(
    "run_ingestion.py"
    "run_pipeline.py"
    "create_schema.py"
)

for file in "${LEGACY_FILES[@]}"; do
    if [ -f "$file" ]; then
        warn "Archivo legacy '$file' todavía existe (puede eliminarse)"
    else
        success "Archivo legacy '$file' eliminado correctamente"
    fi
done

echo ""

# 6. Verificar imports en pipelines
echo "6. Verificando imports en pipelines..."

if grep -r "from src.pipeline" pipelines/ src/ 2>/dev/null; then
    error "Encontrados imports a 'src.pipeline' (carpeta eliminada)"
    ERRORS=$((ERRORS + 1))
else
    success "No hay imports a carpetas eliminadas"
fi

if grep -r "from src.utils" pipelines/ 2>/dev/null | grep -v "from src.common.utils" ; then
    error "Encontrados imports a 'src.utils' (carpeta eliminada)"
    ERRORS=$((ERRORS + 1))
else
    success "No hay imports a 'src.utils' (debe ser 'src.common.utils')"
fi

echo ""

# Resumen final
echo "=========================================="
if [ $ERRORS -eq 0 ]; then
    echo -e "${GREEN}✅ Estructura del proyecto CORRECTA${NC}"
    echo ""
    echo "La estructura está limpia y lista para usar:"
    echo "  - Carpetas legacy eliminadas"
    echo "  - Carpetas necesarias presentes"
    echo "  - Archivos de pipelines completos"
    echo "  - Imports correctos"
    echo ""
    echo "Próximos pasos:"
    echo "  1. Instalar dependencias: pip install -r requirements.txt"
    echo "  2. Iniciar servicios: docker-compose up -d"
    echo "  3. Ejecutar demo: ./example_run_all.sh"
else
    echo -e "${RED}❌ Errores encontrados: $ERRORS${NC}"
    echo ""
    echo "Por favor, corrige los errores antes de continuar."
fi
echo "=========================================="

exit $ERRORS
