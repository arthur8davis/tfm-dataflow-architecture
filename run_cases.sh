#!/bin/bash
# Script para ejecutar el pipeline de CASES

echo "=================================================="
echo "  CASES Pipeline"
echo "=================================================="

ACTION=${1:-pipeline}  # Por defecto ejecuta pipeline

case $ACTION in
  ingest)
    echo "🔄 Running CASES ingestion..."
    python pipelines/cases/ingestion.py "$@"
    ;;
  pipeline)
    echo "🚀 Running CASES pipeline..."
    python pipelines/cases/pipeline.py
    ;;
  both)
    echo "🔄 Running CASES ingestion..."
    python pipelines/cases/ingestion.py
    echo ""
    echo "🚀 Running CASES pipeline..."
    python pipelines/cases/pipeline.py
    ;;
  *)
    echo "Usage: $0 [ingest|pipeline|both]"
    echo ""
    echo "Examples:"
    echo "  $0 ingest          # Run only ingestion"
    echo "  $0 pipeline        # Run only pipeline (default)"
    echo "  $0 both            # Run ingestion then pipeline"
    exit 1
    ;;
esac

echo ""
echo "✅ Done!"
