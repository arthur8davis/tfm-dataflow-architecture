#!/bin/bash
# Script para ejecutar el pipeline de DEATHS

echo "=================================================="
echo "  DEATHS Pipeline"
echo "=================================================="

ACTION=${1:-pipeline}  # Por defecto ejecuta pipeline

case $ACTION in
  ingest)
    echo "🔄 Running DEATHS ingestion..."
    python pipelines/deaths/ingestion.py "$@"
    ;;
  pipeline)
    echo "🚀 Running DEATHS pipeline..."
    python pipelines/deaths/pipeline.py
    ;;
  both)
    echo "🔄 Running DEATHS ingestion..."
    python pipelines/deaths/ingestion.py
    echo ""
    echo "🚀 Running DEATHS pipeline..."
    python pipelines/deaths/pipeline.py
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
