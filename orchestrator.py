"""
Orquestador para ejecutar múltiples pipelines en paralelo
"""
import sys
import argparse
import logging
from pathlib import Path
from multiprocessing import Process
import importlib.util

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orquestador de pipelines por schema"""

    def __init__(self):
        self.pipelines_dir = Path("pipelines")
        self.available_schemas = self._discover_schemas()

    def _discover_schemas(self):
        """Descubre automáticamente los schemas disponibles"""
        schemas = []
        if not self.pipelines_dir.exists():
            logger.warning(f"Pipelines directory not found: {self.pipelines_dir}")
            return schemas

        for schema_dir in self.pipelines_dir.iterdir():
            if schema_dir.is_dir() and not schema_dir.name.startswith('__'):
                pipeline_file = schema_dir / "pipeline.py"
                if pipeline_file.exists():
                    schemas.append(schema_dir.name)
                    logger.info(f"Discovered schema: {schema_dir.name}")

        return schemas

    def list_schemas(self):
        """Lista todos los schemas disponibles"""
        print("\n🔍 Schemas disponibles:")
        print("=" * 50)
        for schema in self.available_schemas:
            config_file = self.pipelines_dir / schema / "config.yaml"
            if config_file.exists():
                print(f"  ✓ {schema}")
                print(f"    - Pipeline: pipelines/{schema}/pipeline.py")
                print(f"    - Ingestion: pipelines/{schema}/ingestion.py")
                print(f"    - Config: pipelines/{schema}/config.yaml")
                print()
        print("=" * 50)

    def run_pipeline(self, schema_name: str):
        """
        Ejecuta el pipeline de un schema específico

        Args:
            schema_name: Nombre del schema
        """
        if schema_name not in self.available_schemas:
            logger.error(f"Schema not found: {schema_name}")
            logger.info(f"Available schemas: {', '.join(self.available_schemas)}")
            return

        logger.info(f"Starting pipeline for schema: {schema_name}")

        # Importar dinámicamente el módulo del pipeline
        pipeline_file = self.pipelines_dir / schema_name / "pipeline.py"

        spec = importlib.util.spec_from_file_location(
            f"pipelines.{schema_name}.pipeline",
            pipeline_file
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        # Ejecutar el pipeline
        module.main()

    def run_ingestion(self, schema_name: str, **kwargs):
        """
        Ejecuta la ingesta de un schema específico

        Args:
            schema_name: Nombre del schema
            **kwargs: Argumentos adicionales para la ingesta
        """
        if schema_name not in self.available_schemas:
            logger.error(f"Schema not found: {schema_name}")
            return

        logger.info(f"Starting ingestion for schema: {schema_name}")

        # Importar dinámicamente el módulo de ingesta
        ingestion_file = self.pipelines_dir / schema_name / "ingestion.py"

        spec = importlib.util.spec_from_file_location(
            f"pipelines.{schema_name}.ingestion",
            ingestion_file
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        # Obtener la clase de ingesta (asumiendo que sigue el patrón)
        class_name = f"{schema_name.capitalize()}Ingestion"
        ingestion_class = getattr(module, class_name)

        # Ejecutar ingesta
        ingestion = ingestion_class()
        ingestion.run(**kwargs)

    def run_multiple_pipelines(self, schemas: list, parallel: bool = True):
        """
        Ejecuta múltiples pipelines

        Args:
            schemas: Lista de nombres de schemas
            parallel: Si True, ejecuta en paralelo. Si False, secuencial
        """
        logger.info(f"Running pipelines for schemas: {', '.join(schemas)}")

        if parallel:
            processes = []
            for schema in schemas:
                if schema in self.available_schemas:
                    p = Process(target=self.run_pipeline, args=(schema,))
                    p.start()
                    processes.append(p)
                    logger.info(f"Started pipeline process for {schema} (PID: {p.pid})")

            # Esperar a que todos terminen
            for p in processes:
                p.join()

            logger.info("All pipelines completed")
        else:
            for schema in schemas:
                if schema in self.available_schemas:
                    self.run_pipeline(schema)

    def run_multiple_ingestions(self, schemas: list, parallel: bool = True, **kwargs):
        """
        Ejecuta múltiples ingests

        Args:
            schemas: Lista de nombres de schemas
            parallel: Si True, ejecuta en paralelo
            **kwargs: Argumentos para la ingesta
        """
        logger.info(f"Running ingestions for schemas: {', '.join(schemas)}")

        if parallel:
            processes = []
            for schema in schemas:
                if schema in self.available_schemas:
                    p = Process(target=self.run_ingestion, args=(schema,), kwargs=kwargs)
                    p.start()
                    processes.append(p)
                    logger.info(f"Started ingestion process for {schema} (PID: {p.pid})")

            # Esperar a que todos terminen
            for p in processes:
                p.join()

            logger.info("All ingestions completed")
        else:
            for schema in schemas:
                if schema in self.available_schemas:
                    self.run_ingestion(schema, **kwargs)


def main():
    """Función principal"""
    parser = argparse.ArgumentParser(
        description='Orquestador de pipelines multi-schema',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  # Listar schemas disponibles
  python orchestrator.py --list

  # Ejecutar pipeline de un schema
  python orchestrator.py --pipeline cases

  # Ejecutar ingesta de un schema
  python orchestrator.py --ingest cases

  # Ejecutar múltiples pipelines en paralelo
  python orchestrator.py --pipeline cases deaths --parallel

  # Ejecutar todas las ingests
  python orchestrator.py --ingest-all

  # Ejecutar todos los pipelines
  python orchestrator.py --pipeline-all --parallel
        """
    )

    parser.add_argument('--list', action='store_true',
                        help='Listar schemas disponibles')

    parser.add_argument('--pipeline', nargs='+', metavar='SCHEMA',
                        help='Ejecutar pipeline(s) para schema(s) específico(s)')

    parser.add_argument('--ingest', nargs='+', metavar='SCHEMA',
                        help='Ejecutar ingesta(s) para schema(s) específico(s)')

    parser.add_argument('--pipeline-all', action='store_true',
                        help='Ejecutar todos los pipelines')

    parser.add_argument('--ingest-all', action='store_true',
                        help='Ejecutar todas las ingests')

    parser.add_argument('--parallel', action='store_true',
                        help='Ejecutar en paralelo (solo con --pipeline-all o --ingest-all)')

    parser.add_argument('--directory', type=str,
                        help='Directorio para ingesta')

    parser.add_argument('--file', type=str,
                        help='Archivo específico para ingesta')

    args = parser.parse_args()

    orchestrator = PipelineOrchestrator()

    # Listar schemas
    if args.list:
        orchestrator.list_schemas()
        return

    # Ejecutar pipelines específicos
    if args.pipeline:
        if len(args.pipeline) > 1 and args.parallel:
            orchestrator.run_multiple_pipelines(args.pipeline, parallel=True)
        else:
            for schema in args.pipeline:
                orchestrator.run_pipeline(schema)
        return

    # Ejecutar ingests específicas
    if args.ingest:
        kwargs = {}
        if args.directory:
            kwargs['directory'] = args.directory
        if args.file:
            kwargs['file'] = args.file

        if len(args.ingest) > 1 and args.parallel:
            orchestrator.run_multiple_ingestions(args.ingest, parallel=True, **kwargs)
        else:
            for schema in args.ingest:
                orchestrator.run_ingestion(schema, **kwargs)
        return

    # Ejecutar todos los pipelines
    if args.pipeline_all:
        orchestrator.run_multiple_pipelines(
            orchestrator.available_schemas,
            parallel=args.parallel
        )
        return

    # Ejecutar todas las ingests
    if args.ingest_all:
        kwargs = {}
        if args.directory:
            kwargs['directory'] = args.directory

        orchestrator.run_multiple_ingestions(
            orchestrator.available_schemas,
            parallel=args.parallel,
            **kwargs
        )
        return

    # Si no se especificó ninguna acción, mostrar ayuda
    parser.print_help()


if __name__ == "__main__":
    main()
