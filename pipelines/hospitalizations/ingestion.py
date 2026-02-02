"""
Script de ingesta para el schema HOSPITALIZATIONS
"""
import os
import sys
import yaml
import argparse
from pathlib import Path
import logging

# Agregar el directorio raíz al path
ROOT_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.ingestion.kafka_processor import KafkaProcessor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HospitalizationsIngestion:
    """Ingesta de datos para el schema HOSPITALIZATIONS"""

    def __init__(self, config_path: str = None):
        """
        Inicializa la ingesta

        Args:
            config_path: Ruta al archivo de configuración
        """
        if config_path is None:
            config_path = Path(__file__).parent / "config.yaml"

        self.config = self._load_config(config_path)
        self.schema_name = self.config['schema']['name']

    def _load_config(self, config_path: Path) -> dict:
        """Carga la configuración desde YAML"""
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def run(self, directory: str = None, file: str = None, file_type: str = "csv"):
        """
        Ejecuta la ingesta

        Args:
            directory: Directorio con archivos a procesar
            file: Archivo específico a procesar
            file_type: Tipo de archivo
        """
        logger.info(f"Starting ingestion for schema: {self.schema_name}")

        kafka_config = self.config['source']['kafka']

        # Crear procesador
        processor = KafkaProcessor(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            producer_config={
                'acks': 'all',
                'compression.type': 'snappy'
            }
        )

        # Asegurar que el topic existe
        processor.ensure_topic_exists(kafka_config['topic'])

        try:
            if file:
                # Procesar archivo específico
                logger.info(f"Processing file: {file}")
                if file_type == 'csv':
                    processor.process_csv(file, kafka_config['topic'], self.schema_name)
                else:
                    processor.process_parquet(file, kafka_config['topic'], self.schema_name)

            elif directory:
                # Procesar directorio
                logger.info(f"Processing directory: {directory}")
                processor.process_directory(directory, kafka_config['topic'], self.schema_name)

            else:
                # Procesar directorio por defecto
                default_dir = f"datasets/{self.schema_name}"
                logger.info(f"Processing default directory: {default_dir}")
                processor.process_directory(default_dir, kafka_config['topic'], self.schema_name)

            logger.info("Ingestion completed successfully")

        except Exception as e:
            logger.error(f"Error during ingestion: {e}", exc_info=True)

        finally:
            processor.close()


def main():
    """Función principal"""
    parser = argparse.ArgumentParser(description='Ingesta de datos para HOSPITALIZATIONS')
    parser.add_argument('--directory', type=str, help='Directorio con archivos')
    parser.add_argument('--file', type=str, help='Archivo específico')
    parser.add_argument('--file-type', type=str, choices=['csv', 'parquet'], default='csv')

    args = parser.parse_args()

    ingestion = HospitalizationsIngestion()
    ingestion.run(directory=args.directory, file=args.file, file_type=args.file_type)


if __name__ == "__main__":
    main()
