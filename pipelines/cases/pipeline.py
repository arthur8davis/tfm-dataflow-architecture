"""
Pipeline específico para el schema CASES
"""
import sys
import yaml
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from pathlib import Path
import logging

# Agregar el directorio raíz al path para imports
ROOT_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

# Imports de componentes comunes
from src.common.sources.kafka_source_native import create_kafka_source_native
from src.common.sources.storage_source import create_storage_source
from src.common.transforms.normalize import NormalizeRecord
from src.common.transforms.validate import ValidateSchema
from src.common.transforms.timestamp import AssignTimestamp
from src.common.transforms.windowing import create_windowing_transform, LogWindow
from src.common.transforms.metadata import AddMetadata
from src.common.batching.manual_batch import GroupIntoBatches
from src.common.batching.native_batch import NativeBatcher
from src.common.sinks.mongo_sink import MongoDBSink
from src.common.sinks.dlq_sink import DLQSink, CombineDLQErrors
from src.common.transforms.enrich_geo import EnrichGeoFromUbigeo
from src.common.transforms.compute_metrics import create_compute_metrics_branch
from src.common.transforms.predictive_model import create_analytics_branch
from src.common.sinks.metrics_sink import MetricsSink
from src.common.sinks.anomalies_sink import AnomaliesSink
from src.common.sinks.predictions_sink import PredictionsSink

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CasesPipeline:
    """Pipeline para procesar datos de CASES"""

    def __init__(self, config_path: str = None, mode: str = "streaming"):
        """
        Inicializa el pipeline de cases

        Args:
            config_path: Ruta al archivo de configuración
            mode: Modo de ejecución ("streaming" o "batch")
        """
        if config_path is None:
            config_path = Path(__file__).parent / "config.yaml"

        self.config = self._load_config(config_path)
        self.schema_name = self.config['schema']['name']

        # Configurar modo de ejecución
        if mode == "batch":
            self.config['source']['type'] = "storage"
            self.config['pipeline']['streaming'] = False
        else:
            self.config['source']['type'] = "kafka"
            self.config['pipeline']['streaming'] = True

        logger.info(f"Initializing pipeline for schema: {self.schema_name} (mode: {mode})")

    def _load_config(self, config_path: Path) -> dict:
        """Carga la configuración desde YAML"""
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def build(self) -> beam.Pipeline:
        """
        Construye el pipeline completo

        Returns:
            beam.Pipeline: Pipeline construido
        """
        # Configurar opciones del pipeline
        options = PipelineOptions()
        options.view_as(StandardOptions).runner = self.config['pipeline']['runner']

        pipeline = beam.Pipeline(options=options)

        logger.info(f"Building pipeline for schema: {self.schema_name}")

        # Construir según el tipo de source
        source_type = self.config['source']['type']

        if source_type == "kafka":
            processed_data, dlq_list = self._build_kafka_pipeline(pipeline)
        elif source_type == "storage":
            processed_data, dlq_list = self._build_storage_pipeline(pipeline)
        else:
            raise ValueError(f"Unknown source type: {source_type}")

        # Rama analítica: Métricas descriptivas por ventana
        if hasattr(self, '_windowed_data') and self._windowed_data is not None:
            metrics = create_compute_metrics_branch(
                self._windowed_data, schema_name='cases'
            )
            _ = (
                metrics
                | "Write Metrics" >> beam.ParDo(
                    MetricsSink(
                        connection_string=self.config['sink']['mongodb']['connection_string'],
                        database=self.config['sink']['mongodb']['database'],
                        collection='metrics_cases'
                    )
                )
            )

            # Rama analítica: Anomalías + Predicciones (5.11.2 + 5.12.3)
            anomalies, predictions = create_analytics_branch(
                metrics, schema_name='cases', label_prefix="Cases"
            )

            _ = (
                anomalies
                | "Write Anomalies" >> beam.ParDo(
                    AnomaliesSink(
                        connection_string=self.config['sink']['mongodb']['connection_string'],
                        database=self.config['sink']['mongodb']['database'],
                        collection='anomalies_cases'
                    )
                )
            )

            _ = (
                predictions
                | "Write Predictions" >> beam.ParDo(
                    PredictionsSink(
                        connection_string=self.config['sink']['mongodb']['connection_string'],
                        database=self.config['sink']['mongodb']['database'],
                        collection='predictions_cases'
                    )
                )
            )

        # Sink: Escribir a MongoDB
        write_results = (
            processed_data
            | f"Write to MongoDB" >> beam.ParDo(
                MongoDBSink(
                    connection_string=self.config['sink']['mongodb']['connection_string'],
                    database=self.config['sink']['mongodb']['database'],
                    collection_config=self.config['sink']['mongodb']['collection']
                )
            ).with_outputs('dlq', main='main')
        )

        # Agregar DLQ del sink y combinar todas
        dlq_list.append(write_results.dlq)
        all_dlq = CombineDLQErrors(*dlq_list).expand(pipeline)

        # Escribir DLQ
        _ = (
            all_dlq
            | f"Write to DLQ" >> beam.ParDo(
                DLQSink(
                    connection_string=self.config['sink']['mongodb']['connection_string'],
                    database=self.config['sink']['mongodb']['database'],
                    dlq_collection=self.config['sink']['dlq']['collection']
                )
            )
        )

        return pipeline

    def _build_kafka_pipeline(self, pipeline):
        """Construye pipeline con source de Kafka usando confluent_kafka nativo"""
        logger.info("Building native Kafka source (confluent_kafka)")

        kafka_config = self.config['source']['kafka']

        # Source: Leer desde Kafka usando confluent_kafka nativo
        # El source nativo ya parsea los mensajes y devuelve main/dlq
        read_result = (
            pipeline
            | "Read from Kafka" >> create_kafka_source_native(
                bootstrap_servers=kafka_config['bootstrap_servers'],
                topic=kafka_config['topic'],
                consumer_config=kafka_config['consumer_config'],
                max_messages=kafka_config.get('max_messages'),
                max_time_seconds=kafka_config.get('max_time_seconds')
            )
        )

        return self._build_transforms(read_result.main, read_result.dlq, source_type="kafka")

    def _build_storage_pipeline(self, pipeline):
        """Construye pipeline con source de storage"""
        logger.info("Building Storage source")

        storage_config = self.config['source']['storage']

        # Source: Leer desde archivos
        read_result = create_storage_source(
            file_pattern=storage_config['file_pattern'],
            schema_name=self.schema_name,
            file_type=storage_config['file_type']
        )(pipeline)

        return self._build_transforms(read_result.main, read_result.dlq, source_type="storage")

    def _build_transforms(self, main_data, dlq_data, source_type: str = "unknown"):
        """Construye la capa de transformaciones"""
        transforms_config = self.config['transforms']

        # Acumular todas las PCollections DLQ en una lista
        dlq_list = [dlq_data] if dlq_data is not None else []

        # Normalize
        if transforms_config['normalize']['enabled']:
            normalized = (
                main_data
                | "Normalize" >> beam.ParDo(NormalizeRecord()).with_outputs('dlq', main='main')
            )
            dlq_list.append(normalized.dlq)
            main_data = normalized.main

        # Enrich Geo (agregar latitud/longitud basado en departamento/provincia/distrito de muestra)
        enriched = (
            main_data
            | "Enrich Geo" >> beam.ParDo(
                EnrichGeoFromUbigeo(
                    ubigeo_field="ubigeo_paciente",
                    dept_field="departamento_muestra",
                    prov_field="provincia_muestra",
                    dist_field="distrito_muestra"
                )
            ).with_outputs('dlq', main='main')
        )
        dlq_list.append(enriched.dlq)
        main_data = enriched.main
        
        # Validate
        if transforms_config['validate']['enabled']:
            schema_file = Path(__file__).parent / "schema.json"
            validated = (
                main_data
                | "Validate Schema" >> beam.ParDo(
                    ValidateSchema(schema_dir=str(schema_file.parent))
                ).with_outputs('dlq', main='main')
            )
            dlq_list.append(validated.dlq)
            main_data = validated.main

        # Assign Timestamp
        if transforms_config['timestamp']['enabled']:
            timestamped = (
                main_data
                | "Assign Timestamp" >> beam.ParDo(
                    AssignTimestamp(timestamp_field=transforms_config['timestamp']['field'])
                ).with_outputs('dlq', main='main')
            )
            dlq_list.append(timestamped.dlq)
            main_data = timestamped.main

        # Windowing
        if transforms_config['windowing']['enabled']:
            windowing_config = transforms_config['windowing']
            main_data = (
                main_data
                | "Apply Windowing" >> create_windowing_transform(
                    window_size_seconds=windowing_config['window_size_seconds'],
                    allowed_lateness_seconds=windowing_config['allowed_lateness_seconds'],
                    trigger_type=windowing_config['trigger']
                )
                | "Log Window" >> beam.ParDo(LogWindow())
            )
            # Guardar referencia para rama analítica (métricas por ventana)
            self._windowed_data = main_data

        # Metadata
        if transforms_config['metadata']['enabled']:
            with_metadata = (
                main_data
                | "Add Metadata" >> beam.ParDo(
                    AddMetadata(
                        pipeline_version=transforms_config['metadata']['pipeline_version'],
                        source_type=source_type  # "kafka" o "storage"
                    )
                ).with_outputs('dlq', main='main')
            )
            dlq_list.append(with_metadata.dlq)
            main_data = with_metadata.main

        # Batching
        batching_config = self.config['batching']
        if batching_config['strategy'] == 'manual':
            main_data = (
                main_data
                | "Manual Batching" >> GroupIntoBatches(
                    batch_size=batching_config['batch_size'],
                    batch_timeout_seconds=batching_config['batch_timeout_seconds']
                )
            )
        else:
            main_data = (
                main_data
                | "Native Batching" >> NativeBatcher(
                    batch_size=batching_config['batch_size'],
                    max_buffering_duration_secs=batching_config['batch_timeout_seconds']
                )
            )

        return main_data, dlq_list

    def run(self):
        """Ejecuta el pipeline"""
        logger.info(f"Starting pipeline for schema: {self.schema_name}")

        pipeline = self.build()
        result = pipeline.run()
        result.wait_until_finish()

        logger.info(f"Pipeline completed for schema: {self.schema_name}")


def main():
    """Función principal"""
    parser = argparse.ArgumentParser(description='Pipeline CASES - COVID-19')
    parser.add_argument(
        '--mode',
        type=str,
        choices=['streaming', 'batch'],
        default='batch',
        help='Modo de ejecución: streaming (Kafka) o batch (Storage)'
    )
    args, unknown = parser.parse_known_args()
    if unknown:
        logger.debug(f"Ignoring unknown args: {unknown}")

    pipeline = CasesPipeline(mode=args.mode)
    pipeline.run()


if __name__ == "__main__":
    main()
