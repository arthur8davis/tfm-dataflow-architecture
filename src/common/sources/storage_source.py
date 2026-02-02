"""
Source para leer desde almacenamiento (CSV/Parquet)
"""
import json
import polars as pl
import apache_beam as beam
from apache_beam.io import fileio
import logging

logger = logging.getLogger(__name__)


class ReadCSVFiles(beam.DoFn):
    """Lee archivos CSV y emite registros"""

    def __init__(self, schema_name: str):
        self.schema_name = schema_name

    def process(self, file_metadata):
        """
        Procesa un archivo CSV

        Args:
            file_metadata: Metadata del archivo

        Yields:
            dict: Registro parseado
        """
        try:
            # Leer el archivo con Polars
            file_path = file_metadata.path
            logger.info(f"Reading CSV file: {file_path}")

            df = pl.read_csv(file_path)

            # Emitir cada registro
            for row in df.iter_rows(named=True):
                yield {
                    "schema": self.schema_name,
                    "data": row
                }

        except Exception as e:
            logger.error(f"Error reading CSV file {file_metadata.path}: {e}")
            yield beam.pvalue.TaggedOutput('dlq', {
                'error': str(e),
                'file_path': file_metadata.path,
                'error_type': 'file_read_error'
            })


class ReadParquetFiles(beam.DoFn):
    """Lee archivos Parquet y emite registros"""

    def __init__(self, schema_name: str):
        self.schema_name = schema_name

    def process(self, file_metadata):
        """
        Procesa un archivo Parquet

        Args:
            file_metadata: Metadata del archivo

        Yields:
            dict: Registro parseado
        """
        try:
            # Leer el archivo con Polars
            file_path = file_metadata.path
            logger.info(f"Reading Parquet file: {file_path}")

            df = pl.read_parquet(file_path)

            # Emitir cada registro
            for row in df.iter_rows(named=True):
                yield {
                    "schema": self.schema_name,
                    "data": row
                }

        except Exception as e:
            logger.error(f"Error reading Parquet file {file_metadata.path}: {e}")
            yield beam.pvalue.TaggedOutput('dlq', {
                'error': str(e),
                'file_path': file_metadata.path,
                'error_type': 'file_read_error'
            })


def create_storage_source(file_pattern: str, schema_name: str, file_type: str = "csv"):
    """
    Crea un source de almacenamiento

    Args:
        file_pattern: Patrón de archivos (ej: "datasets/cases/*.csv")
        schema_name: Nombre del schema
        file_type: Tipo de archivo ("csv" o "parquet")

    Returns:
        PTransform: Transform de lectura de archivos
    """

    def _create_pipeline(pcoll):
        files = pcoll | "Match Files" >> fileio.MatchFiles(file_pattern)

        if file_type.lower() == "csv":
            return files | "Read CSV" >> beam.ParDo(ReadCSVFiles(schema_name)).with_outputs('dlq', main='main')
        elif file_type.lower() == "parquet":
            return files | "Read Parquet" >> beam.ParDo(ReadParquetFiles(schema_name)).with_outputs('dlq', main='main')
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    return _create_pipeline
