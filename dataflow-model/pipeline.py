import logging
import apache_beam as beam
from etl.cases.extraction.kafka_source import ReadFromKafkaConfluent
from etl.cases.loading.mongo_sink import MongoWriteFn
from etl.cases.transformation.parsers import parse_json

def build_pipeline_cases(pipeline, config):
    """
    Construye el grafo del pipeline.
    
    Args:
        pipeline (beam.Pipeline): Objeto pipeline donde adjuntar las transformaciones.
        config (module): Módulo de configuración con constantes (dev o prod).
    """
    (
        pipeline
        | "Impulse" >> beam.Create([None]) 
        | "ReadFromKafka" >> beam.ParDo(ReadFromKafkaConfluent(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS, 
            topic=config.KAFKA_TOPIC
        ))
        | "ParseJSON" >> beam.Map(parse_json)
        | "FilterNone" >> beam.Filter(lambda x: x is not None)
        | "Log" >> beam.Map(lambda x: logging.info(f"Procesado: {x}") or x)
        | "WriteToMongo" >> beam.ParDo(MongoWriteFn(
            uri=config.MONGO_URI, 
            db=config.MONGO_DB, 
            coll=config.MONGO_COLL,
            batch_size=config.BATCH_SIZE
        ))
    )
