import logging
import random
import apache_beam as beam
from dataflow_model.cases.extraction.kafka_source import ReadFromKafkaConfluent
from dataflow_model.cases.loading.mongo_sink import MongoWriteFn
from dataflow_model.cases.transformation.parsers import parse_json

def build_pipeline_cases(pipeline, config):
    """
    Construye el grafo del pipeline.
    
    Args:
        pipeline (beam.Pipeline): Objeto pipeline donde adjuntar las transformaciones.
        config (module): Módulo de configuración con constantes (dev o prod).
    """
    (
        pipeline
        | "DriveSource" >> beam.Create(list(range(100000))) 
        | "ReadFromKafka" >> beam.ParDo(ReadFromKafkaConfluent(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS, 
            topic=config.KAFKA_TOPIC
        ))
        | "ParseJSON" >> beam.Map(parse_json)
        | "FilterNone" >> beam.Filter(lambda x: x is not None)
        | "WriteToMongo" >> beam.ParDo(MongoWriteFn(
            uri=config.MONGO_URI, 
            db=config.MONGO_DB, 
            coll=config.MONGO_COLL,
            batch_size=config.BATCH_SIZE
        ))
        # | "DummySink" >> beam.Map(lambda x: logging.info(f"DUMMY SINK: {x.get('uuid')}") or x)
    )

def build_pipeline_cases_native(pipeline, config):
    """
    Construye el pipeline usando la estrategia NATIVA de GroupIntoBatches.
    Recomendada para Cloud Dataflow.
    """
    from dataflow_model.cases.extraction.kafka_source import ReadFromKafkaConfluent
    from dataflow_model.cases.transformation.parsers import parse_json
    from dataflow_model.cases.loading.mongo_sink import MongoWriteBatchFn
    
    (
        pipeline
        | "DriveSource" >> beam.Create(list(range(100000))) 
        | "ReadFromKafka" >> beam.ParDo(ReadFromKafkaConfluent(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS, 
            topic=config.KAFKA_TOPIC
        ))
        | "ParseJSON" >> beam.Map(parse_json)
        | "FilterNone" >> beam.Filter(lambda x: x is not None)
        
        # --- Lógica GroupIntoBatches ---
        # 1. Sharding: Asignar clave aleatoria para distribución
        | "AddShardKey" >> beam.Map(lambda x: (random.randint(0, 10), x))
        
        # 2. Agrupamiento Nativo
        | "GroupIntoBatches" >> beam.GroupIntoBatches(
            batch_size=config.BATCH_SIZE,
            max_buffering_duration_secs=5 # Timeout para evitar bloqueos
        )
        
        # 3. Extraer solo los valores (quitar la clave del shard)
        | "GetValues" >> beam.Values()
        
        # 4. Escribir el lote directo
        | "WriteNativeBatch" >> beam.ParDo(MongoWriteBatchFn(
            uri=config.MONGO_URI, 
            db=config.MONGO_DB, 
            coll=config.MONGO_COLL
        ))
    )
