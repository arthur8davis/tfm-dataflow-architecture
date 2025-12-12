import logging
import json
import time
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.mongodbio import WriteToMongoDB
from kafka import KafkaConsumer

# Configuración
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'cases'
MONGO_URI = 'mongodb://admin:admin123@localhost:27017'
MONGO_DB = 'covid-db'
# MONGO_COLL = 'processed_data'
MONGO_COLL = 'cases'

class ReadFromKafkaPython(beam.DoFn):
    """
    Un DoFn que lee de Kafka usando kafka-python.
    Nota: Esto no es una implementación de producción escalable (no soporta paralelismo real ni checkpointing avanzado de Beam),
    pero funciona perfectamente para demos y scripts simples en DirectRunner.
    """
    def __init__(self, bootstrap_servers, topic, group_id=None):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id or f'beam-reader-{int(time.time())}'
        self.consumer = None

    def start_bundle(self):
        # Se conecta al inicio del bundle
        if not self.consumer:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                group_id=self.group_id,
                value_deserializer=lambda x: x.decode('utf-8')
            )

    def process(self, element):
        # 'element' es un impulso dummy para iniciar la lectura
        # Leemos mensajes en un bucle con timeout para ceder el control si es necesario,
        # o simplemente hacemos yield de lo que haya.
        # En DirectRunner, un bucle infinito aquí puede bloquear si no se hace con cuidado,
        # pero para streaming está bien.
        
        # Leemos un batch de mensajes
        records = self.consumer.poll(timeout_ms=1000)
        for topic_partition, messages in records.items():
            for message in messages:
                yield message.value

    def teardown(self):
        if self.consumer:
            self.consumer.close()

def parse_json(json_str):
    try:
        return json.loads(json_str)
    except Exception as e:
        logging.error(f"Error parsing JSON: {e}")
        return None

def run():
    options = PipelineOptions([
        "--runner=DirectRunner",
        "--streaming"
    ])

    with beam.Pipeline(options=options) as p:
        # Generamos un pulso periódico para invocar al lector de Kafka
        # Esto simula un loop de lectura
        (
            p
            # Generamos un elemento cada segundo para activar la lectura
            | "Impulse" >> beam.Create([None]) 
            # O usar PeriodicImpulse si estuviera disponible fácilmente, pero Create + Loop en DoFn es complejo.
            # Mejor estrategia: Un ReadTransform que use Create(None) y luego ParDo(ReadFromKafkaPython) 
            # que EMITA muchos elementos por cada llamada, o sea un SplittableDoFn, 
            # pero hagamos la versión simple: Un DoFn que lee con un loop interno infinito (cuidadoso) o
            # mejor: Usar un generador externo con beam.Create(iterator) NO funciona en streaming infinito fácilmente.
            # 
            # Vamos a usar la técnica de: Create([1]) -> ParDo(ReadInfinite)
            | "ReadFromKafka" >> beam.ParDo(ReadFromKafkaInfinite(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC))
            | "ParseJSON" >> beam.Map(parse_json)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
            | "Log" >> beam.Map(lambda x: logging.info(f"Procesado: {x}") or x)
            # | "WriteToMongo" >> WriteToMongoDB(
            #     uri=MONGO_URI,
            #     db=MONGO_DB,
            #     coll=MONGO_COLL,
            #     batch_size=1
            # )
            | "WriteToMongoCustom" >> beam.ParDo(MongoWriteFn(MONGO_URI, MONGO_DB, MONGO_COLL))
        )

class MongoWriteFn(beam.DoFn):
    def __init__(self, uri, db, coll):
        self.uri = uri
        self.db = db
        self.coll = coll
        self.client = None
        self.collection = None

    def start_bundle(self):
        from pymongo import MongoClient
        if not self.client:
            self.client = MongoClient(self.uri)
            self.collection = self.client[self.db][self.coll]

    def process(self, element):
        try:
            self.collection.insert_one(element)
            logging.info(f"Insertado en Mongo: {element}")
        except Exception as e:
            logging.error(f"Error escribiendo a Mongo: {e}")

    def teardown(self):
        if self.client:
            self.client.close()

class ReadFromKafkaInfinite(beam.DoFn):
    """
    Lee indefinidamente de Kafka.
    """
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    def process(self, element):
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            group_id=f'beam-reader-{int(time.time())}', # Nuevo grupo siempre para demo
            value_deserializer=lambda x: x.decode('utf-8')
        )
        logging.info("Iniciando consumo infinito de Kafka...")
        try:
            for message in consumer:
                yield message.value
        finally:
            consumer.close()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
