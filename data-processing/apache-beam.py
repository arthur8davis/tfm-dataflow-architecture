import apache_beam as beam
import json

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
from datetime import datetime
from pymongo import MongoClient, errors
from typing import Dict, Any
# ⚠️ IMPORTANTE: Importar para windowing
from apache_beam.transforms.window import FixedWindows, Sessions
from apache_beam import window

class ParseKafkaMessage(beam.DoFn):
    """Parsea mensajes JSON de Kafka"""
    
    def __init__(self):
        self.message_count = 0

    def process(self, element):
        try:
            # element: (key, value)
            key, value = element
            
            # decodificar el valor JSON
            data = json.loads(value.decode('utf-8'))
            
            self.message_count += 1
            print(f"🔵 [ParseKafkaMessage] Mensaje #{self.message_count} recibido: UUID={data.get('uuid', 'N/A')}")
            
            yield data
        
        except json.JSONDecodeError as e:
            print(f'❌ Error parsing JSON: {e}')
            # No yield = elemento descartado
        except Exception as e:
            print(f'❌ Error parsing message: {e}')
    
    
class EnrichData(beam.DoFn):
    """Enriquece los datos con transformaciones"""
    
    def process(self, element: Dict[str, Any]):
        # MEJORA 1: Usar .copy() para no mutar el original
        enriched = element.copy()
        
        # MEJORA 2: Campo más descriptivo
        enriched['procesado'] = True
        enriched['timestamp_procesamiento'] = datetime.utcnow()
        
        # Direcciones completas
        enriched['direccion_paciente'] = f"{element.get('departamento_paciente', '')} - {element.get('provincia_paciente', '')} - {element.get('distrito_paciente', '')}"
        enriched['direccion_muestra'] = f"{element.get('departamento_muestra', '')} - {element.get('provincia_muestra', '')} - {element.get('distrito_muestra', '')}"
        
        # MEJORA 3: Agregar campos útiles
        edad = element.get('edad', 0)
        if edad:
            if edad < 18:
                enriched['grupo_edad'] = 'MENOR'
            elif edad < 65:
                enriched['grupo_edad'] = 'ADULTO'
            else:
                enriched['grupo_edad'] = 'ADULTO_MAYOR'
        
        # MEJORA 4: Normalizar campos
        if 'sexo' in enriched:
            enriched['sexo'] = enriched['sexo'].upper()
        if 'resultado' in enriched:
            enriched['resultado'] = enriched['resultado'].upper()
        
        yield enriched


class FilterByResult(beam.DoFn):
    """Filtra por resultado (NEGATIVO/POSITIVO)"""
    
    def __init__(self, desired_result):
        self.desired_result = desired_result
        self.process_count = 0
        self.filter_count = 0
        
    def process(self, element: Dict[str, Any]):
        self.process_count += 1
        
        current_result = element.get('resultado', '').upper()
        
        # Solo hacer yield si cumple la condicion
        if current_result == self.desired_result.upper():
            self.filter_count += 1
            yield element
            
    def finish_bundle(self):
        """Se ejecuta después de procesar un lote (bundle) de elementos"""
        if self.process_count > 0:
            porcentaje = (self.filter_count / self.process_count) * 100
            print(f'📊 Bundle procesado: {self.filter_count}/{self.process_count} ({porcentaje:.1f}%) elementos pasaron el filtro')


class WriteToMongoDBCases(beam.DoFn):
    """Escribe registros a MongoDB en lotes (batch inserts)"""
    
    def __init__(self, mongo_uri: str, database: str, collection: str, batch_size: int = 100):
        self.mongo_uri = mongo_uri
        self.database_name = database
        self.collection_name = collection
        self.batch_size = batch_size
        
        # Estos se inicializan en setup() (no en __init__)
        self.client = None
        self.db = None
        self.collection = None
        self.buffer = []
        self.total_inserted = 0
        self.total_errors = 0
        
    def setup(self):
        """Se ejecuta una vez por worker al iniciar"""
        print(f'[MongoDB Writer] Inicializando conexión a {self.database_name}.{self.collection_name}')
        
        try:
            # Crear cliente MongoDB con configuraciones de Producción
            self.client = MongoClient(
                self.mongo_uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000,
                maxPoolSize=50,
                retryWrites=True,
                w='majority',
            )
            
            # Test de conexión
            self.client.admin.command('ping')
            
            # Referencias a DB y colección
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            
            # MEJORA 5: Crear índices con manejo de errores
            try:
                self.collection.create_index('uuid', unique=True)
                self.collection.create_index('fecha_muestra')
                self.collection.create_index('departamento_paciente')
                self.collection.create_index([('resultado', 1), ('fecha_muestra', -1)])
                print(f'[MongoDB Writer] ✅ Índices creados correctamente')
            except errors.OperationFailure as e:
                # Los índices ya existen, no es un error crítico
                print(f'[MongoDB Writer] ⚠️ Índices ya existen: {e}')
            
            print(f'[MongoDB Writer] ✅ Conexión establecida correctamente')
            
        except errors.ConnectionFailure as e:
            print(f'[MongoDB Writer] ❌ Error de conexión: {e}')
            raise
        except Exception as e:
            print(f'[MongoDB Writer] ❌ Error inesperado en setup: {e}')
            raise
        
    def process(self, element: Dict[str, Any]):
        """Se ejecuta por cada elemento"""
        
        # Preparar documento para MongoDB
        document = self._prepare_document(element)
        
        # Agregar al buffer
        self.buffer.append(document)
        
        # Si el buffer está lleno, insertar lote
        if len(self.buffer) >= self.batch_size:
            self._flush_buffer()
            
        # Continuar el flujo (el elemento disponible para otras transformaciones)
        yield element
    
    def finish_bundle(self):
        """Se ejecuta después de procesar un BUNDLE (lote de elementos)"""
        if self.buffer:
            print(f'[MongoDB Writer] finish_bundle: insertando {len(self.buffer)} elementos pendientes')
            self._flush_buffer()
        
    def teardown(self):
        """Se ejecuta cuando el worker termina"""
        
        # Insertar elementos pendientes
        if self.buffer:
            print(f'[MongoDB Writer] teardown: insertando {len(self.buffer)} elementos finales')
            self._flush_buffer()
        
        # Cerrar Conexión
        if self.client:
            self.client.close()
            print(f'[MongoDB Writer] 🔒 Conexión cerrada')
            
        # Estadísticas
        print(f'[MongoDB Writer] 📊 Estadísticas finales:')
        print(f'    - Total insertado: {self.total_inserted}')
        print(f'    - Total errores: {self.total_errors}')
        
    def _prepare_document(self, element: Dict[str, Any]) -> Dict[str, Any]:
        """Prepara el documento para MongoDB"""
        
        document = element.copy()
        
        # MEJORA 6: Usar UTC para timestamps (mejor práctica)
        document['inserted_at'] = datetime.utcnow()
        
        # MEJORA 7: Convertir fecha_muestra a datetime
        if 'fecha_muestra' in document and isinstance(document['fecha_muestra'], str):
            try:
                fecha_str = document['fecha_muestra']
                document['fecha_muestra_date'] = datetime.strptime(fecha_str, '%Y%m%d')
            except ValueError:
                print(f'⚠️ Fecha inválida: {fecha_str}')
        
        # Edad is integer
        if 'edad' in document:
            try:
                document['edad'] = int(document['edad'])
            except (ValueError, TypeError):
                document['edad'] = None
                
        return document
    
    def _flush_buffer(self):
        """Inserta el buffer completo en MongoDB"""
        
        if not self.buffer:
            return
        
        try:
            # Insertar lote completo
            result = self.collection.insert_many(
                self.buffer,
                ordered=False   # Continuar aunque algunos documentos fallen
            )
            
            inserted_count = len(result.inserted_ids)
            self.total_inserted += inserted_count
            
            print(f'[MongoDB Writer] ✅ Insertados {inserted_count} documentos (Total: {self.total_inserted})')
            
            # Limpiar buffer
            self.buffer = []
            
        except errors.BulkWriteError as e:
            # Algunos documentos fallaron (ej: duplicados)
            inserted_count = e.details['nInserted']
            self.total_inserted += inserted_count
            
            # Contar errores que no sean duplicados
            write_errors = e.details.get('writeErrors', [])
            non_duplicate_errors = [
                err for err in write_errors
                if err['code'] != 11000     # 11000 = duplicate key error
            ]
            
            self.total_errors += len(non_duplicate_errors)
            
            if non_duplicate_errors:
                print(f'[MongoDB Writer] ⚠️ Insertados {inserted_count}, errores no-duplicados: {len(non_duplicate_errors)}')
                # Logger primer error para debugging
                print(f'[MongoDB Writer] Ejemplo de error: {non_duplicate_errors[0]}')
            else:
                print(f'[MongoDB Writer] ✅ Insertados {inserted_count}, {len(write_errors)} duplicados ignorados')
                
            # Limpiar buffer
            self.buffer = []
            
        except errors.AutoReconnect as e:
            # Pérdida temporal de conexión
            print(f'[MongoDB Writer] ⚠️ Reconectando a MongoDB: {e}')
            self.total_errors += len(self.buffer)
            # No limpiar buffer - se reintentará en siguiente flush
        
        except Exception as e:
            # Error crítico
            print(f'[MongoDB Writer] ❌ Error crítico insertando lote: {e}')
            self.total_errors += len(self.buffer)
            self.buffer = []  # Limpiar buffer para evitar reintento infinito


def run_pipeline():
    """Pipeline main of Apache Beam"""
    
    # Pipeline Configuration 
    options = PipelineOptions([
        # "--runner=FlinkRunner",
        # "--flink_master=localhost:8081",
        # "--environment_type=DOCKER",
        # "--environment_config=apache/beam_python3.11_sdk:2.60.0"
        "--runner=DirectRunner",
        "--streaming"
    ])
    
    # Kafka Configuration
    kafka_config = {
        'topic': 'cases',
        'bootstrap_servers': 'localhost:9092',
        'consumer_group': 'beam-consumer-group',
        'auto_offset_reset': 'earliest'  # ⚠️ CAMBIO: earliest para leer desde el inicio
    }
    
    # MEJORA 9: MongoDB Configuration
    mongo_config = {
        'uri': 'mongodb://localhost:27017/',
        'database': 'covid_db',
        'collection_all': 'muestras',
        'collection_negatives': 'muestras_negativas',
        'collection_stats': 'estadisticas'
    }
    
    print(f"""
    ╔══════════════════════════════════════════════════════════════╗
    ║              INICIANDO PIPELINE DE STREAMING                 ║
    ╚══════════════════════════════════════════════════════════════╝
    
    🔌 Conectando a Kafka:
       - Broker: {kafka_config['bootstrap_servers']}
       - Topic: {kafka_config['topic']}
       - Consumer Group: {kafka_config['consumer_group']}
       - Offset: {kafka_config['auto_offset_reset']}
    
    💾 Conectando a MongoDB:
       - URI: {mongo_config['uri']}
       - Database: {mongo_config['database']}
    
    ⏳ Pipeline iniciando... esperando mensajes de Kafka
    
    💡 NOTA: El pipeline se quedará ejecutando indefinidamente.
       - Si no ves mensajes, verifica que Kafka esté enviando datos
       - Usa Ctrl+C para detener el pipeline
       - Ejecuta kafka_test_producer.py para enviar mensajes de prueba
    
    ════════════════════════════════════════════════════════════════
    """)
    
    with beam.Pipeline(options=options) as pipeline:
        
        # 1. Read from kafka
        print("🔌 [Pipeline] Configurando ReadFromKafka...")
        messages = (
            pipeline
            | 'Read from Kafka' >> ReadFromKafka(
                consumer_config={
                    'bootstrap.servers': kafka_config['bootstrap_servers'],
                    'group.id': kafka_config['consumer_group'],
                    'auto.offset.reset': kafka_config['auto_offset_reset'],
                    # ⚠️ IMPORTANTE: Todos los valores deben ser STRINGS
                    'enable.auto.commit': 'true',  # ✅ String, no boolean
                    'session.timeout.ms': '30000',  # ✅ String, no int
                    'max.poll.records': '500',
                    'fetch.min.bytes': '1',
                    'fetch.max.wait.ms': '500',
                },
                topics=[kafka_config['topic']],
                with_metadata=False
            )
        )
        print("✅ [Pipeline] ReadFromKafka configurado")
        
        # 2. Parsed messages Json
        print("🔌 [Pipeline] Configurando ParseKafkaMessage...")
        parsed_data = (
            messages
            | 'Parsed JSON' >> beam.ParDo(ParseKafkaMessage())
        )
        print("✅ [Pipeline] ParseKafkaMessage configurado")
        
        # 3. Enrichment data
        enrichment_data = (
            parsed_data
            | 'Enrichment Data' >> beam.ParDo(EnrichData())
        )
        
        # 4. Filter by Result NEGATIVO
        negatives = (
            enrichment_data
            | 'Filter Negatives' >> beam.ParDo(FilterByResult('NEGATIVO'))
        )
        
        # MEJORA 11: También filtrar positivos
        positives = (
            enrichment_data
            | 'Filter Positives' >> beam.ParDo(FilterByResult('POSITIVO'))
        )
        
        # 5. Group by Department (Negativos)
        # ⚠️ CRÍTICO: En streaming necesitamos ventanas de tiempo
        negatives_by_department = (
            negatives
            # Definir ventanas de 60 segundos
            | 'Window 60s (Neg)' >> beam.WindowInto(FixedWindows(60))
            | 'Extract Department (Neg)' >> beam.Map(
                lambda x: (x.get('departamento_paciente', 'DESCONOCIDO'), 1)
            )
            | 'Count by Department (Neg)' >> beam.CombinePerKey(sum)
        )
        
        # MEJORA 12: Group by Department (Positivos)
        positives_by_department = (
            positives
            # Definir ventanas de 60 segundos
            | 'Window 60s (Pos)' >> beam.WindowInto(FixedWindows(60))
            | 'Extract Department (Pos)' >> beam.Map(
                lambda x: (x.get('departamento_paciente', 'DESCONOCIDO'), 1)
            )
            | 'Count by Department (Pos)' >> beam.CombinePerKey(sum)
        )
        
        # ====================================================================
        # ESCRITURA A MONGODB
        # ====================================================================
        
        # Opción A: Escribir TODOS los datos enriquecidos
        enrichment_data | 'Escribir Todos a MongoDB' >> beam.ParDo(
            WriteToMongoDBCases(
                mongo_uri=mongo_config['uri'],
                database=mongo_config['database'],
                collection=mongo_config['collection_all'],
                batch_size=1000
            )
        )
        
        # MEJORA 13: Escribir NEGATIVOS a colección separada
        negatives | 'Escribir Negativos a MongoDB' >> beam.ParDo(
            WriteToMongoDBCases(
                mongo_uri=mongo_config['uri'],
                database=mongo_config['database'],
                collection=mongo_config['collection_negatives'],
                batch_size=500
            )
        )
        
        # MEJORA 14: Escribir ESTADÍSTICAS a MongoDB
        stats_negatives = (
            negatives_by_department
            | 'Preparar Stats Negativos' >> beam.Map(
                lambda x: {
                    'departamento': x[0],
                    'total_negativos': x[1],
                    'total_positivos': 0,
                    'fecha_calculo': datetime.utcnow(),
                    'tipo': 'estadistica_tiempo_real'
                }
            )
            | 'Escribir Stats Negativos' >> beam.ParDo(
                WriteToMongoDBCases(
                    mongo_uri=mongo_config['uri'],
                    database=mongo_config['database'],
                    collection=mongo_config['collection_stats'],
                    batch_size=50
                )
            )
        )
        
        # ====================================================================
        # LOGGING Y MONITORING
        # ====================================================================
        
        # MEJORA 15: Logging más detallado
        negatives_by_department | 'Print Negatives Stats' >> beam.Map(
            lambda x: print(f'📊 Departamento {x[0]}: {x[1]} casos NEGATIVOS')
        )
        
        positives_by_department | 'Print Positives Stats' >> beam.Map(
            lambda x: print(f'⚠️ Departamento {x[0]}: {x[1]} casos POSITIVOS')
        )
        
        # MEJORA 16: Contar total de registros procesados
        # ⚠️ En streaming, también necesita ventanas
        total_count = (
            enrichment_data
            | 'Window 60s (Total)' >> beam.WindowInto(FixedWindows(60))
            | 'Count Total' >> beam.combiners.Count.Globally().without_defaults()
            | 'Print Total' >> beam.Map(
                lambda x: print(f'✅ Total de registros procesados en últimos 60s: {x}')
            )
        )


def run_batch_pipeline():
    """Alternative Pipeline Batch (without streaming)"""
    
    options = PipelineOptions([
        '--runner=DirectRunner',
    ])
        
    with beam.Pipeline(options=options) as pipeline:
        
        # Simulación con datos de ejemplos
        example_data = [
            {
                "uuid": "7895303",
                "fecha_muestra": "20211002",
                "edad": 38,
                "sexo": "FEMENINO",
                "institucion": "GOBIERNO REGIONAL",
                "ubigeo_paciente": "190601",
                "departamento_paciente": "PIURA",
                "provincia_paciente": "SULLANA",
                "distrito_paciente": "SULLANA",
                "departamento_muestra": "PIURA",
                "provincia_muestra": "SULLANA",
                "distrito_muestra": "SULLANA",
                "tipo_muestra": "HISOPADO NASAL Y FARINGEO",
                "resultado": "NEGATIVO"
            },
            {
                "uuid": "7895304",
                "fecha_muestra": "20211002",
                "edad": 65,
                "sexo": "MASCULINO",
                "institucion": "HOSPITAL REGIONAL",
                "departamento_paciente": "LIMA",
                "provincia_paciente": "LIMA",
                "distrito_paciente": "MIRAFLORES",
                "tipo_muestra": "HISOPADO NASAL",
                "resultado": "POSITIVO"
            },
            {
                "uuid": "7895305",
                "fecha_muestra": "20211003",
                "edad": 25,
                "sexo": "FEMENINO",
                "institucion": "CLINICA PRIVADA",
                "departamento_paciente": "PIURA",
                "provincia_paciente": "PIURA",
                "distrito_paciente": "PIURA",
                "tipo_muestra": "HISOPADO NASAL",
                "resultado": "NEGATIVO"
            }
        ]
        
        results = (
            pipeline
            | 'Create Data' >> beam.Create(example_data)
            | 'Process' >> beam.ParDo(EnrichData())
            | 'Filter Negatives' >> beam.ParDo(FilterByResult('NEGATIVO'))
            # MEJORA 17: Escribir a MongoDB en batch
            | 'Write to MongoDB' >> beam.ParDo(
                WriteToMongoDBCases(
                    mongo_uri='mongodb://localhost:27017/',
                    database='covid_db',
                    collection='muestras',
                    batch_size=10
                )
            )
            | 'Print' >> beam.Map(
                lambda x: print(f"✅ Procesado: UUID={x.get('uuid')}, Dept={x.get('departamento_paciente')}")
            )
        )


if __name__ == '__main__':
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║       APACHE BEAM + KAFKA + MONGODB - COVID PIPELINE        ║
    ╚══════════════════════════════════════════════════════════════╝
    
    COLECCIONES MONGODB:
    - covid_db.muestras (todos los datos enriquecidos)
    - covid_db.muestras_negativas (solo negativos)
    - covid_db.estadisticas (agregaciones tiempo real)
    
    WINDOWING (VENTANAS DE TIEMPO):
    - FixedWindows(60): Ventanas fijas de 60 segundos
    - Las estadísticas se calculan cada 60 segundos
    - Necesario para agregaciones en streaming
    
    ⚠️ IMPORTANTE: 
    En pipelines de streaming, TODA agregación (GroupByKey, CombinePerKey, 
    Count.Globally) DEBE tener windowing definido.
    
    VERIFICAR DATOS:
    mongosh
    > use covid_db
    > db.muestras.countDocuments()
    > db.muestras.find().limit(5).pretty()
    > db.estadisticas.find().sort({fecha_calculo: -1})
    
    """)
    
    # Streaming
    run_pipeline()
    
    # Batch (descomentar para pruebas)
    # run_batch_pipeline()