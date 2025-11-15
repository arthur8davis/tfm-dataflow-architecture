from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema

from pymongo import MongoClient

from datetime import datetime

import os
import sys

# Configuration connection to MongoDB
MONGO_USERNAME = "admin"
MONGO_PASSWORD = "admin123"
MONGO_URI = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@localhost:27017/"
MONGO_DB = "covid_db"
MONGO_COLLECTION = "positive_cases"

def get_mongo_client():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        return client
    except Exception as e:
        return None

# Configuration for the Flink environment
env = StreamExecutionEnvironment.get_execution_environment()

# Add JAR connector of Kafka to the Flink environment
jar_path = os.path.expanduser("~/flink-jars/flink-sql-connector-kafka-4.0.0-2.0.jar")

# Load JAR connector of Kafka into the Flink environment
env.add_jars(f"file://{jar_path}")

# Define the Kafka source with configuration
kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("localhost:9092") \
        .set_topics("covid-cases") \
        .set_group_id("group_cases_covid") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
        
# Create a DataStream from the Kafka source
ds = env.from_source(
    kafka_source,
    watermark_strategy=WatermarkStrategy.no_watermarks(),
    source_name="Kafka Source"
    )

# Process the data stream to extract relevant information and perform
def parse_and_filter(json_str):
    """
    Parsea el JSON y filtra casos confirmados de COVID (POSITIVO)
    Estructura esperada: {"registers": {"RESULTADO": "POSITIVO", "UUID": "...", ...}}
    """
    import json
    
    try:
        # Parsear JSON si es string
        if isinstance(json_str, str):
            data = json.loads(json_str)
        elif isinstance(json_str, dict):
            data = json_str
        else:
            print(f"Tipo inesperado: {type(json_str)}")
            return None
        
        # Debug: mostrar estructura del batch
        batch_num = data.get("number_batch", 0)
        rows_batch = data.get("rows_batch", 0)
        print(f'Batch #{batch_num} con {rows_batch} filas')
        
        # Obtener columnas y registros
        # columns = data.get("columns", [])
        registers_str = data.get("registers", [])
        
        if isinstance(registers_str, str):
            registers = json.loads(registers_str)
        else:
            registers = registers_str
        
        print(f"registros:\n {type(registers)}")
        print(f'hora de ejecucion: {datetime.now()}')
  
        
        # Procesar cada registro del batch
        positive_cases_list = []
        
        for row in registers:
            if isinstance(row, dict):
                result = row.get("RESULTADO")
                
                if result == "POSITIVO":
                    # Crear documento para MongoDB con los campos que necesites
                    case = {
                        "uuid": row.get("UUID"),
                        "resultado": result,
                        "fecha_muestra": row.get("FECHA_MUESTRA"),
                        "ubigeo_paciente": row.get("UBIGEO_PACIENTE"),
                        "edad": row.get("EDAD", None),
                        "sexo": row.get("SEXO"),
                        "institucion": row.get("INSTITUCION"),
                        "departamento_paciente": row.get("DEPARTAMENTO_PACIENTE"),
                        "provincia_paciente": row.get("PROVINCIA_PACIENTE"),
                        "distrito_paciente": row.get("DISTRITO_PACIENTE"),
                        "departamento_muestra": row.get("DEPARTAMENTO_MUESTRA"),
                        "provincia_muestra": row.get("PROVINCIA_MUESTRA"),
                        "distrito_muestra": row.get("DISTRITO_MUESTRA"),
                        "fecha_resultado": row.get("FECHA_RESULTADO"),
                        "tipo_muestra": row.get("TIPO_MUESTRA"),
                        "batch_number": batch_num,
                        "fecha_procesamiento": datetime.now().isoformat()
                    }
                    positive_cases_list.append(case)
            else:
                print(f"Registro no es dict: {type(row)}")
        
        total_positive = len(positive_cases_list)
        total_registers = len(registers)
        
        print(f"Batch #{batch_num}: {total_positive} casos POSITIVOS de {total_registers} registros")
        
        # print(f"first element of registers: {registers[0]["RESULTADO"]}")
        
        
        # Retornar todos los casos positivos del batch como string
        return positive_cases_list
        
    except json.JSONDecodeError as e:
        print(f"Error parseando JSON: {e}")
        return []
    except Exception as e:
        print(f"Error inesperado: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return []


# save to mongo
def save_to_mongo(list_cases):
    """
    Guarda una lista de casos en MongoDB
    """
    # Debug: mostrar tipo y cantidad
    print(f"save_to_mongo recibe: tipo={type(list_cases)}, cantidad={len(list_cases) if isinstance(list_cases, list) else 'N/A'}")
    
    if not list_cases:
        print("No hay casos para guardar (None o vacío)")
        return "No hay casos para guardar"
    
    if not isinstance(list_cases, list):
        print(f"list_cases no es una lista: {type(list_cases)}")
        return f"Error: tipo incorrecto {type(list_cases)}"
    
    if len(list_cases) == 0:
        print("Lista vacía, no hay casos para guardar")
        return "Lista vacía"
    
    try:
        # Conectar a MongoDB
        client_mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client_mongo[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Insertar documentos
        if len(list_cases) == 1:
            result = collection.insert_one(list_cases[0])
            msg = f"Guardado 1 caso: {list_cases[0].get('uuid', 'N/A')}"
            print(msg)
            return msg
        else:
            result = collection.insert_many(list_cases)
            msg = f"Guardados {len(result.inserted_ids)} casos en MongoDB"
            print(msg)
            return msg
        
    except Exception as e:
        error_msg = f"Error guardando en MongoDB: {e}"
        print(error_msg)
        import traceback
        traceback.print_exc()
        return error_msg
    finally:
        if 'client_mongo' in locals():
            client_mongo.close()
        

# process batches and filter positive cases
stream_cases = ds.map(parse_and_filter)



# save list positive cases in MongoDB
results = stream_cases.map(save_to_mongo)

results.print()

try:
    env.execute("Cases Covid Processing")
except KeyboardInterrupt:
    print("\nProcesamiento detenido")
    sys.exit(0)
except Exception as e:
    print(f"\nERROR: {e}")
    sys.exit(1)