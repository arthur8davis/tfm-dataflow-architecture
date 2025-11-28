"""
Script para enviar mensajes de prueba a Kafka
Ejecuta esto MIENTRAS el pipeline de Beam está corriendo
"""

from kafka import KafkaProducer
import json
import time
from datetime import datetime
import sys

# Configuración
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'cases'

print(f"""
╔══════════════════════════════════════════════════════════════╗
║             KAFKA TEST PRODUCER - Enviando Mensajes          ║
╚══════════════════════════════════════════════════════════════╝

Kafka Broker: {KAFKA_BROKER}
Topic: {TOPIC}

Opciones:
1. Enviar 10 mensajes de prueba
2. Modo continuo (1 mensaje/segundo)

""")

# Crear producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("✅ Producer conectado a Kafka\n")
except Exception as e:
    print(f"❌ Error conectando a Kafka: {e}")
    exit(1)

# Función para generar mensaje
def generate_message(index):
    return {
        "uuid": f"TEST_{int(time.time())}_{index}",
        "fecha_muestra": datetime.now().strftime('%Y%m%d'),
        "edad": 30 + (index % 50),
        "sexo": "FEMENINO" if index % 2 == 0 else "MASCULINO",
        "institucion": "GOBIERNO REGIONAL",
        "ubigeo_paciente": "190601",
        "departamento_paciente": ["PIURA", "LIMA", "CUSCO", "AREQUIPA"][index % 4],
        "provincia_paciente": "SULLANA",
        "distrito_paciente": "SULLANA",
        "departamento_muestra": "PIURA",
        "provincia_muestra": "SULLANA",
        "distrito_muestra": "SULLANA",
        "tipo_muestra": "HISOPADO NASAL Y FARINGEO",
        "resultado": "NEGATIVO" if index % 3 == 0 else "POSITIVO"
    }

# Opción 1: 10 mensajes
if len(sys.argv) == 1 or sys.argv[1] == '1':
    print("📤 Enviando 10 mensajes de prueba...\n")
    
    for i in range(10):
        case = generate_message(i)
        
        future = producer.send(TOPIC, value=case)
        result = future.get(timeout=10)
        
        print(f"✅ [{i+1}/10] Enviado:")
        print(f"    UUID: {case['uuid']}")
        print(f"    Dept: {case['departamento_paciente']}")
        print(f"    Resultado: {case['resultado']}")
        print(f"    Partition: {result.partition}, Offset: {result.offset}")
        print()
        
        time.sleep(1)
    
    print("🎉 ¡10 mensajes enviados exitosamente!")
    
# Opción 2: Modo continuo
elif sys.argv[1] == '2':
    print("🔄 MODO CONTINUO activado")
    print("   Enviando 1 mensaje por segundo")
    print("   Presiona Ctrl+C para detener\n")
    
    counter = 0
    try:
        while True:
            case = generate_message(counter)
            
            future = producer.send(TOPIC, value=case)
            result = future.get(timeout=10)
            
            print(f"✅ [{counter+1}] {case['departamento_paciente']:10} | "
                  f"{case['resultado']:8} | "
                  f"UUID: {case['uuid']}")
            
            counter += 1
            time.sleep(1)
            
    except KeyboardInterrupt:
        print(f"\n\n⏹️  Deteniendo producer...")
        print(f"📊 Total enviados: {counter} mensajes")

producer.close()

print("""
╔══════════════════════════════════════════════════════════════╗
║                         FINALIZADO                           ║
╚══════════════════════════════════════════════════════════════╝

💡 Si el pipeline de Beam está corriendo, deberías ver:
   - Mensajes siendo procesados
   - Inserción a MongoDB
   - Estadísticas cada 60 segundos

🔍 Si no ves nada en el pipeline:
   1. Verifica que el pipeline esté corriendo
   2. Revisa los logs del pipeline
   3. Ejecuta: python beam_kafka_minimal.py
""")