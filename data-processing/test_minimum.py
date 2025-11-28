"""
Pipeline MÍNIMO para verificar que Beam puede leer de Kafka
Si este funciona, el problema está en otra parte del pipeline complejo
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.kafka import ReadFromKafka
import json
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class SimplePrint(beam.DoFn):
    """DoFn simple que solo imprime"""
    
    def __init__(self):
        self.count = 0
    
    def setup(self):
        """Se ejecuta al iniciar el worker"""
        print("\n" + "="*60)
        print("🚀 WORKER INICIADO - SimplePrint setup()")
        print("="*60 + "\n")
    
    def process(self, element):
        self.count += 1
        key, value = element
        
        try:
            data = json.loads(value.decode('utf-8'))
            print(f"\n{'='*60}")
            print(f"🎉 MENSAJE #{self.count} RECIBIDO DE KAFKA")
            print(f"{'='*60}")
            print(f"UUID: {data.get('uuid', 'N/A')}")
            print(f"Departamento: {data.get('departamento_paciente', 'N/A')}")
            print(f"Resultado: {data.get('resultado', 'N/A')}")
            print(f"{'='*60}\n")
            yield data
        except Exception as e:
            print(f"❌ Error procesando mensaje: {e}")
            import traceback
            traceback.print_exc()


def run_minimal_pipeline():
    """Pipeline minimalista solo para leer e imprimir"""
    
    print("""
╔══════════════════════════════════════════════════════════════╗
║              PIPELINE MÍNIMO - TEST KAFKA                    ║
╚══════════════════════════════════════════════════════════════╝

Este es un pipeline SUPER SIMPLE que solo:
1. Lee de Kafka
2. Imprime los mensajes
3. No hace nada más (sin MongoDB, sin filtros, sin agregaciones)

Si esto NO funciona → problema con Kafka/Beam
Si esto SÍ funciona → problema en el pipeline complejo

⏳ Iniciando...
""")
    
    # Opciones con más verbosidad
    options = PipelineOptions([
        '--runner=DirectRunner',
        '--streaming',
    ])
    
    # Habilitar logging detallado
    options.view_as(StandardOptions).streaming = True
    
    try:
        print("🔧 Creando pipeline...")
        
        with beam.Pipeline(options=options) as pipeline:
            
            print("🔌 Configurando ReadFromKafka...")
            
            # Configuración simple y explícita
            kafka_config = {
                'bootstrap.servers': 'localhost:9092',
                'group.id': 'beam-minimal-test',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': 'false',
            }
            
            print(f"   Kafka Config: {kafka_config}")
            print(f"   Topic: cases")
            
            messages = (
                pipeline
                | 'Read from Kafka' >> ReadFromKafka(
                    consumer_config=kafka_config,
                    topics=['cases'],
                    with_metadata=False,
                    expansion_service=None  # Usar implementación local
                )
            )
            
            print("✅ ReadFromKafka configurado")
            
            # Procesamiento simple
            result = messages | 'Print Messages' >> beam.ParDo(SimplePrint())
            
            print("✅ Pipeline configurado completamente")
            print("\n" + "="*60)
            print("⏳ INICIANDO PIPELINE - ESPERANDO MENSAJES...")
            print("="*60)
            print("\n💡 Si no ves mensajes:")
            print("   1. Verifica: python kafka_consumer_test.py")
            print("   2. Envía datos: python kafka_test_producer.py")
            print("\n🛑 Presiona Ctrl+C para detener\n")
        
        # El pipeline termina aquí (cuando sale del 'with')
        print("\n⚠️ Pipeline terminó (esto no debería pasar en streaming)")
        
    except KeyboardInterrupt:
        print("\n\n⏹️ Pipeline detenido por usuario")
        
    except Exception as e:
        print(f"\n❌ ERROR EN PIPELINE:")
        print(f"{type(e).__name__}: {e}")
        print("\n📋 Traceback completo:")
        import traceback
        traceback.print_exc()
        
        print(f"\n🔧 PASOS DE DEBUG:")
        print(f"1. Verificar instalación: python beam_debug_versions.py")
        print(f"2. Test consumer puro: python kafka_consumer_test.py")
        print(f"3. Ver logs de Kafka: docker logs <kafka_container>")
        print(f"4. Reinstalar: pip install --force-reinstall apache-beam[kafka]")


if __name__ == '__main__':
    # Mostrar versión de Beam
    try:
        import apache_beam
        print(f"\n📦 Apache Beam Version: {apache_beam.__version__}\n")
    except:
        pass
    
    run_minimal_pipeline()