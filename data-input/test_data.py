import polars as pl
from confluent_kafka import Producer, KafkaException
import json
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaParquetProcessor:
    """Procesador optimizado de Parquet a Kafka"""
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        compression_type: str = 'snappy',
        acks: str = 'all'
    ):
        """Inicializa el procesador con configuración optimizada"""
        
        # ✅ Configuración corregida para confluent-kafka
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'compression.type': compression_type,
            'acks': acks,
            
            # Buffer y batching (en KB y número de mensajes)
            'queue.buffering.max.kbytes': 65536,  # 64 MB
            'queue.buffering.max.messages': 100000,
            'batch.num.messages': 10000,
            'linger.ms': 50,
            
            # Timeouts (en ms)
            'delivery.timeout.ms': 120000,  # 2 minutos total
            'request.timeout.ms': 30000,    # 30 segundos por request
            'socket.timeout.ms': 60000,     # 1 minuto socket
            
            # Reliability
            'message.send.max.retries': 3,
            'retry.backoff.ms': 100,
            
            # Performance
            'max.in.flight.requests.per.connection': 5,
            'socket.keepalive.enable': True,
        }
        
        self.producer = None
        self.enviados_ok = 0
        self.enviados_error = 0
        self.errores_detalle = []
        
        logger.info("Configuración del Producer:")
        for key, value in self.config.items():
            logger.info(f"  {key}: {value}")
    
    def _callback(self, err, msg):
        """Callback para procesar confirmaciones"""
        if err:
            self.enviados_error += 1
            error_info = {
                'error': str(err),
                'timestamp': datetime.now().isoformat()
            }
            self.errores_detalle.append(error_info)
            logger.error(f"Error al enviar mensaje: {err}")
        else:
            self.enviados_ok += 1
            if self.enviados_ok % 1000 == 0:
                logger.debug(f"Confirmados: {self.enviados_ok:,}")
    
    def process(
        self,
        archivo_parquet: str,
        kafka_topic: str,
        batch_size: int = 5000,
        filtros: Optional[List] = None,
        columnas: Optional[List[str]] = None,
        key_column: Optional[str] = 'id'
    ) -> Dict[str, Any]:
        """Procesa archivo Parquet y envía a Kafka"""
        
        inicio = datetime.now()
        self.enviados_ok = 0
        self.enviados_error = 0
        self.errores_detalle = []
        
        # Crear productor
        try:
            self.producer = Producer(self.config)
            logger.info(f"Productor Kafka iniciado: {self.config['bootstrap.servers']}")
        except Exception as e:
            logger.error(f"Error al crear productor: {e}")
            raise
        
        try:
            # Construir query
            query = pl.scan_parquet(archivo_parquet)
            
            if columnas:
                query = query.select(columnas)
            
            if filtros:
                for filtro in filtros:
                    query = query.filter(filtro)
            
            total = query.select(pl.len()).collect().item()
            logger.info(f"Total de registros a procesar: {total:,}")
            
            if total == 0:
                logger.warning("No hay registros para procesar")
                return self._generar_reporte(inicio, 0)
            
            # Procesar por batches
            for offset in range(0, total, batch_size):
                inicio_batch = time.time()
                
                try:
                    batch = query.slice(offset, batch_size).collect()
                    num_registros = len(batch)
                    
                    for row in batch.iter_rows(named=True):
                        self._enviar_mensaje(kafka_topic, row, key_column)
                    
                    # Poll para procesar callbacks
                    self.producer.poll(0)
                    
                    # Métricas
                    duracion_batch = time.time() - inicio_batch
                    throughput_batch = num_registros / duracion_batch if duracion_batch > 0 else 0
                    progreso = min(offset + batch_size, total)
                    porcentaje = (progreso / total) * 100
                    
                    logger.info(
                        f"Progreso: {porcentaje:.1f}% ({progreso:,}/{total:,}) | "
                        f"Batch: {throughput_batch:.0f} msg/s | "
                        f"Confirmados: {self.enviados_ok:,}"
                    )
                    
                except Exception as e:
                    logger.error(f"Error en batch offset {offset}: {e}")
            
            # Flush final
            logger.info("⏳ Esperando confirmación final...")
            inicio_flush = time.time()
            pendientes = self.producer.flush(timeout=60)
            duracion_flush = time.time() - inicio_flush
            
            if pendientes > 0:
                logger.warning(f"⚠️ Quedaron {pendientes} mensajes sin confirmar")
            else:
                logger.info(f"✅ Flush completado ({duracion_flush:.2f}s)")
            
            return self._generar_reporte(inicio, total)
            
        except Exception as e:
            logger.error(f"Error fatal: {e}", exc_info=True)
            raise
        
        finally:
            if self.producer:
                self.producer.flush(timeout=10)
    
    def _enviar_mensaje(self, topic: str, row: Dict, key_column: Optional[str]):
        """Envía mensaje a Kafka"""
        try:
            key = None
            if key_column and key_column in row:
                key = str(row[key_column]).encode('utf-8')
            
            value = json.dumps(row, default=str).encode('utf-8')
            
            self.producer.produce(topic, key=key, value=value, callback=self._callback)
            
        except BufferError:
            logger.warning("Buffer lleno, haciendo flush...")
            self.producer.flush()
            self.producer.produce(topic, key=key, value=value, callback=self._callback)
        except Exception as e:
            logger.error(f"Error enviando: {e}")
            self.enviados_error += 1
    
    def _generar_reporte(self, inicio: datetime, total: int) -> Dict[str, Any]:
        """Genera reporte final"""
        duracion = (datetime.now() - inicio).total_seconds()
        throughput = self.enviados_ok / duracion if duracion > 0 else 0
        tasa_exito = (self.enviados_ok / total * 100) if total > 0 else 0
        
        logger.info(f"\n{'='*60}")
        logger.info("RESUMEN")
        logger.info(f"{'='*60}")
        logger.info(f"Total: {total:,}")
        logger.info(f"Enviados: {self.enviados_ok:,}")
        logger.info(f"Errores: {self.enviados_error:,}")
        logger.info(f"Éxito: {tasa_exito:.2f}%")
        logger.info(f"Tiempo: {duracion:.2f}s")
        logger.info(f"Throughput: {throughput:.0f} msg/s")
        logger.info(f"{'='*60}")
        
        if self.errores_detalle:
            logger.warning(f"\nErrores encontrados: {len(self.errores_detalle)}")
            for i, error in enumerate(self.errores_detalle[:5], 1):
                logger.warning(f"  {i}. {error['error']}")
        
        return {
            'total': total,
            'enviados': self.enviados_ok,
            'errores': self.enviados_error,
            'tasa_exito': tasa_exito,
            'duracion': duracion,
            'throughput': throughput
        }


if __name__ == '__main__':
    processor = KafkaParquetProcessor(
        bootstrap_servers='localhost:9092',
        compression_type='snappy',
        acks='all'
    )
    
    resultado = processor.process(
        archivo_parquet='ventas.parquet',
        kafka_topic='ventas-stream',
        batch_size=10000,
        filtros=[
            pl.col('monto') > 100,
            pl.col('fecha') >= '2024-01-01'
        ],
        key_column='id'
    )
    
    print(f"\n✅ Resultado: {resultado}")