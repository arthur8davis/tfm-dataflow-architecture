import json
import logging
import polars as pl
import time

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, StringSerializer
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

script_dir = Path(__file__).parent

stringSerializer = StringSerializer('utf_8')

class KafkaParquetProcessor:
    """Procesador optimizado de Parquet a Kafka"""
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        compression_type: str = 'snappy',
        acks: str = 'all'
    ):
        """Inicializa el procesador con configuración optimizada"""
        
        # Configuración corregida para confluent-kafka
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
        
        # Estadisticas
        self.send_oks = 0
        self.send_errors = 0
        self.detail_errors = []
                
    def _callback(self, err, msg):
        """Callback para procesar confirmaciones"""
        if err:
            self.send_errors += 1
            error_info = {
                'error': str(err),
                'timestamp': datetime.now().isoformat()
            }
            self.detail_errors.append(error_info)
            print(f'Error to send message: {err}')
        else:
            self.send_oks += 1

    def process(
        self,
        filename_parquet: str,
        kafka_topic: str,
        batch_size: int = 5000,
        filters: Optional[List] = None,
        columns: Optional[List[str]] = None,
        key_column: Optional[str] = 'uuid'
    ) -> Dict[str, Any]:
        """Procesa archivo Parquet y envia a Kafka

        Args:
            filename_parquet (str): Ruta al archivo Parquet
            kafka_topic (str): Nombre del topic de Kafka
            batch_size (int, optional): Tamaño del batch para lectura (registros). Defaults to 5000.
            filters (Optional[List], optional): Lista de filtros Polars a aplicar. Defaults to None.
            columns (Optional[List[str]], optional): Lista de columnas a seleccionar (None = todas). Defaults to None.
            key_column (Optional[str], optional): Columna a usar como key en Kafka. Defaults to 'id'.

        Returns:
            Dict[str, Any]: Diccionario con estaditicas del proceso
        """
        start_time = datetime.now()
        self.send_oks = 0
        self.send_errors = 0
        self.detail_errors = []
        
        # Create Producer
        try:
            self.producer = Producer(self.config)
            print(f"Producer Kafka starting: {self.config['bootstrap.servers']}")
        except Exception as e:
            print(f'Error create producer: {e}')
            raise
        
        try:
            # build query lazy
            query = pl.scan_parquet(filename_parquet)
            
            # applied select columns
            if columns:
                query = query.select(columns)
                
            # applied filters
            if filters:
                for filter in filters:
                    query = query.filter(filter)
                    
            # get total registers
            total = query.select(pl.len()).collect().item()
            print(f'Total registers to processing: {total:,}')
            
            if total == 0:
                print(f"Don't have registers after applied filters")
                return self._generate_report(start_time, 0)
            
            # Processing for batches
            for offset in range(0, total, batch_size):
                start_batch = time.time()
                
                try:
                    # read batch
                    batch = query.slice(offset, batch_size).collect()
                    # num_registers_batch = len(batch)
                    
                    # send message to batch
                    for row in batch.iter_rows(named=True):
                        self._send_message(
                            topic=kafka_topic,
                            row=row,
                            key_column=key_column
                        )
                    
                    if offset >= 1:
                        break
                    # Poll for process callbacks (don't block)
                    self.producer.poll(0)
                    
                    # # calculate metrics batch
                    # duration_batch = time.time() - start_batch
                    # throughput_batch = num_registers_batch / duration_batch if duration_batch > 0 else 0
                    
                    # # log process
                    # progress = min(offset + batch_size, total)
                    # percent = (progress / total) * 100

                    # print(
                    #     f"Progress: {percent:.1f}% ({progress:,}/{total:,}) | "
                    #     f"Batch: {throughput_batch:.0f} msg/s | "
                    #     f"Confirmed: {self.send_oks:,}"
                    # )
                    
                except Exception as e:
                    print(f'Error processing batch en offset {offset}: {e}')
            
            # Flush final to ensure all messages are sent
            print(f'Waiting confirmed final of all messages')
            start_flush = time.time()
            pending = self.producer.flush(timeout=60)
            duration_flush = time.time() - start_flush
            
            if pending > 0:
                print(f'Remained {pending} messages unconfirmed')
            else:
                print(f'All messages confirmed (flush: {duration_flush:.2f}s)')
            
            return self._generate_report(start_time, total)
        
        except Exception as e:
            print(f'Error fatal during process: {e}')
            raise
        
        finally:
            if self.producer:
                self.producer.flush(timeout=10)
    
    
    def _send_message(self, topic: str, row: Dict, key_column: Optional[str]):
        """Send a message to kafka"""
        try:
            # get key
            key = None
            if key_column and key_column in row:
                key = str(row[key_column]).encode('utf-8')
                
            # serialize value
            value = json.dumps(row, default=str).encode('utf-8')
            
            # send with callback
            self.producer.produce(
                topic,
                key=key,
                value=value,
                callback=self._callback
            )
                
        except BufferError:
            # buffer full, do flush and retry
            print(f'Buffer full, doing flush')
            self.producer.flush()
            self.producer.produce(
                topic,
                key=key,
                value=value,
                callback=self._callback
            )
            
        except Exception as e:
            print(f'Error send a message: {e}')
            self.send_errors += 1
            
        
    def _generate_report(self, start_time: datetime, total: int) -> Dict[str, Any]:
        """Generate final report of process"""
        duration = (datetime.now() - start_time).total_seconds()
        throughput = self.send_oks / duration if duration > 0 else 0
        success_rate = (self.send_oks / total * 100) if total > 0 else 0
        
        print(f"\n{'='*60}")
        print("RESUME OF PROCESS")
        print(f"{'='*60}")
        print(f"Total registers: {total:,}")
        print(f"Success sent: {self.send_oks:,}")
        print(f"Errors: {self.send_errors:,}")
        print(f"Success rate: {success_rate:.2f}%")
        print(f"Time total: {duration:.2f}s")
        print(f"Throughput average: {throughput:.0f} msg/s")
        print(f"{'='*60}")
        
        # show errors if exists
        if self.detail_errors:
            print(f'\nHave {len(self.detail_errors)} errors')
            for i, error in enumerate(self.detail_errors[:5], 1):
                print(f"    {i}. {error['error']}")

        return {
            'total': total,
            'sents': self.send_oks,
            'errors': self.send_errors,
            'success_rate': success_rate,
            'duration': duration,
            'throughput': throughput,
        }


if __name__ == '__main__':
    processor = KafkaParquetProcessor(
        bootstrap_servers='localhost:9092',
        compression_type='snappy',
        acks='all'
    )
    
    result = processor.process(
        filename_parquet=script_dir/'../datasets/covid_cases.parquet',
        kafka_topic='cases',
        batch_size=10000,
        filters=None,
        key_column='uuid'
    )
    
    print(f'\nResult: {result}')