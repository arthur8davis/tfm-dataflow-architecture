from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime

import json
import polars as pl
import time

# ----------------------------------------
# Producer
# ----------------------------------------
def create_producer():
    # Create a Kafka client instance
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        max_request_size=10485760,
        compression_type='gzip'
    )
    
    return producer

def send_message():
    # send message to kafka topic
    producer = create_producer()
    
    topic = 'covid-cases'
    
    path_dataset = './datasets/dataset-casos.csv'
    
    reader_cases_file = pl.read_csv_batched(
        path_dataset, 
        batch_size=1,
        separator=';',
        # ignore_errors=True
        null_values=['NULL', 'null', 'NA', '']
        )
    
    batch_num = 0
    total_rows = 0
    start_time = time.time()
    
    while True:
        batches = reader_cases_file.next_batches(1)
        if batches is None or len(batches) == 0:
            break
        
        batch_num += 1
        df_batch = batches[0]
        rows_batch = df_batch.shape[0]
        total_rows += rows_batch
        
        print(f" batch {batch_num}: {len(df_batch)} filas")
        # print(f'registers: \n{df_batch}')
        
        registers = df_batch.write_json()
        # print(f'registers: \n{registers}')
        
        message = {
            'number_batch': batch_num,
            'rows_batch': rows_batch,
            'columns': df_batch.columns,
            'timestamp': datetime.now().isoformat(),
            'registers': registers
        }
        
        future = producer.send(topic, value=message)
        
        try:
            record_metadata = future.get(timeout=10)
            print(f"✓ Batch {batch_num} enviado:")
            print(f"  - Filas: {rows_batch}")
            print(f"  - Total acumulado: {total_rows}")
            print(f"  - Partition: {record_metadata.partition}")
            print(f"  - Offset: {record_metadata.offset}")
        except Exception as e:
            print(f"✗ Error enviando batch {batch_num}: {e}")
        
        if batch_num >= 1:
            break
        
    producer.flush()
    producer.close()
    
    total_time = time.time() - start_time
    
    print(f"\n{'='*60}")
    print(f"✅ Proceso completado")
    print(f"{'='*60}")
    print(f"Total batches enviados: {batch_num}")
    print(f"Total filas procesadas: {total_rows}")
    print(f"Tiempo total: {total_time:.2f} segundos")
    print(f"Velocidad: {total_rows/total_time:.0f} filas/segundo")
    
    
# ----------------------------------------
# CONSUMER
# ----------------------------------------
def consumer_batches(topic='cases'):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='group-csv-batches',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    
    total_rows = 0
    batch_received = 0
    
    try:
        for message in consumer:
            data = message.value
            batch_received += 1
            
            registers = data['registers']
            total_rows += len(registers)
            
            if registers and len(registers) > 0:
                print(f' values:{registers[0]}')
    except KeyboardInterrupt:
        print(f'Consumer stopped.')
    finally:
        consumer.close()


if __name__ == '__main__':
    send_message()
    # consumer_batches()
    