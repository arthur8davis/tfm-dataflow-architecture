import json
import logging
import polars as pl

from confluent_kafka import Producer
from datetime import datetime


def process_parquet_to_kafka(
    filename_parquet: str,
    kafka_topic: str,
    bootstrap_servers:str = 'localhost:9092',
    batch_size:int = 5000,
    filters:pl =None
):
    """
    Process file parquet and send to kafka

    Args:
        filename_parquet (str): _description_
        kafka_topic (str): _description_
        bootstrap_servers (_type_, optional): _description_. Defaults to 'localhost:9092'.
        batch_size (int, optional): _description_. Defaults to 5000.
        filters (pl, optional): _description_. Defaults to None.
    """
    start_time = datetime.now()
    
    producer = Producer({
        'bootstrap.server': bootstrap_servers,
        'compression.type': 'snappy',
        'batch.size': 327
    })