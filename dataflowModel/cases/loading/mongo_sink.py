import logging
import random
import apache_beam as beam

class MongoWriteFn(beam.DoFn):
    def __init__(self, uri, db, coll, batch_size=100):
        self.uri = uri
        self.db = db
        self.coll_name = coll
        self.batch_size = batch_size
        self.client = None
        self.collection = None
        self.buffer = []

    def start_bundle(self):
        from pymongo import MongoClient
        if not self.client:
            self.client = MongoClient(self.uri)
            self.collection = self.client[self.db][self.coll_name]
        self.buffer = []

    def process(self, element):
        self.buffer.append(element)
        if len(self.buffer) >= self.batch_size:
            self._flush()

    def finish_bundle(self):
        if self.buffer:
            self._flush()

    def _flush(self):
        if not self.buffer:
            return
        try:
            res = self.collection.insert_many(self.buffer)
            logging.info(f"Insertados {len(res.inserted_ids)} documentos en Mongo.")
            
        except Exception as e:
            logging.error(f"Error escribiendo batch a Mongo: {e}")
        finally:
            self.buffer = []

    def teardown(self):
        if self.client:
            self.client.close()
