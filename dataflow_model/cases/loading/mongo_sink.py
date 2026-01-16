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

    def process(self, element):
        self.buffer.append(element)
        if len(self.buffer) >= self.batch_size:
            self._flush()

    def finish_bundle(self):
        if self.buffer:
            logging.info(f"Finishing bundle. Flushing {len(self.buffer)} items.")
            self._flush()

    def _flush(self):
        if not self.buffer:
            return
        logging.info(f"Flushing batch of {len(self.buffer)} to Mongo...")
        if not self.client:
            from pymongo import MongoClient
            self.client = MongoClient(self.uri)
            self.collection = self.client[self.db][self.coll_name]
        try:
            res = self.collection.insert_many(self.buffer)
            logging.info(f"Insertados {len(res.inserted_ids)} documentos en Mongo.")
            
        except Exception as e:
            logging.error(f"Error escribiendo batch a Mongo: {e}")
        finally:
            self.buffer = []

    def teardown(self):
        # Force flush of any remaining items
        if self.buffer:
            logging.info(f"Teardown: Flushing {len(self.buffer)} remaining items.")
            self._flush()

        if self.client:
            self.client.close()

class MongoWriteBatchFn(beam.DoFn):
    """
    DoFn optimizado para escribir lotes pre-agrupados (e.g. por GroupIntoBatches).
    No mantiene buffer interno.
    """
    def __init__(self, uri, db, coll):
        self.uri = uri
        self.db = db
        self.coll_name = coll
        self.client = None
        self.collection = None

    def start_bundle(self):
        from pymongo import MongoClient
        if not self.client:
            self.client = MongoClient(self.uri)
            self.collection = self.client[self.db][self.coll_name]

    def process(self, batch):
        # batch es una LISTA de documentos (o valores de un GroupBy)
        if not batch:
            return
        
        try:
            res = self.collection.insert_many(batch)
            logging.info(f"[Native Batch] Insertados {len(res.inserted_ids)} documentos.")
        except Exception as e:
            logging.error(f"Error escribiendo batch nativo a Mongo: {e}")

    def teardown(self):
        if self.client:
            self.client.close()
