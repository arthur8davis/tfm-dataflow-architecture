import socket

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'cases'
KAFKA_GROUP_ID_PREFIX = 'beam-reader'

# Mongo Configuration
MONGO_URI = 'mongodb://admin:admin123@localhost:27017'
MONGO_DB = 'covid-db'
MONGO_COLL = 'cases'
BATCH_SIZE = 10000

# Producer Configuration
CLIENT_ID = socket.gethostname()
