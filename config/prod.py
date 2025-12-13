# Production Configuration Placeholder

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'kafka-prod:9092'
KAFKA_TOPIC = 'cases'
KAFKA_GROUP_ID_PREFIX = 'beam-reader-prod'

# Mongo Configuration
MONGO_URI = 'mongodb://user:pass@mongo-prod:27017'
MONGO_DB = 'covid-prod-db'
MONGO_COLL = 'cases'
BATCH_SIZE = 1000

# Producer Configuration
CLIENT_ID = 'prod-client'
