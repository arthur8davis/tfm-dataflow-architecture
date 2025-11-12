# add documentation for TFM
-- kafka
docker run -d --name kafka-local -p 9092:9092 apache/kafka:latest

-- postgres
docker run --name local-postgres -p 5432:5432 -e POSTGRES_PASSWORD=admin123 -e POSTGRES_USER=userdb -e POSTGRES_DB=tfm -d postgres:15-alpine

-- mongodb
docker run -d \
  --name mongodb \
  -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=admin123 \
  mongo:8.0.0