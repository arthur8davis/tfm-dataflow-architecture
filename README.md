# Pipeline de Procesamiento de Datos COVID-19 en Tiempo Real

## Tabla de Contenidos

1. [Introduccion](#introduccion)
2. [Arquitectura del Sistema](#arquitectura-del-sistema)
3. [Componentes Principales](#componentes-principales)
4. [Flujo de Datos Detallado](#flujo-de-datos-detallado)
5. [Instalacion y Configuracion](#instalacion-y-configuracion)
6. [Guia de Ejecucion](#guia-de-ejecucion)
7. [Schemas Disponibles](#schemas-disponibles)
8. [Configuracion Avanzada](#configuracion-avanzada)
9. [Monitoreo y Observabilidad](#monitoreo-y-observabilidad)
10. [Visualizacion en Tiempo Real](#visualizacion-en-tiempo-real)
11. [Casos de Uso y Ejemplos](#casos-de-uso-y-ejemplos)
12. [Troubleshooting](#troubleshooting)
13. [Desarrollo y Extension](#desarrollo-y-extension)

---

## Introduccion

Este proyecto implementa una **arquitectura de procesamiento de datos en tiempo real** para datos de COVID-19 utilizando:

- **Apache Beam**: Framework de procesamiento distribuido
- **Apache Kafka (KRaft)**: Sistema de mensajeria para ingesta de datos (sin Zookeeper)
- **MongoDB**: Base de datos con colecciones time-series
- **Polars**: Procesamiento eficiente de archivos CSV/Parquet
- **Flask + Socket.IO**: Dashboard de visualizacion en tiempo real con D3.js y Leaflet

### Caracteristicas Principales

- **Multi-Schema**: 3 pipelines independientes (cases, demises, hospitalizations)
- **Tiempo Real**: Procesamiento streaming con ventanas temporales
- **Escalable**: Arquitectura horizontal con Apache Beam
- **Resiliente**: Dead Letter Queue (DLQ) para manejo de errores
- **Configurable**: Configuracion independiente por schema
- **Paralelo**: Ejecucion simultanea de multiples schemas
- **Enriquecimiento Geografico**: Coordenadas lat/lon desde codigos UBIGEO
- **Dashboard en Tiempo Real**: Visualizaciones D3.js + mapas de calor Leaflet

---

## Arquitectura del Sistema

### Vista General de la Arquitectura

```mermaid
graph TB
    subgraph "ARQUITECTURA MULTI-SCHEMA"
        subgraph "SCHEMA: CASES - Datos de Pacientes Individuales"
            CSV1[CSV Files<br/>13 archivos] --> Kafka1[Kafka Topic<br/>cases]
            Kafka1 --> Beam1[Apache Beam Pipeline<br/>Transforms + Validate + Enrich Geo]
            Beam1 --> Mongo1[MongoDB Collection<br/>cases Time-Series]
        end

        subgraph "SCHEMA: DEMISES - Datos de Fallecimientos"
            CSV2[CSV Files<br/>13 archivos] --> Kafka2[Kafka Topic<br/>demises]
            Kafka2 --> Beam2[Apache Beam Pipeline<br/>Transforms + Validate + Enrich Geo]
            Beam2 --> Mongo2[MongoDB Collection<br/>demises Time-Series]
        end

        subgraph "SCHEMA: HOSPITALIZATIONS - Datos de Hospitalizaciones"
            CSV3[CSV Files<br/>13 archivos] --> Kafka3[Kafka Topic<br/>hospitalizations]
            Kafka3 --> Beam3[Apache Beam Pipeline<br/>Transforms + Validate + Enrich Geo]
            Beam3 --> Mongo3[MongoDB Collection<br/>hospitalizations Time-Series]
        end

        subgraph "COMPONENTES COMPARTIDOS - Docker Compose"
            KafkaService[Kafka KRaft<br/>Port: 9092]
            MongoService[MongoDB 8.0<br/>Port: 27017]
            KafkaUI[Kafka UI<br/>Port: 8080]
            MongoExpress[Mongo Express<br/>Port: 8083]
        end

        subgraph "VISUALIZACION"
            Dashboard[Flask + Socket.IO<br/>D3.js + Leaflet<br/>Port: 5006]
        end
    end

    style CSV1 fill:#e1f5ff
    style CSV2 fill:#e1f5ff
    style CSV3 fill:#e1f5ff
    style Kafka1 fill:#fff4e6
    style Kafka2 fill:#fff4e6
    style Kafka3 fill:#fff4e6
    style Beam1 fill:#f3e5f5
    style Beam2 fill:#f3e5f5
    style Beam3 fill:#f3e5f5
    style Mongo1 fill:#e8f5e9
    style Mongo2 fill:#e8f5e9
    style Mongo3 fill:#e8f5e9
    style KafkaService fill:#fff9c4
    style MongoService fill:#fff9c4
    style KafkaUI fill:#f1f8e9
    style MongoExpress fill:#f1f8e9
    style Dashboard fill:#e0f7fa
```

### Arquitectura de Pipeline Individual (Apache Beam)

```mermaid
graph TD
    Start[Kafka Source<br/>Topic: cases/demises/hospitalizations] --> Parse[1. Parse Message<br/>JSON - Dict]

    Parse --> Normalize[2. Normalize<br/>Handle nulls<br/>Type conversions]
    Parse --> |Parse Error| DLQ1[DLQ: Parse Errors]

    Normalize --> EnrichGeo[3. Enrich Geo<br/>UBIGEO - lat/lon<br/>Coordenadas geograficas]

    EnrichGeo --> Validate[4. Validate Schema<br/>Check required fields<br/>Validate types]
    Normalize --> |Normalization Error| DLQ2[DLQ: Normalization Errors]

    Validate --> Timestamp[5. Assign Timestamp<br/>Parse campo fecha<br/>Add timestamp field]
    Validate --> |Validation Error| DLQ3[DLQ: Validation Errors]

    Timestamp --> Window[6. Windowing<br/>Fixed Window 60s]
    Timestamp --> |Timestamp Error| DLQ4[DLQ: Timestamp Errors]

    Window --> Metadata[7. Add Metadata<br/>pipeline_version<br/>source_type<br/>window info]

    Metadata --> Batch[8. Batching<br/>Native/Manual<br/>Group N items]

    Batch --> MongoSink[9. MongoDB Sink<br/>Bulk Write<br/>Time-Series Collection]

    MongoSink --> Success[MongoDB<br/>Collection: schema]
    MongoSink --> |Write Error| DLQ5[DLQ: Sink Errors]

    DLQ1 --> DLQCollection[(DLQ Collection<br/>dead_letter_queue)]
    DLQ2 --> DLQCollection
    DLQ3 --> DLQCollection
    DLQ4 --> DLQCollection
    DLQ5 --> DLQCollection

    Success --> Logs[Console Logs<br/>& Metrics]

    style Start fill:#e3f2fd
    style Parse fill:#fff3e0
    style Normalize fill:#f3e5f5
    style EnrichGeo fill:#e0f2f1
    style Validate fill:#e8f5e9
    style Timestamp fill:#fff9c4
    style Window fill:#fce4ec
    style Metadata fill:#e0f2f1
    style Batch fill:#f1f8e9
    style MongoSink fill:#e8eaf6
    style Success fill:#c8e6c9
    style DLQCollection fill:#ffcdd2
```

### Estructura de Directorios

```mermaid
graph LR
    Root[tfm-dataflow-architecture/] --> Pipelines[pipelines/]
    Root --> Src[src/]
    Root --> Datasets[datasets/]
    Root --> Viz[visualization/]
    Root --> Config[Config Files]

    Pipelines --> Cases[cases/]
    Pipelines --> Demises[demises/]
    Pipelines --> Hospitals[hospitalizations/]

    Cases --> C_Config[config.yaml]
    Cases --> C_Schema[cases.json]
    Cases --> C_Pipeline[pipeline.py]
    Cases --> C_Ingestion[ingestion.py]

    Demises --> D_Config[config.yaml]
    Demises --> D_Schema[demises.json]
    Demises --> D_Pipeline[pipeline.py]
    Demises --> D_Ingestion[ingestion.py]

    Hospitals --> H_Config[config.yaml]
    Hospitals --> H_Schema[hospitalizations.json]
    Hospitals --> H_Pipeline[pipeline.py]
    Hospitals --> H_Ingestion[ingestion.py]

    Src --> Common[common/]
    Common --> Sources[sources/<br/>kafka_source_native.py<br/>storage_source.py]
    Common --> Transforms[transforms/<br/>normalize.py<br/>validate.py<br/>timestamp.py<br/>windowing.py<br/>metadata.py<br/>enrich_geo.py]
    Common --> Batching[batching/<br/>native_batch.py<br/>manual_batch.py]
    Common --> Sinks[sinks/<br/>mongo_sink.py<br/>dlq_sink.py]
    Common --> Utils[utils/<br/>config_loader.py<br/>schema_loader.py]
    Common --> Data[data/<br/>ubigeo_coords.py]

    Src --> Ingestion[ingestion/<br/>kafka_processor.py]

    Datasets --> DS_Cases[cases/<br/>13 archivos CSV]
    Datasets --> DS_Demises[demises/<br/>13 archivos CSV]
    Datasets --> DS_Hospitals[hospitalizations/<br/>13 archivos CSV]

    Viz --> VizApp[app.py<br/>Flask + Socket.IO]
    Viz --> VizStatic[static/<br/>D3.js + Leaflet]

    Config --> Orchestrator[orchestrator.py]
    Config --> RunCases[run_cases.sh]
    Config --> RunDeaths[run_deaths.sh]
    Config --> Docker[docker-compose.yaml]
    Config --> Reqs[requirements.txt]

    style Root fill:#e3f2fd
    style Pipelines fill:#fff3e0
    style Src fill:#f3e5f5
    style Datasets fill:#e8f5e9
    style Common fill:#fff9c4
    style Cases fill:#fce4ec
    style Demises fill:#fce4ec
    style Hospitals fill:#fce4ec
    style Viz fill:#e0f7fa
```

---

## Componentes Principales

### 1. Ingesta de Datos (Ingestion)

```mermaid
sequenceDiagram
    participant CSV as Archivos CSV<br/>file_0 a file_12
    participant KP as KafkaProcessor<br/>kafka_processor.py
    participant Polars as Polars Library
    participant Kafka as Kafka Topic

    CSV->>KP: Leer archivo
    KP->>Polars: Usar Polars para parsing
    Polars->>KP: DataFrame con datos

    loop Para cada fila
        KP->>KP: Convertir fila a dict
        KP->>KP: Agregar metadata<br/>(schema_name, timestamp)
        KP->>Kafka: Enviar mensaje JSON<br/>Key: uuid<br/>Value: record
        Kafka-->>KP: ACK
    end

    KP->>KP: Log: N mensajes enviados
```

**Componentes:**
- **KafkaProcessor** (`src/ingestion/kafka_processor.py`): Procesador comun para ingesta usando confluent_kafka
- **Ingestion Scripts** (`pipelines/{schema}/ingestion.py`): Scripts especificos por schema

### 2. Transformaciones del Pipeline

```mermaid
flowchart TD
    Input[Raw Data] --> T1[TRANSFORM 1: NORMALIZE]

    T1 --> |Nulls a None<br/>Type conversions| T2[TRANSFORM 2: ENRICH GEO]

    T2 --> |UBIGEO a lat/lon<br/>Coordenadas geograficas| T3[TRANSFORM 3: VALIDATE]

    T3 --> |Check required fields<br/>Validate types| T4[TRANSFORM 4: TIMESTAMP]

    T4 --> |Parse campo fecha<br/>Add timestamp field| T5[TRANSFORM 5: WINDOWING]

    T5 --> |Fixed Window 60s| T6[TRANSFORM 6: METADATA]

    T6 --> |Add pipeline_version<br/>source_type<br/>window info| T7[TRANSFORM 7: BATCHING]

    T7 --> |Native/Manual<br/>Group N elements| Output[Batched Data<br/>Ready for MongoDB]

    T3 -.->|Validation fails| DLQ_V[DLQ: Validation Error]
    T4 -.->|Parse fails| DLQ_T[DLQ: Timestamp Error]

    style T1 fill:#e3f2fd
    style T2 fill:#e0f2f1
    style T3 fill:#f3e5f5
    style T4 fill:#e8f5e9
    style T5 fill:#fff9c4
    style T6 fill:#fce4ec
    style T7 fill:#e0f2f1
    style Output fill:#c8e6c9
    style DLQ_V fill:#ffcdd2
    style DLQ_T fill:#ffcdd2
```

### 3. Sinks (Destinos)

#### MongoDB Sink

```mermaid
graph LR
    Batch[Batch de Records<br/>100 elementos] --> Prepare[Preparar<br/>Bulk Write]

    Prepare --> Execute[Ejecutar<br/>insert_many]

    Execute --> Check{Resultado?}

    Check -->|Success| Success[MongoDB<br/>Collection: schema<br/>N docs insertados]
    Check -->|Partial Error| Partial[Algunos docs<br/>escritos<br/>BulkWriteError]
    Check -->|Total Error| Error[Error completo<br/>Retry logic]

    Partial --> Log1[Log errores<br/>Continuar]
    Error --> Retry{Retry?}
    Retry -->|Yes| Execute
    Retry -->|No| DLQ[Send to DLQ]

    Success --> Metrics[Actualizar<br/>metricas]

    style Batch fill:#e3f2fd
    style Success fill:#c8e6c9
    style DLQ fill:#ffcdd2
```

#### Dead Letter Queue (DLQ)

```mermaid
graph TD
    Error1[Parse Error] --> DLQ[DLQ Sink<br/>dlq_sink.py]
    Error2[Validation Error] --> DLQ
    Error3[Timestamp Error] --> DLQ
    Error4[Sink Error] --> DLQ

    DLQ --> Create[Crear documento DLQ<br/>error + error_type<br/>+ timestamp<br/>+ schema<br/>+ record]

    Create --> Insert[Insertar en MongoDB]

    Insert --> Collection[(dead_letter_queue<br/>Collection)]

    Collection --> Indexes[Indexes:<br/>- error_type<br/>- schema<br/>- timestamp]

    style DLQ fill:#fff3e0
    style Collection fill:#ffcdd2
    style Indexes fill:#fff9c4
```

---

## Flujo de Datos Detallado

### Flujo Completo End-to-End

```mermaid
graph TB
    subgraph "PASO 1: PREPARACION"
        CSV[datasets/cases/<br/>file_0_cases.csv a<br/>file_12_cases.csv]
    end

    subgraph "PASO 2: INGESTA"
        CSV --> Ingestion[python pipelines/cases/<br/>ingestion.py]
        Ingestion --> |KafkaProcessor<br/>Lee CSV con Polars<br/>Convierte a JSON| Producer[Kafka Producer<br/>confluent_kafka]
        Producer --> |Envia mensajes| KafkaTopic[Kafka Topic: cases<br/>KRaft mode]
    end

    subgraph "PASO 3: PROCESAMIENTO"
        KafkaTopic --> Pipeline[python pipelines/cases/<br/>pipeline.py<br/>Apache Beam Pipeline]
        Pipeline --> T1[1. Parse JSON]
        T1 --> T2[2. Normalize]
        T2 --> T2b[3. Enrich Geo]
        T2b --> T3[4. Validate]
        T3 --> T4[5. Timestamp]
        T4 --> T5[6. Window 60s]
        T5 --> T6[7. Metadata]
        T6 --> T7[8. Batch 100]
    end

    subgraph "PASO 4: ALMACENAMIENTO"
        T7 --> |Datos validos| MongoWrite[MongoDB Sink<br/>Bulk Write]
        MongoWrite --> MongoDB[(MongoDB<br/>Collection: cases<br/>Time-Series)]

        T3 -.->|Errores| DLQWrite[DLQ Sink]
        T4 -.->|Errores| DLQWrite
        T7 -.->|Errores| DLQWrite
        DLQWrite --> DLQ[(MongoDB<br/>Collection: dead_letter_queue)]
    end

    subgraph "PASO 5: MONITOREO Y VISUALIZACION"
        MongoDB --> KafkaUI[Kafka UI<br/>http://localhost:8080]
        MongoDB --> MongoExpress[Mongo Express<br/>http://localhost:8083]
        MongoDB --> Dashboard[Dashboard D3.js<br/>http://localhost:5006]
        DLQ --> MongoExpress
    end

    style CSV fill:#e3f2fd
    style KafkaTopic fill:#fff3e0
    style Pipeline fill:#f3e5f5
    style MongoDB fill:#c8e6c9
    style DLQ fill:#ffcdd2
    style KafkaUI fill:#fff9c4
    style MongoExpress fill:#fff9c4
    style Dashboard fill:#e0f7fa
```

---

## Instalacion y Configuracion

### Requisitos Previos

```
Sistema Operativo:
  - Linux / macOS / Windows (con WSL2)

Software requerido:
  - Python 3.8 o superior
  - Docker y Docker Compose
  - Git (opcional)

Recursos minimos:
  - 4 GB RAM
  - 10 GB espacio en disco
  - Conexion a internet
```

### Paso 1: Clonar el Repositorio

```bash
git clone <repository-url>
cd tfm-dataflow-architecture
```

### Paso 2: Crear Entorno Virtual

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Paso 3: Instalar Dependencias

```bash
pip install -r requirements.txt
```

**Dependencias principales:**
- `apache-beam[gcp]==2.52.0`: Framework de procesamiento
- `confluent-kafka==2.3.0`: Cliente nativo de Kafka
- `pymongo==4.6.1`: Cliente de MongoDB
- `polars==0.20.3`: Procesamiento de datos
- `pyyaml==6.0.1`: Parsing de configuracion

### Paso 4: Iniciar Servicios con Docker

```bash
docker-compose up -d
docker-compose ps
```

**Servicios iniciados:**

| Servicio | Puerto | Imagen | Descripcion |
|----------|--------|--------|-------------|
| Kafka (KRaft) | 9092 | apache/kafka:4.1.1 | Message broker (sin Zookeeper) |
| MongoDB | 27017 | mongo:8.0.0 | Base de datos time-series |
| Kafka UI | 8080 | kafbat/kafka-ui | Interface web para Kafka |
| Mongo Express | 8083 | mongo-express | Interface web para MongoDB |

> **Nota**: Kafka usa modo KRaft (sin Zookeeper). No se necesita un servicio Zookeeper separado.

### Paso 5: Configurar MongoDB

```bash
docker exec -it mongodb mongosh -u admin -p admin123

use covid-db

# Crear coleccion time-series para cases
db.createCollection("cases", {
  timeseries: {
    timeField: "timestamp",
    metaField: "metadata",
    granularity: "hours"
  }
})

# Crear coleccion time-series para demises
db.createCollection("demises", {
  timeseries: {
    timeField: "timestamp",
    metaField: "metadata",
    granularity: "hours"
  }
})

# Crear coleccion time-series para hospitalizations
db.createCollection("hospitalizations", {
  timeseries: {
    timeField: "timestamp",
    metaField: "metadata",
    granularity: "hours"
  }
})

# Crear coleccion para DLQ
db.createCollection("dead_letter_queue")
db.dead_letter_queue.createIndex({"error_type": 1})
db.dead_letter_queue.createIndex({"schema": 1})
db.dead_letter_queue.createIndex({"timestamp": 1})

exit
```

---

## Guia de Ejecucion

### Opciones de Ejecucion

```mermaid
graph TD
    Start([Ejecutar Pipeline]) --> Choice{Metodo de<br/>ejecucion?}

    Choice -->|Opcion 1| Scripts[Scripts Shell<br/>run_cases.sh<br/>run_deaths.sh]
    Choice -->|Opcion 2| Direct[Ejecucion Directa<br/>python pipelines/X/...]
    Choice -->|Opcion 3| Orchestrator[Orquestador<br/>orchestrator.py]

    Scripts --> ScriptMode{Modo?}
    ScriptMode -->|ingest| S1[./run_cases.sh ingest]
    ScriptMode -->|pipeline| S2[./run_cases.sh pipeline]
    ScriptMode -->|both| S3[./run_cases.sh both]

    Direct --> D1[Terminal 1:<br/>python pipelines/cases/ingestion.py]
    Direct --> D2[Terminal 2:<br/>python pipelines/cases/pipeline.py --mode streaming]

    Orchestrator --> O1[Single schema:<br/>--ingest cases]
    Orchestrator --> O2[Multiple schemas:<br/>--pipeline cases demises hospitalizations --parallel]
    Orchestrator --> O3[All schemas:<br/>--pipeline-all --parallel]

    style Start fill:#e3f2fd
    style Scripts fill:#fff3e0
    style Direct fill:#f3e5f5
    style Orchestrator fill:#e8f5e9
```

### Opcion 1: Ejecucion con Scripts

```bash
chmod +x run_cases.sh run_deaths.sh

# CASES
./run_cases.sh ingest      # Solo ingesta
./run_cases.sh pipeline    # Solo pipeline
./run_cases.sh both        # Ingesta + pipeline

# DEMISES
./run_deaths.sh ingest
./run_deaths.sh pipeline
./run_deaths.sh both
```

### Opcion 2: Ejecucion Directa

```bash
# Schema CASES
python pipelines/cases/ingestion.py
python pipelines/cases/pipeline.py --mode streaming

# Schema DEMISES
python pipelines/demises/ingestion.py
python pipelines/demises/pipeline.py --mode streaming

# Schema HOSPITALIZATIONS
python pipelines/hospitalizations/ingestion.py
python pipelines/hospitalizations/pipeline.py --mode streaming

# Modo batch (desde archivos CSV directamente)
python pipelines/cases/pipeline.py --mode batch
```

### Opcion 3: Orquestador Multi-Schema (Recomendado)

```bash
# Listar schemas disponibles
python orchestrator.py --list

# Ejecutar ingesta de un schema
python orchestrator.py --ingest cases

# Ejecutar pipeline de un schema
python orchestrator.py --pipeline cases

# Ejecutar multiples pipelines en paralelo
python orchestrator.py --pipeline cases demises hospitalizations --parallel

# Ejecutar TODOS los pipelines en paralelo
python orchestrator.py --pipeline-all --parallel

# Ejecutar TODAS las ingests en paralelo
python orchestrator.py --ingest-all --parallel

# Ingestar archivo especifico
python orchestrator.py --ingest cases --file datasets/cases/file_0_cases.csv
```

---

## Schemas Disponibles

### Schema: CASES (Datos de Pacientes Individuales)

**Campos requeridos:** `fecha_muestra`, `edad`, `sexo`, `resultado`

**Campos opcionales:** `uuid`, `institucion`, `ubigeo_paciente`, `departamento_paciente`, `provincia_paciente`, `distrito_paciente`, `departamento_muestra`, `provincia_muestra`, `distrito_muestra`, `tipo_muestra`

**Configuracion:**
- Topic Kafka: `cases`
- Consumer group: `beam-pipeline-cases`
- Timestamp field: `fecha_muestra`
- Window: 60s
- Batch: native, 100

**Datasets:** 13 archivos CSV (`file_0_cases.csv` a `file_12_cases.csv`)

### Schema: DEMISES (Datos de Fallecimientos)

**Campos requeridos:** `fecha_fallecimiento`, `edad_declarada`, `sexo`, `clasificacion_def`

**Campos opcionales:** `uuid`, `departamento`, `provincia`, `distrito`, `ubigeo`

**Configuracion:**
- Topic Kafka: `demises`
- Consumer group: `beam-pipeline-demises`
- Timestamp field: `fecha_fallecimiento`
- Window: 60s
- Batch: native, 100

**Datasets:** 13 archivos CSV (`file_0_demises.csv` a `file_12_demises.csv`)

### Schema: HOSPITALIZATIONS (Datos de Hospitalizaciones)

**Campos requeridos:** `id_persona`, `sexo`, `fecha_ingreso_hosp`, `edad`

**Campos opcionales:** 50+ campos incluyendo datos de vacunacion, UCI, ventilacion mecanica, oxigeno, datos de establecimiento de salud

**Configuracion:**
- Topic Kafka: `hospitalizations`
- Consumer group: `beam-pipeline-hospitalizations`
- Timestamp field: `fecha_ingreso_hosp`
- Window: 60s
- Batch: native, 100

**Datasets:** 13 archivos CSV (`file_0_hospital.csv` a `file_12_hospital.csv`)

---

## Configuracion Avanzada

### Archivo config.yaml

Cada schema tiene su propio `config.yaml` en `pipelines/{schema}/config.yaml`:

```yaml
schema:
  name: "cases"
  version: "1.0.0"
  description: "Pipeline para casos de COVID-19"

source:
  type: "kafka"  # "kafka" o "storage"
  kafka:
    bootstrap_servers: "localhost:9092"
    topic: "cases"
    consumer_config:
      group.id: "beam-pipeline-cases"
      auto.offset.reset: "earliest"
      enable.auto.commit: "true"
  storage:
    file_pattern: "datasets/cases/*.csv"
    file_type: "csv"

transforms:
  normalize:
    enabled: true
  validate:
    enabled: true
    schema_file: "pipelines/cases/schema.json"
  timestamp:
    enabled: true
    field: "fecha_muestra"
  windowing:
    enabled: true
    window_size_seconds: 60
    allowed_lateness_seconds: 300
    trigger: "default"
  metadata:
    enabled: true
    pipeline_version: "1.0.0"

batching:
  strategy: "native"
  batch_size: 100
  batch_timeout_seconds: 30

sink:
  mongodb:
    connection_string: "mongodb://admin:admin123@localhost:27017"
    database: "covid-db"
    collection:
      name: "cases"
      timeseries:
        timeField: "timestamp"
        metaField: "metadata"
        granularity: "hours"
  dlq:
    collection: "dead_letter_queue"

pipeline:
  runner: "DirectRunner"
  streaming: true
```

---

## Monitoreo y Observabilidad

### Herramientas de Monitoreo

| Herramienta | URL | Uso |
|-------------|-----|-----|
| Kafka UI | http://localhost:8080 | Topics, mensajes, consumer lag |
| Mongo Express | http://localhost:8083 | Colecciones, queries, DLQ |
| Dashboard D3.js | http://localhost:5006 | Visualizacion en tiempo real |

### Consultas Utiles de MongoDB

```javascript
use("covid-db");

// Ver ultimos casos procesados
db.cases.find().sort({"timestamp": -1}).limit(10)

// Contar registros por coleccion
db.cases.countDocuments()
db.demises.countDocuments()
db.hospitalizations.countDocuments()

// Ver errores en DLQ
db.dead_letter_queue.find().sort({"timestamp": -1})

// Contar errores por schema
db.dead_letter_queue.aggregate([
  {$group: {_id: "$schema", count: {$sum: 1}}}
])

// Contar errores por tipo
db.dead_letter_queue.aggregate([
  {$group: {_id: "$error_type", count: {$sum: 1}}}
])
```

---

## Visualizacion en Tiempo Real

El proyecto incluye un **dashboard interactivo** en `visualization/` que se conecta a MongoDB y muestra visualizaciones actualizadas en tiempo real via WebSockets.

### Ejecucion

```bash
cd visualization
pip install flask flask-socketio flask-cors pymongo
python app.py
# Abrir http://localhost:5006
```

### Visualizaciones Incluidas

| Visualizacion | Tecnologia | Descripcion |
|---------------|------------|-------------|
| Casos por Departamento | D3.js (barras) | Top departamentos con casos positivos |
| Fallecidos por Departamento | D3.js (barras) | Top departamentos con fallecidos |
| Casos por Fecha | D3.js (area) | Serie temporal de casos confirmados |
| Fallecidos por Fecha | D3.js (area) | Serie temporal de fallecidos |
| Distribucion por Sexo | D3.js (donut) | Casos y fallecidos por genero |
| Casos por Edad | D3.js (barras) | Piramide poblacional por grupo etario |
| Mapa Casos | Leaflet + Heat | Mapa de calor geografico de casos |
| Mapa Fallecidos | Leaflet + Heat | Mapa de calor geografico de fallecidos |
| Mapa Hospitalizaciones | Leaflet + Heat | Mapa de calor de hospitalizaciones |

---

## Troubleshooting

### Problemas Comunes

#### No se puede conectar a Kafka

```bash
docker-compose ps kafka
docker-compose restart kafka
docker-compose logs kafka | tail -50
```

#### Pipeline no procesa mensajes

```bash
# Verificar mensajes en topic
docker exec kafka-kraft kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cases \
  --from-beginning \
  --max-messages 5
```

#### No se escriben datos en MongoDB

```bash
docker exec -it mongodb mongosh -u admin -p admin123
use covid-db
db.cases.countDocuments()
db.dead_letter_queue.find({schema: "cases"}).pretty()
```

#### Error de importacion de modulos

```bash
# Asegurate de estar en el directorio raiz del proyecto
pwd  # Debe ser tfm-dataflow-architecture/
ls src/common/
ls pipelines/cases/
```

---

## Desarrollo y Extension

### Agregar un Nuevo Schema

1. Crear directorios: `mkdir -p pipelines/mi_schema datasets/mi_schema`
2. Copiar plantilla: `cp pipelines/cases/* pipelines/mi_schema/`
3. Editar `config.yaml`: cambiar name, topic, group.id, collection
4. Editar `{schema}.json`: definir campos requeridos y tipos
5. Editar `pipeline.py`: cambiar nombre de clase
6. Editar `ingestion.py`: cambiar nombre de clase
7. Agregar datos CSV en `datasets/mi_schema/`
8. Ejecutar: `python orchestrator.py --ingest mi_schema && python orchestrator.py --pipeline mi_schema`
9. El orquestador lo descubre automaticamente: `python orchestrator.py --list`

---

## Referencias

- **Apache Beam**: https://beam.apache.org/
- **Apache Kafka**: https://kafka.apache.org/
- **MongoDB Time-Series**: https://www.mongodb.com/docs/manual/core/timeseries-collections/
- **Polars**: https://pola.rs/
- **D3.js**: https://d3js.org/
- **Leaflet.js**: https://leafletjs.com/

---

**Ultima actualizacion:** 2026-02-10
**Version:** 2.0.0
