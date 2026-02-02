# Pipeline de Procesamiento de Datos COVID-19 en Tiempo Real

## Tabla de Contenidos

1. [Introducción](#introducción)
2. [Arquitectura del Sistema](#arquitectura-del-sistema)
3. [Componentes Principales](#componentes-principales)
4. [Flujo de Datos Detallado](#flujo-de-datos-detallado)
5. [Instalación y Configuración](#instalación-y-configuración)
6. [Guía de Ejecución](#guía-de-ejecución)
7. [Schemas Disponibles](#schemas-disponibles)
8. [Configuración Avanzada](#configuración-avanzada)
9. [Monitoreo y Observabilidad](#monitoreo-y-observabilidad)
10. [Casos de Uso y Ejemplos](#casos-de-uso-y-ejemplos)
11. [Troubleshooting](#troubleshooting)
12. [Desarrollo y Extensión](#desarrollo-y-extensión)

---

## Introducción

Este proyecto implementa una **arquitectura de procesamiento de datos en tiempo real** para datos de COVID-19 utilizando:

- **Apache Beam**: Framework de procesamiento distribuido
- **Apache Kafka**: Sistema de mensajería para ingesta de datos
- **MongoDB**: Base de datos con colecciones time-series
- **Polars**: Procesamiento eficiente de archivos CSV/Parquet

### Características Principales

✅ **Multi-Schema**: Múltiples pipelines independientes (cases, demises, etc.)
✅ **Tiempo Real**: Procesamiento streaming con ventanas temporales
✅ **Escalable**: Arquitectura horizontal con Apache Beam
✅ **Resiliente**: Dead Letter Queue (DLQ) para manejo de errores
✅ **Configurable**: Configuración independiente por schema
✅ **Paralelo**: Ejecución simultánea de múltiples schemas

---

## Arquitectura del Sistema

### Vista General de la Arquitectura

```mermaid
graph TB
    subgraph "ARQUITECTURA MULTI-SCHEMA"
        subgraph "SCHEMA: CASES - Datos de Pacientes Individuales"
            CSV1[CSV Files<br/>sample_data.csv] --> Kafka1[Kafka Topic<br/>cases]
            Kafka1 --> Beam1[Apache Beam Pipeline<br/>Transforms + Validate]
            Beam1 --> Mongo1[MongoDB Collection<br/>cases Time-Series]

            note1[Config: window=60s<br/>batch=100<br/>strategy=native]
        end

        subgraph "SCHEMA: DEMISES - Datos de Fallecimientos"
            CSV2[CSV Files<br/>demises data] --> Kafka2[Kafka Topic<br/>demises]
            Kafka2 --> Beam2[Apache Beam Pipeline<br/>Transforms + Validate]
            Beam2 --> Mongo2[MongoDB Collection<br/>demises Time-Series]

            note2[Config: window=120s<br/>batch=50<br/>strategy=manual]
        end

        subgraph "COMPONENTES COMPARTIDOS - Docker Compose"
            KafkaService[Kafka Service<br/>Port: 9092]
            MongoService[MongoDB Service<br/>Port: 27017]
            KafkaUI[Kafka UI<br/>Port: 8080]
            MongoExpress[Mongo Express<br/>Port: 8083]
        end
    end

    style CSV1 fill:#e1f5ff
    style CSV2 fill:#e1f5ff
    style Kafka1 fill:#fff4e6
    style Kafka2 fill:#fff4e6
    style Beam1 fill:#f3e5f5
    style Beam2 fill:#f3e5f5
    style Mongo1 fill:#e8f5e9
    style Mongo2 fill:#e8f5e9
    style KafkaService fill:#fff9c4
    style MongoService fill:#fff9c4
    style KafkaUI fill:#f1f8e9
    style MongoExpress fill:#f1f8e9
```

### Arquitectura de Pipeline Individual (Apache Beam)

```mermaid
graph TD
    Start[Kafka Source<br/>Topic: cases/demises] --> Parse[1. Parse Message<br/>JSON → Dict]

    Parse --> Normalize[2. Normalize<br/>Handle nulls<br/>Type conversions]
    Parse --> |Parse Error| DLQ1[DLQ: Parse Errors]

    Normalize --> Validate[3. Validate Schema<br/>Check required fields<br/>Validate types]
    Normalize --> |Normalization Error| DLQ2[DLQ: Normalization Errors]

    Validate --> Timestamp[4. Assign Timestamp<br/>Parse fecha_muestra<br/>Add timestamp field]
    Validate --> |Validation Error| DLQ3[DLQ: Validation Errors]

    Timestamp --> Window[5. Windowing<br/>Fixed Window<br/>60s or 120s]
    Timestamp --> |Timestamp Error| DLQ4[DLQ: Timestamp Errors]

    Window --> Metadata[6. Add Metadata<br/>pipeline_version<br/>worker_host<br/>window info]

    Metadata --> Batch[7. Batching<br/>Native/Manual<br/>Group N items]

    Batch --> MongoSink[8. MongoDB Sink<br/>Bulk Write<br/>Time-Series Collection]

    MongoSink --> Success[✓ MongoDB<br/>Collection: cases/demises]
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
    Root[tfm/] --> Pipelines[pipelines/]
    Root --> Src[src/]
    Root --> Datasets[datasets/]
    Root --> Config[Config Files]

    Pipelines --> Cases[cases/]
    Pipelines --> Deaths[demises/]

    Cases --> C_Config[config.yaml]
    Cases --> C_Schema[cases.json]
    Cases --> C_Pipeline[pipeline.py]
    Cases --> C_Ingestion[ingestion.py]

    Deaths --> D_Config[config.yaml]
    Deaths --> D_Schema[demises.json]
    Deaths --> D_Pipeline[pipeline.py]
    Deaths --> D_Ingestion[ingestion.py]

    Src --> Common[common/]
    Common --> Sources[sources/<br/>kafka_source.py<br/>storage_source.py]
    Common --> Transforms[transforms/<br/>normalize.py<br/>validate.py<br/>timestamp.py<br/>windowing.py<br/>metadata.py]
    Common --> Batching[batching/<br/>native_batch.py<br/>manual_batch.py]
    Common --> Sinks[sinks/<br/>mongo_sink.py<br/>dlq_sink.py]
    Common --> Utils[utils/<br/>config_loader.py<br/>schema_loader.py]

    Src --> Ingestion[ingestion/<br/>kafka_processor.py]

    Datasets --> DS_Cases[cases/<br/>sample_data.csv]
    Datasets --> DS_Deaths[demises/<br/>sample_data.csv]

    Config --> Orchestrator[orchestrator.py]
    Config --> RunCases[run_cases.sh]
    Config --> RunDemises[run_demises.sh]
    Config --> Docker[docker-compose.yaml]
    Config --> Reqs[requirements.txt]

    style Root fill:#e3f2fd
    style Pipelines fill:#fff3e0
    style Src fill:#f3e5f5
    style Datasets fill:#e8f5e9
    style Common fill:#fff9c4
    style Cases fill:#fce4ec
    style Deaths fill:#fce4ec
```

---

## Componentes Principales

### 1. Ingesta de Datos (Ingestion)

```mermaid
sequenceDiagram
    participant CSV as Archivo CSV<br/>sample_data.csv
    participant KP as KafkaProcessor<br/>kafka_processor.py
    participant Polars as Polars Library
    participant Kafka as Kafka Topic<br/>"cases"

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
- **KafkaProcessor** (`src/ingestion/kafka_processor.py`): Procesador común para ingesta
- **Ingestion Scripts** (`pipelines/{schema}/ingestion.py`): Scripts específicos por schema

### 2. Transformaciones del Pipeline

```mermaid
flowchart TD
    Input[Raw Data<br/>uuid: 1001<br/>edad: null] --> T1[TRANSFORM 1: NORMALIZE]

    T1 --> |Nulls → None<br/>Type conversions| T2[TRANSFORM 2: VALIDATE]

    T2 --> |Check required fields<br/>Validate types| T3[TRANSFORM 3: TIMESTAMP]

    T3 --> |Parse fecha_muestra<br/>Add timestamp field| T4[TRANSFORM 4: WINDOWING]

    T4 --> |Fixed Window<br/>60s / 120s| T5[TRANSFORM 5: METADATA]

    T5 --> |Add pipeline_version<br/>worker_host<br/>window info| T6[TRANSFORM 6: BATCHING]

    T6 --> |Native/Manual<br/>Group N elements| Output[Batched Data<br/>Ready for MongoDB]

    T2 -.->|Validation fails| DLQ_V[DLQ: Validation Error]
    T3 -.->|Parse fails| DLQ_T[DLQ: Timestamp Error]

    style T1 fill:#e3f2fd
    style T2 fill:#f3e5f5
    style T3 fill:#e8f5e9
    style T4 fill:#fff9c4
    style T5 fill:#fce4ec
    style T6 fill:#e0f2f1
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

    Check -->|Success| Success[MongoDB<br/>Collection: cases<br/>✓ N docs insertados]
    Check -->|Partial Error| Partial[Algunos docs<br/>escritos<br/>BulkWriteError]
    Check -->|Total Error| Error[Error completo<br/>Retry logic]

    Partial --> Log1[Log errores<br/>Continuar]
    Error --> Retry{Retry?}
    Retry -->|Yes| Execute
    Retry -->|No| DLQ[Send to DLQ]

    Success --> Metrics[Actualizar<br/>métricas]

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
    subgraph "PASO 1: PREPARACIÓN"
        CSV[datasets/cases/<br/>sample_data.csv<br/>20 registros]
    end

    subgraph "PASO 2: INGESTA"
        CSV --> Ingestion[python pipelines/cases/<br/>ingestion.py]
        Ingestion --> |KafkaProcessor<br/>Lee CSV con Polars<br/>Convierte a JSON| Producer[Kafka Producer]
        Producer --> |Envía mensajes| KafkaTopic[Kafka Topic: cases<br/>Partition 0: msgs 1-7<br/>Partition 1: msgs 8-14<br/>Partition 2: msgs 15-20]
    end

    subgraph "PASO 3: PROCESAMIENTO"
        KafkaTopic --> Pipeline[python pipelines/cases/<br/>pipeline.py<br/>Apache Beam Pipeline]
        Pipeline --> T1[1. Parse JSON]
        T1 --> T2[2. Normalize]
        T2 --> T3[3. Validate]
        T3 --> T4[4. Timestamp]
        T4 --> T5[5. Window 60s]
        T5 --> T6[6. Metadata]
        T6 --> T7[7. Batch 100]
    end

    subgraph "PASO 4: ALMACENAMIENTO"
        T7 --> |Datos válidos| MongoWrite[MongoDB Sink<br/>Bulk Write]
        MongoWrite --> MongoDB[(MongoDB<br/>Collection: cases<br/>Time-Series<br/>20 documentos)]

        T3 -.->|Errores| DLQWrite[DLQ Sink]
        T4 -.->|Errores| DLQWrite
        T7 -.->|Errores| DLQWrite
        DLQWrite --> DLQ[(MongoDB<br/>Collection: dead_letter_queue<br/>Errores capturados)]
    end

    subgraph "PASO 5: MONITOREO"
        MongoDB --> KafkaUI[Kafka UI<br/>http://localhost:8080]
        MongoDB --> MongoExpress[Mongo Express<br/>http://localhost:8083]
        DLQ --> MongoExpress
    end

    style CSV fill:#e3f2fd
    style KafkaTopic fill:#fff3e0
    style Pipeline fill:#f3e5f5
    style MongoDB fill:#c8e6c9
    style DLQ fill:#ffcdd2
    style KafkaUI fill:#fff9c4
    style MongoExpress fill:#fff9c4
```

### Diagrama de Secuencia Temporal

```mermaid
sequenceDiagram
    actor Usuario
    participant Ingestion
    participant Kafka
    participant Pipeline
    participant MongoDB
    participant DLQ

    Usuario->>Ingestion: Start Ingestion
    activate Ingestion

    Ingestion->>Ingestion: Read CSV with Polars

    loop For each row
        Ingestion->>Kafka: Send message<br/>(topic: cases, key: uuid)
        Kafka-->>Ingestion: ACK
    end

    Ingestion-->>Usuario: Done (20 messages sent)
    deactivate Ingestion

    Usuario->>Pipeline: Start Pipeline
    activate Pipeline

    loop Streaming Loop
        Pipeline->>Kafka: Poll messages
        Kafka-->>Pipeline: Batch of messages

        Pipeline->>Pipeline: Parse JSON
        Pipeline->>Pipeline: Normalize
        Pipeline->>Pipeline: Validate

        alt Valid data
            Pipeline->>Pipeline: Timestamp
            Pipeline->>Pipeline: Window
            Pipeline->>Pipeline: Add metadata
            Pipeline->>Pipeline: Batch (100 elements)

            Pipeline->>MongoDB: Bulk write
            MongoDB-->>Pipeline: Success
        else Invalid data
            Pipeline->>DLQ: Write error
            DLQ-->>Pipeline: Stored
        end

        Pipeline->>Pipeline: Log metrics
    end

    deactivate Pipeline
```

---

## Instalación y Configuración

### Diagrama de Instalación

```mermaid
graph TD
    Start([Inicio de Instalación]) --> Clone[1. Clonar repositorio<br/>o navegar a directorio]

    Clone --> Venv[2. Crear entorno virtual<br/>python3 -m venv venv]

    Venv --> Activate[3. Activar entorno<br/>source venv/bin/activate]

    Activate --> Install[4. Instalar dependencias<br/>pip install -r requirements.txt]

    Install --> Docker[5. Iniciar servicios<br/>docker-compose up -d]

    Docker --> Verify{6. Verificar<br/>servicios?}

    Verify -->|Kafka OK| VerifyMongo{MongoDB OK?}
    Verify -->|Kafka FAIL| TroubleshootK[Troubleshoot Kafka<br/>Ver logs]

    VerifyMongo -->|Mongo OK| Setup[7. Configurar MongoDB<br/>Crear colecciones<br/>time-series]
    VerifyMongo -->|Mongo FAIL| TroubleshootM[Troubleshoot MongoDB<br/>Ver logs]

    Setup --> Ready([✓ Sistema Listo])

    TroubleshootK -.-> Docker
    TroubleshootM -.-> Docker

    style Start fill:#e3f2fd
    style Ready fill:#c8e6c9
    style TroubleshootK fill:#ffcdd2
    style TroubleshootM fill:#ffcdd2
```

### Requisitos Previos

```
Sistema Operativo:
  ✓ Linux / macOS / Windows (con WSL2)

Software requerido:
  ✓ Python 3.8 o superior
  ✓ Docker y Docker Compose
  ✓ Git (opcional)

Recursos mínimos:
  ✓ 4 GB RAM
  ✓ 10 GB espacio en disco
  ✓ Conexión a internet
```

### Paso 1: Clonar el Repositorio

```bash
# Si el proyecto está en Git
git clone <repository-url>
cd tfm

# Si es un directorio local
cd /home/r2d2/master/TFM/tfm
```

### Paso 2: Crear Entorno Virtual

```bash
# Crear entorno virtual
python3 -m venv venv

# Activar entorno virtual
# En Linux/macOS:
source venv/bin/activate

# En Windows:
venv\Scripts\activate
```

### Paso 3: Instalar Dependencias

```bash
# Instalar todas las dependencias
pip install -r requirements.txt

# Verificar instalación
pip list | grep -E "(apache-beam|kafka|pymongo|polars)"
```

**Dependencias principales:**
- `apache-beam[gcp]`: Framework de procesamiento
- `kafka-python`: Cliente de Kafka
- `pymongo`: Cliente de MongoDB
- `polars`: Procesamiento de datos
- `pyyaml`: Parsing de configuración

### Paso 4: Iniciar Servicios con Docker

```bash
# Iniciar todos los servicios en background
docker-compose up -d

# Verificar que los servicios están corriendo
docker-compose ps

# Debe mostrar:
# NAME                STATUS          PORTS
# kafka               Up              9092->9092
# mongodb             Up              27017->27017
# kafka-ui            Up              8080->8080
# mongo-express       Up              8083->8083
# zookeeper           Up              2181->2181
```

**Servicios iniciados:**

| Servicio | Puerto | Descripción |
|----------|--------|-------------|
| Kafka | 9092 | Message broker |
| MongoDB | 27017 | Base de datos |
| Kafka UI | 8080 | Interface web para Kafka |
| Mongo Express | 8083 | Interface web para MongoDB |
| Zookeeper | 2181 | Coordinación de Kafka |

### Paso 5: Configurar MongoDB

```bash
# Conectar a MongoDB
docker exec -it mongodb mongosh

# Crear base de datos y colecciones time-series
use covid-db

# Crear colección time-series para cases
db.createCollection("cases", {
  timeseries: {
    timeField: "timestamp",
    metaField: "metadata",
    granularity: "hours"
  }
})

# Crear colección time-series para demises
db.createCollection("demises", {
  timeseries: {
    timeField: "timestamp",
    metaField: "metadata",
    granularity: "hours"
  }
})

# Crear colección para DLQ
db.createCollection("dead_letter_queue")

# Crear índices en DLQ
db.dead_letter_queue.createIndex({"error_type": 1})
db.dead_letter_queue.createIndex({"schema": 1})
db.dead_letter_queue.createIndex({"timestamp": 1})

# Salir
exit
```

---

## Guía de Ejecución

### Opciones de Ejecución

```mermaid
graph TD
    Start([Ejecutar Pipeline]) --> Choice{Método de<br/>ejecución?}

    Choice -->|Opción 1| Scripts[Scripts Shell<br/>run_cases.sh<br/>run_demises.sh]
    Choice -->|Opción 2| Direct[Ejecución Directa<br/>python pipelines/X/...]
    Choice -->|Opción 3| Orchestrator[Orquestador<br/>orchestrator.py]

    Scripts --> ScriptMode{Modo?}
    ScriptMode -->|ingest| S1[./run_cases.sh ingest]
    ScriptMode -->|pipeline| S2[./run_cases.sh pipeline]
    ScriptMode -->|both| S3[./run_cases.sh both]

    Direct --> D1[Terminal 1:<br/>python pipelines/cases/ingestion.py]
    Direct --> D2[Terminal 2:<br/>python pipelines/cases/pipeline.py --mode streaming]

    Orchestrator --> O1[Single schema:<br/>--ingest cases]
    Orchestrator --> O2[Multiple schemas:<br/>--pipeline cases demises --parallel]
    Orchestrator --> O3[All schemas:<br/>--pipeline-all --parallel]

    S1 --> Monitor
    S2 --> Monitor
    S3 --> Monitor
    D1 --> Monitor
    D2 --> Monitor
    O1 --> Monitor
    O2 --> Monitor
    O3 --> Monitor

    Monitor([Monitorear Resultados<br/>Kafka UI / Mongo Express])

    style Start fill:#e3f2fd
    style Scripts fill:#fff3e0
    style Direct fill:#f3e5f5
    style Orchestrator fill:#e8f5e9
    style Monitor fill:#c8e6c9
```

### Opción 1: Ejecución con Scripts

```bash
# Hacer script ejecutable (solo primera vez)
chmod +x run_cases.sh

# Ejecutar solo ingesta
./run_cases.sh ingest

# Ejecutar solo pipeline
./run_cases.sh pipeline

# Ejecutar ingesta + pipeline
./run_cases.sh both
```

### Opción 2: Ejecución Directa

```bash
# Schema CASES
# Terminal 1: Ingesta
python pipelines/cases/ingestion.py

# Terminal 2: Pipeline (streaming desde Kafka)
python pipelines/cases/pipeline.py --mode streaming

# Terminal 2: Pipeline (batch desde archivos)
python pipelines/cases/pipeline.py --mode batch

# Schema DEMISES
python pipelines/demises/ingestion.py
python pipelines/demises/pipeline.py --mode streaming
```

### Opción 3: Orquestador Multi-Schema

```bash
# Listar schemas disponibles
python orchestrator.py --list

# Ejecutar ingesta de un schema
python orchestrator.py --ingest cases

# Ejecutar pipeline de un schema
python orchestrator.py --pipeline cases

# Ejecutar múltiples pipelines en paralelo
python orchestrator.py --pipeline cases demises --parallel

# Ejecutar TODOS los pipelines en paralelo
python orchestrator.py --pipeline-all --parallel

# Ejecutar TODAS las ingests en paralelo
python orchestrator.py --ingest-all --parallel
```

### Flujo de Trabajo Recomendado

```mermaid
sequenceDiagram
    actor Usuario
    participant Docker as Docker Services
    participant Orchestrator as Orchestrator
    participant Kafka
    participant Pipeline as Apache Beam
    participant MongoDB

    Usuario->>Docker: 1. docker-compose ps
    Docker-->>Usuario: ✓ Servicios OK

    Usuario->>Orchestrator: 2. orchestrator.py<br/>--ingest-all --parallel
    activate Orchestrator
    Orchestrator->>Kafka: Ingestar casos
    Orchestrator->>Kafka: Ingestar demises
    Kafka-->>Orchestrator: ✓ Datos en Kafka
    Orchestrator-->>Usuario: ✓ Ingesta completada
    deactivate Orchestrator

    Usuario->>Orchestrator: 3. orchestrator.py<br/>--pipeline-all --parallel
    activate Orchestrator
    Orchestrator->>Pipeline: Iniciar pipeline cases
    Orchestrator->>Pipeline: Iniciar pipeline demises

    Pipeline->>Kafka: Consumir mensajes
    Pipeline->>MongoDB: Escribir datos procesados

    MongoDB-->>Pipeline: ✓ Escritura OK
    Pipeline-->>Orchestrator: ✓ Pipeline completado
    Orchestrator-->>Usuario: ✓ Procesamiento completado
    deactivate Orchestrator

    Usuario->>MongoDB: 4. Verificar resultados<br/>http://localhost:8083
    MongoDB-->>Usuario: Datos disponibles
```

---

## Schemas Disponibles

### Schema: CASES (Datos de Pacientes Individuales)

#### Estructura de Datos

```mermaid
erDiagram
    CASES {
        integer uuid PK "Identificador único"
        integer fecha_muestra "Fecha de muestra (Unix timestamp)"
        integer edad "Edad del paciente"
        string sexo "Sexo (M/F)"
        string resultado "Resultado (Positivo/Negativo)"
        string institucion "Hospital o clínica"
        integer ubigeo_paciente "Código UBIGEO"
        string departamento_paciente "Departamento"
        string provincia_paciente "Provincia"
        string distrito_paciente "Distrito"
        string departamento_muestra "Depto donde se tomó muestra"
        string provincia_muestra "Prov donde se tomó muestra"
        string distrito_muestra "Dist donde se tomó muestra"
        string tipo_muestra "Tipo (Nasofaringea/Saliva/Orofaringea)"
        number timestamp "Timestamp procesamiento"
        object metadata "Metadata del pipeline"
    }
```

**Campos requeridos:**
- `uuid` (int): Identificador único del paciente
- `fecha_muestra` (int): Fecha de toma de muestra (Unix timestamp)
- `edad` (int): Edad del paciente
- `sexo` (string): Sexo del paciente (M/F)
- `resultado` (string): Resultado de la prueba (Positivo/Negativo)

**Campos opcionales:**
- `institucion`, ubicación geográfica, `tipo_muestra`, etc.

#### Ejemplo de Registro

```json
{
  "uuid": 1001,
  "fecha_muestra": 1672531200,
  "edad": 45,
  "sexo": "M",
  "institucion": "Hospital Nacional Dos de Mayo",
  "ubigeo_paciente": 150101,
  "departamento_paciente": "Lima",
  "provincia_paciente": "Lima",
  "distrito_paciente": "Lima",
  "departamento_muestra": "Lima",
  "provincia_muestra": "Lima",
  "distrito_muestra": "Lima",
  "tipo_muestra": "Nasofaringea",
  "resultado": "Positivo",
  "timestamp": 1672531200.0,
  "metadata": {
    "pipeline_version": "1.0.0",
    "processed_at": "2023-01-01T12:00:00.123456",
    "worker_host": "worker-1",
    "window_start": "2023-01-01T12:00:00",
    "window_end": "2023-01-01T12:01:00",
    "schema": "cases"
  }
}
```

#### Configuración del Pipeline

```yaml
windowing:
  window_size_seconds: 60      # Ventanas de 1 minuto

batching:
  strategy: "native"           # Batching nativo de Apache Beam
  batch_size: 100              # 100 elementos por batch

kafka:
  topic: "cases"
  consumer_config:
    group.id: "beam-pipeline-cases"
```

#### Ejecución

```bash
# Con script
./run_cases.sh both

# Directo
python pipelines/cases/ingestion.py
python pipelines/cases/pipeline.py --mode streaming

# Con orquestador
python orchestrator.py --ingest cases
python orchestrator.py --pipeline cases
```

### Schema: DEMISES (Datos de Fallecimientos)

Similar estructura pero con datos de fallecimientos por COVID-19.

```yaml
windowing:
  window_size_seconds: 120     # Ventanas de 2 minutos

batching:
  strategy: "manual"           # Batching manual
  batch_size: 50               # 50 elementos por batch
```

---

## Configuración Avanzada

### Archivo config.yaml

```mermaid
graph TD
    Config[config.yaml] --> Schema[Schema Info<br/>name, version, description]
    Config --> Source[Source Config]
    Config --> Transforms[Transforms Config]
    Config --> Batching[Batching Config]
    Config --> Sink[Sink Config]
    Config --> Pipeline[Pipeline Options]

    Source --> Kafka[Kafka Config<br/>bootstrap_servers<br/>topic<br/>consumer_config]
    Source --> Storage[Storage Config<br/>file_pattern<br/>file_type]

    Transforms --> T1[normalize: enabled]
    Transforms --> T2[validate: enabled<br/>schema_file]
    Transforms --> T3[timestamp: enabled<br/>field]
    Transforms --> T4[windowing: enabled<br/>window_size_seconds<br/>allowed_lateness_seconds]
    Transforms --> T5[metadata: enabled<br/>pipeline_version]

    Batching --> B1[strategy: native/manual]
    Batching --> B2[batch_size: N]
    Batching --> B3[batch_timeout_seconds]

    Sink --> S1[mongodb:<br/>connection_string<br/>database<br/>collection]
    Sink --> S2[dlq:<br/>collection]

    Pipeline --> P1[runner: DirectRunner/<br/>DataflowRunner]
    Pipeline --> P2[streaming: true]

    style Config fill:#e3f2fd
    style Kafka fill:#fff3e0
    style T4 fill:#f3e5f5
    style S1 fill:#e8f5e9
```

#### Estructura Completa

```yaml
# Configuración del pipeline para el schema CASES
schema:
  name: "cases"
  version: "1.0.0"
  description: "Pipeline para casos de COVID-19 - Datos de pacientes individuales"

# Source configuration
source:
  type: "kafka"  # "kafka" o "storage"

  kafka:
    bootstrap_servers: "localhost:9092"
    topic: "cases"
    consumer_config:
      group.id: "beam-pipeline-cases"
      auto.offset.reset: "earliest"
      enable.auto.commit: false
      max.poll.records: 500

  storage:
    file_pattern: "datasets/cases/*.csv"
    file_type: "csv"

# Transform configuration
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

# Batching configuration
batching:
  strategy: "native"
  batch_size: 100
  batch_timeout_seconds: 30

# Sink configuration
sink:
  mongodb:
    connection_string: "mongodb://root:example@localhost:27017/"
    database: "covid-db"
    collection:
      name: "cases"
      timeseries:
        timeField: "timestamp"
        metaField: "metadata"
        granularity: "hours"

  dlq:
    collection: "dead_letter_queue"

# Pipeline options
pipeline:
  runner: "DirectRunner"
  streaming: true
```

---

## Monitoreo y Observabilidad

### Herramientas de Monitoreo

```mermaid
graph TB
    subgraph "MONITOREO Y OBSERVABILIDAD"
        Pipeline[Apache Beam Pipeline]

        Pipeline --> Kafka[Kafka Monitoring]
        Pipeline --> Mongo[MongoDB Monitoring]
        Pipeline --> Logs[Logs & Metrics]

        Kafka --> KafkaUI[Kafka UI<br/>http://localhost:8080<br/>- Ver topics<br/>- Ver mensajes<br/>- Consumer lag<br/>- Particiones]

        Mongo --> MongoExpress[Mongo Express<br/>http://localhost:8083<br/>- Ver colecciones<br/>- Ejecutar queries<br/>- Export/Import<br/>- Análisis de DLQ]

        Logs --> Console[Console Output<br/>- INFO logs<br/>- WARNING logs<br/>- ERROR logs<br/>- Métricas]

        Logs --> Files[Log Files<br/>pipeline.log<br/>ingestion.log]

        KafkaUI --> Analysis1[Análisis:<br/>- Messages per second<br/>- Consumer lag<br/>- Partition distribution]

        MongoExpress --> Analysis2[Análisis:<br/>- Documentos procesados<br/>- Errores en DLQ<br/>- Time-series queries]

        Console --> Analysis3[Análisis:<br/>- Throughput<br/>- Error rate<br/>- Processing time]
    end

    style Pipeline fill:#e3f2fd
    style KafkaUI fill:#fff3e0
    style MongoExpress fill:#f3e5f5
    style Console fill:#e8f5e9
```

### Consultas Útiles de MongoDB

**Ver últimos casos procesados:**
```javascript
use covid-db
db.cases.find().sort({"timestamp": -1}).limit(10)
```

**Contar registros por resultado:**
```javascript
db.cases.aggregate([
  {$group: {_id: "$resultado", count: {$sum: 1}}}
])
```

**Ver errores en DLQ:**
```javascript
db.dead_letter_queue.find().sort({"timestamp": -1})
```

**Contar errores por tipo:**
```javascript
db.dead_letter_queue.aggregate([
  {$group: {_id: "$error_type", count: {$sum: 1}}}
])
```

---

## Casos de Uso y Ejemplos

### Caso de Uso 1: Análisis de Casos por Departamento

```mermaid
graph LR
    Query[Query MongoDB] --> Filter[Filtrar:<br/>resultado = Positivo]
    Filter --> Group[Agrupar por:<br/>departamento_paciente]
    Group --> Aggregate[Calcular:<br/>total_casos<br/>edad_promedio]
    Aggregate --> Sort[Ordenar por:<br/>total_casos DESC]
    Sort --> Result[Resultado:<br/>Lima: 12 casos<br/>Arequipa: 3 casos<br/>etc.]

    style Query fill:#e3f2fd
    style Result fill:#c8e6c9
```

**Consulta MongoDB:**

```javascript
use covid-db

db.cases.aggregate([
  {
    $match: {
      "resultado": "Positivo"
    }
  },
  {
    $group: {
      _id: "$departamento_paciente",
      total_casos: {$sum: 1},
      edad_promedio: {$avg: "$edad"}
    }
  },
  {
    $sort: {total_casos: -1}
  }
])
```

### Caso de Uso 2: Pipeline en Tiempo Real

```mermaid
sequenceDiagram
    participant T1 as Terminal 1<br/>Pipeline
    participant T2 as Terminal 2<br/>Ingesta Continua
    participant T3 as Terminal 3<br/>Monitor
    participant Kafka
    participant MongoDB

    T1->>Kafka: Start streaming pipeline
    activate T1

    loop Every 10 seconds
        T2->>Kafka: Ingest new data
    end

    loop Streaming
        T1->>Kafka: Poll messages
        T1->>T1: Process batch
        T1->>MongoDB: Write results
    end

    loop Every 5 seconds
        T3->>MongoDB: Query count
        MongoDB-->>T3: Current count
    end

    deactivate T1
```

**Comandos:**

```bash
# Terminal 1: Iniciar pipeline (modo streaming)
python pipelines/cases/pipeline.py --mode streaming

# Terminal 2: Ingestar datos continuamente
while true; do
  python pipelines/cases/ingestion.py
  sleep 10
done

# Terminal 3: Monitorear MongoDB
watch -n 5 'docker exec mongodb mongosh --quiet --eval "use covid-db; db.cases.count()"'
```

---

## Troubleshooting

### Diagrama de Troubleshooting

```mermaid
graph TD
    Start([Problema Detectado]) --> Type{Tipo de<br/>problema?}

    Type -->|Kafka| K1[No se puede conectar]
    Type -->|MongoDB| M1[MongoDB no responde]
    Type -->|Pipeline| P1[Pipeline no procesa]
    Type -->|Errores| E1[Errores de validación]
    Type -->|Performance| Perf1[Consumer lag alto]

    K1 --> K2{Kafka corriendo?}
    K2 -->|No| K3[docker-compose restart kafka]
    K2 -->|Sí| K4[Verificar logs<br/>docker-compose logs kafka]

    M1 --> M2{MongoDB corriendo?}
    M2 -->|No| M3[docker-compose restart mongodb]
    M2 -->|Sí| M4[Test conexión<br/>mongosh --eval ping]

    P1 --> P2[Verificar mensajes<br/>en topic]
    P2 --> P3[Verificar consumer lag]
    P3 --> P4[Reset offsets si<br/>es necesario]

    E1 --> E2[Query DLQ<br/>ver ejemplos de errores]
    E2 --> E3{Tipo de error?}
    E3 -->|Missing field| E4[Agregar campo a CSV<br/>o hacer opcional]
    E3 -->|Type error| E5[Corregir tipos en<br/>schema.json]

    Perf1 --> Perf2[Aumentar batch_size]
    Perf2 --> Perf3[Aumentar max_poll_records]
    Perf3 --> Perf4[Escalar horizontalmente]

    K3 --> Resolved([✓ Resuelto])
    K4 --> Resolved
    M3 --> Resolved
    M4 --> Resolved
    P4 --> Resolved
    E4 --> Resolved
    E5 --> Resolved
    Perf4 --> Resolved

    style Start fill:#e3f2fd
    style Resolved fill:#c8e6c9
    style K1 fill:#ffcdd2
    style M1 fill:#ffcdd2
    style P1 fill:#ffcdd2
    style E1 fill:#ffcdd2
    style Perf1 fill:#fff3e0
```

### Problemas Comunes

#### Problema 1: No se puede conectar a Kafka

**Síntoma:**
```
ERROR: NoBrokersAvailable
```

**Solución:**
```bash
docker-compose ps kafka
docker-compose restart kafka
docker-compose logs kafka | tail -50
```

#### Problema 2: Pipeline no procesa mensajes

**Diagnóstico:**
```bash
# Verificar mensajes en topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cases \
  --from-beginning \
  --max-messages 5

# Resetear offset
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group beam-pipeline-cases \
  --reset-offsets \
  --to-earliest \
  --topic cases \
  --execute
```

---

## Desarrollo y Extensión

### Agregar un Nuevo Schema

```mermaid
graph TD
    Start([Nuevo Schema]) --> Step1[1. Crear directorios<br/>pipelines/mi_schema<br/>datasets/mi_schema]

    Step1 --> Step2[2. Copiar archivos base<br/>config.yaml<br/>schema.json<br/>pipeline.py<br/>ingestion.py]

    Step2 --> Step3[3. Modificar config.yaml<br/>- Cambiar name<br/>- Cambiar topic<br/>- Cambiar consumer group]

    Step3 --> Step4[4. Modificar schema.json<br/>- Definir campos<br/>- required_fields<br/>- field_types]

    Step4 --> Step5[5. Actualizar pipeline.py<br/>- Cambiar clase<br/>MiSchemaPipeline]

    Step5 --> Step6[6. Actualizar ingestion.py<br/>- Cambiar clase<br/>MiSchemaIngestion]

    Step6 --> Step7[7. Crear datos ejemplo<br/>sample_data.csv]

    Step7 --> Step8[8. Crear colección MongoDB<br/>db.createCollection]

    Step8 --> Step9[9. Crear script ejecución<br/>run_mi_schema.sh]

    Step9 --> Step10[10. Ejecutar<br/>orchestrator.py --ingest mi_schema<br/>orchestrator.py --pipeline mi_schema]

    Step10 --> End([✓ Schema Listo])

    style Start fill:#e3f2fd
    style End fill:#c8e6c9
```

#### Pasos Detallados

**1. Crear estructura:**
```bash
mkdir -p pipelines/mi_schema
mkdir -p datasets/mi_schema
touch pipelines/mi_schema/__init__.py
```

**2. Copiar archivos base:**
```bash
cp pipelines/cases/config.yaml pipelines/mi_schema/
cp pipelines/cases/schema.json pipelines/mi_schema/
cp pipelines/cases/pipeline.py pipelines/mi_schema/
cp pipelines/cases/ingestion.py pipelines/mi_schema/
```

**3. Modificar config.yaml:**
```yaml
schema:
  name: "mi_schema"
  version: "1.0.0"
  description: "Pipeline para mi schema"

source:
  kafka:
    topic: "mi_schema"
    consumer_config:
      group.id: "beam-pipeline-mi_schema"
```

**4. Modificar schema.json:**
```json
{
  "schema_name": "mi_schema",
  "required_fields": ["id", "timestamp"],
  "field_types": {
    "id": "string",
    "timestamp": "number"
  }
}
```

**5-6. Actualizar código Python** (cambiar nombres de clases)

**7. Crear datos:**
```bash
cat > datasets/mi_schema/sample_data.csv << EOF
id,timestamp,value
1,1672531200,100.5
2,1672531260,105.2
EOF
```

**8. Configurar MongoDB:**
```bash
docker exec -it mongodb mongosh
use covid-db
db.createCollection("mi_schema", {
  timeseries: {
    timeField: "timestamp",
    metaField: "metadata",
    granularity: "hours"
  }
})
exit
```

**9-10. Ejecutar:**
```bash
python orchestrator.py --list
python orchestrator.py --ingest mi_schema
python orchestrator.py --pipeline mi_schema
```

---

## Arquitectura de Despliegue

### Despliegue en Google Cloud Platform

```mermaid
graph TB
    subgraph "GCP - PRODUCCIÓN"
        subgraph "Data Ingestion"
            GCS[Cloud Storage<br/>CSV files] --> Kafka[Confluent Kafka<br/>Managed Service]
        end

        subgraph "Processing"
            Kafka --> Dataflow[Cloud Dataflow<br/>Apache Beam<br/>Auto-scaling workers]
        end

        subgraph "Storage"
            Dataflow --> MongoAtlas[MongoDB Atlas<br/>Time-Series Collections]
            Dataflow --> BigQuery[BigQuery<br/>Analytics]
        end

        subgraph "Monitoring"
            CloudMon[Cloud Monitoring]
            CloudLog[Cloud Logging]
            ErrorReport[Error Reporting]
        end

        Dataflow -.-> CloudMon
        Dataflow -.-> CloudLog
        Dataflow -.-> ErrorReport
    end

    style GCS fill:#e3f2fd
    style Kafka fill:#fff3e0
    style Dataflow fill:#f3e5f5
    style MongoAtlas fill:#e8f5e9
    style BigQuery fill:#fff9c4
```

---

## Glosario de Términos

**Apache Beam**: Framework de procesamiento unificado para batch y streaming

**Kafka**: Sistema de mensajería distribuido para streaming de datos

**MongoDB Time-Series**: Colecciones optimizadas para datos temporales

**Pipeline**: Secuencia de transformaciones sobre datos

**Schema**: Definición de estructura y validación de datos

**Window**: Agrupación de datos en intervalos temporales

**Batch**: Grupo de elementos procesados juntos

**DLQ (Dead Letter Queue)**: Cola para mensajes con errores

**Consumer Group**: Grupo de consumidores que leen del mismo topic

**Offset**: Posición en el log de Kafka

**Partition**: División lógica de un topic de Kafka

**Transform**: Operación que modifica datos

**Sink**: Destino final de los datos

**Source**: Origen de los datos

---

## Referencias

- **Apache Beam**: https://beam.apache.org/
- **Apache Kafka**: https://kafka.apache.org/
- **MongoDB Time-Series**: https://www.mongodb.com/docs/manual/core/timeseries-collections/
- **Polars**: https://pola.rs/
- **Mermaid**: https://mermaid.js.org/

---

## Soporte y Contacto

Para preguntas, problemas o sugerencias, por favor contactar a:

- **Email**: [email de soporte]
- **Issues**: [GitHub Issues URL]
- **Documentation**: Este archivo README.md

---

## Licencia

[Especificar licencia del proyecto]

---

**Última actualización:** 2026-01-26
**Versión:** 1.0.1
