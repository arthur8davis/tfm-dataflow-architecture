# Estructura del Proyecto - Documentación Detallada

Este documento describe cada carpeta y archivo del proyecto, explicando su funcionalidad y cómo interactúan entre sí.

---

## Tabla de Contenidos

1. [Vista General](#vista-general)
2. [Carpeta Raíz](#carpeta-raíz-tfm)
3. [Carpeta pipelines/](#carpeta-pipelines)
4. [Carpeta src/](#carpeta-src)
5. [Carpeta datasets/](#carpeta-datasets)
6. [Diagrama de Dependencias](#diagrama-de-dependencias)
7. [Resumen de Componentes](#resumen-de-componentes)

---

## Vista General

```mermaid
graph TB
    subgraph "ESTRUCTURA DEL PROYECTO"
        Root[tfm/]

        Root --> Pipelines[pipelines/<br/>Pipelines por schema]
        Root --> Src[src/<br/>Código fuente común]
        Root --> Datasets[datasets/<br/>Archivos de datos]
        Root --> Config[Archivos de<br/>configuración]
        Root --> Docs[Documentación]
    end

    subgraph "PIPELINES"
        Pipelines --> Cases[cases/]
        Pipelines --> Demises[demises/]
    end

    subgraph "SOURCE CODE"
        Src --> Common[common/]
        Src --> Ingestion[ingestion/]
        Common --> Sources[sources/]
        Common --> Transforms[transforms/]
        Common --> Batching[batching/]
        Common --> Sinks[sinks/]
        Common --> Utils[utils/]
    end

    style Root fill:#e3f2fd
    style Pipelines fill:#fff3e0
    style Src fill:#f3e5f5
    style Datasets fill:#e8f5e9
```

---

## Carpeta Raíz (`tfm/`)

### Archivos de Configuración y Ejecución

| Archivo | Tipo | Descripción |
|---------|------|-------------|
| `docker-compose.yaml` | Configuración | Define los servicios Docker: Kafka, Zookeeper, MongoDB, Kafka UI, Mongo Express |
| `requirements.txt` | Dependencias | Lista de paquetes Python necesarios |
| `orchestrator.py` | Python | **Orquestador central** - Descubre y ejecuta pipelines de múltiples schemas |

### Scripts de Ejecución

| Archivo | Descripción |
|---------|-------------|
| `run_cases.sh` | Script bash para ejecutar el pipeline de CASES (opciones: ingest/pipeline/both) |
| `run_deaths.sh` | Script bash para ejecutar el pipeline de DEATHS |
| `example_run_all.sh` | Ejemplo de ejecución de todos los pipelines en paralelo |
| `verify_structure.sh` | Verifica que la estructura del proyecto esté correcta |

### Documentación

| Archivo | Descripción |
|---------|-------------|
| `README.md` | Documentación principal con diagramas Mermaid |
| `ARCHITECTURE.md` | Documentación de arquitectura técnica |
| `QUICKSTART.md` | Guía rápida de inicio |
| `GUIA_NUEVO_SCHEMA.md` | Guía paso a paso para agregar nuevos schemas |
| `ESTRUCTURA_PROYECTO.md` | Este archivo - Documentación de estructura |

---

### Detalle: `orchestrator.py`

El orquestador es el punto de entrada principal para ejecutar pipelines.

```mermaid
graph TD
    Orch[orchestrator.py] --> Discover[Descubrir schemas<br/>automáticamente]
    Discover --> List[--list<br/>Listar schemas]
    Discover --> Ingest[--ingest X<br/>Ejecutar ingesta]
    Discover --> Pipeline[--pipeline X<br/>Ejecutar pipeline]
    Discover --> Parallel[--parallel<br/>Ejecución paralela]

    Ingest --> Cases[cases/ingestion.py]
    Ingest --> Demises[demises/ingestion.py]

    Pipeline --> CasesP[cases/pipeline.py]
    Pipeline --> DemisesP[demises/pipeline.py]
```

**Funcionalidades principales:**

```python
class PipelineOrchestrator:
    def _discover_schemas()    # Auto-descubre schemas en pipelines/
    def list_schemas()         # Lista schemas disponibles
    def run_ingest()           # Ejecuta ingesta de un schema
    def run_pipeline()         # Ejecuta pipeline de un schema
    def run_parallel()         # Ejecuta múltiples schemas en paralelo
```

**Comandos disponibles:**

```bash
# Listar schemas
python orchestrator.py --list

# Ejecutar ingesta
python orchestrator.py --ingest cases
python orchestrator.py --ingest-all --parallel

# Ejecutar pipeline
python orchestrator.py --pipeline cases
python orchestrator.py --pipeline-all --parallel

# Ejecutar múltiples schemas
python orchestrator.py --pipeline cases demises --parallel
```

---

### Detalle: `docker-compose.yaml`

Define los servicios de infraestructura necesarios.

```mermaid
graph LR
    subgraph "Docker Compose Services"
        Zookeeper[Zookeeper<br/>:2181]
        Kafka[Kafka<br/>:9092]
        MongoDB[MongoDB<br/>:27017]
        KafkaUI[Kafka UI<br/>:8080]
        MongoExpress[Mongo Express<br/>:8083]
    end

    Zookeeper --> Kafka
    Kafka --> KafkaUI
    MongoDB --> MongoExpress

    style Kafka fill:#fff3e0
    style MongoDB fill:#e8f5e9
```

| Servicio | Puerto | Descripción |
|----------|--------|-------------|
| Zookeeper | 2181 | Coordinación de Kafka |
| Kafka | 9092 | Message broker para streaming |
| MongoDB | 27017 | Base de datos time-series |
| Kafka UI | 8080 | Interface web para monitorear Kafka |
| Mongo Express | 8083 | Interface web para MongoDB |

---

## Carpeta `pipelines/`

Contiene los **pipelines específicos por schema**. Cada schema es completamente independiente.

```mermaid
graph TB
    Pipelines[pipelines/] --> Init1[__init__.py]

    Pipelines --> Cases[cases/]
    Pipelines --> Demises[demises/]

    subgraph "cases/"
        Cases --> CC[config.yaml]
        Cases --> CJ[cases.json]
        Cases --> CP[pipeline.py]
        Cases --> CI[ingestion.py]
        Cases --> CInit[__init__.py]
    end

    subgraph "demises/"
        Demises --> DC[config.yaml]
        Demises --> DJ[demises.json]
        Demises --> DP[pipeline.py]
        Demises --> DI[ingestion.py]
        Demises --> DInit[__init__.py]
    end

    style Cases fill:#fce4ec
    style Demises fill:#e8f5e9
```

---

### `pipelines/cases/` - Schema de Casos COVID-19

#### Archivos

| Archivo | Tipo | Descripción |
|---------|------|-------------|
| `config.yaml` | Configuración | Configuración completa del pipeline |
| `cases.json` | Schema | Definición de campos y validación |
| `pipeline.py` | Python | Clase `CasesPipeline` - Pipeline Apache Beam |
| `ingestion.py` | Python | Clase `CasesIngestion` - Ingesta de datos |
| `__init__.py` | Python | Hace la carpeta un módulo importable |

---

#### Detalle: `config.yaml`

Configuración completa del pipeline de CASES.

```yaml
# Identificación del schema
schema:
  name: "cases"
  version: "1.0.0"
  description: "Pipeline para casos de COVID-19"

# Fuente de datos
source:
  type: "storage"  # "kafka" o "storage"

  kafka:
    bootstrap_servers: "localhost:9092"
    topic: "cases"
    consumer_config:
      group.id: "beam-pipeline-cases"
      auto.offset.reset: "earliest"

  storage:
    file_pattern: "datasets/cases/*.csv"
    file_type: "csv"

# Transformaciones
transforms:
  normalize:
    enabled: true

  validate:
    enabled: true
    schema_file: "pipelines/cases/cases.json"

  timestamp:
    enabled: true
    field: "fecha_muestra"

  windowing:
    enabled: true
    window_size_seconds: 60
    allowed_lateness_seconds: 300

  metadata:
    enabled: true
    pipeline_version: "1.0.0"

# Batching
batching:
  strategy: "native"  # "native" o "manual"
  batch_size: 100
  batch_timeout_seconds: 30

# Destino
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

# Opciones del pipeline
pipeline:
  runner: "DirectRunner"
  streaming: true
```

```mermaid
graph TD
    Config[config.yaml] --> Schema[schema:<br/>name, version]
    Config --> Source[source:<br/>kafka/storage]
    Config --> Transforms[transforms:<br/>normalize, validate,<br/>timestamp, windowing,<br/>metadata]
    Config --> Batching[batching:<br/>strategy, batch_size]
    Config --> Sink[sink:<br/>mongodb, dlq]
    Config --> Pipeline[pipeline:<br/>runner, streaming]

    style Config fill:#e3f2fd
```

---

#### Detalle: `cases.json`

Schema de validación de datos para CASES.

```json
{
  "schema_name": "cases",
  "version": "1.0.0",
  "description": "Schema para casos de COVID-19",

  "required_fields": [
    "fecha_muestra",
    "edad",
    "sexo",
    "resultado"
  ],

  "field_types": {
    "uuid": "integer",
    "fecha_muestra": "integer",
    "edad": "integer",
    "sexo": "string",
    "institucion": "string",
    "ubigeo_paciente": "integer",
    "departamento_paciente": "string",
    "provincia_paciente": "string",
    "distrito_paciente": "string",
    "departamento_muestra": "string",
    "provincia_muestra": "string",
    "distrito_muestra": "string",
    "tipo_muestra": "string",
    "resultado": "string",
    "timestamp": "number"
  },

  "optional_fields": [
    "uuid",
    "institucion",
    "ubigeo_paciente",
    "departamento_paciente",
    "provincia_paciente",
    "distrito_paciente",
    "departamento_muestra",
    "provincia_muestra",
    "distrito_muestra",
    "tipo_muestra",
    "timestamp"
  ]
}
```

**Validaciones realizadas:**
- ✅ Campos requeridos presentes
- ✅ Tipos de datos correctos
- ✅ Campos opcionales aceptados

---

#### Detalle: `pipeline.py`

Clase principal que construye el pipeline Apache Beam.

```python
class CasesPipeline:
    """Pipeline para procesar datos de CASES"""

    def __init__(self, config_path: str = None):
        # Carga configuración desde config.yaml
        self.config = self._load_config(config_path)
        self.schema_name = self.config['schema']['name']

    def build(self) -> beam.Pipeline:
        # Construye el pipeline completo
        pass

    def run(self):
        # Ejecuta el pipeline
        pipeline = self.build()
        pipeline.run().wait_until_finish()
```

```mermaid
graph TD
    Pipeline[CasesPipeline] --> Init[__init__<br/>Cargar config.yaml]
    Pipeline --> Build[build<br/>Construir pipeline]
    Pipeline --> Run[run<br/>Ejecutar pipeline]

    Build --> Source[1. Create Source<br/>Kafka/Storage]
    Build --> Parse[2. Parse Messages]
    Build --> Normalize[3. Normalize]
    Build --> Validate[4. Validate]
    Build --> Timestamp[5. Assign Timestamp]
    Build --> Window[6. Apply Windowing]
    Build --> Metadata[7. Add Metadata]
    Build --> Batch[8. Batch Elements]
    Build --> Sink[9. Write to MongoDB]
    Build --> DLQ[10. Handle Errors → DLQ]
```

---

#### Detalle: `ingestion.py`

Clase que maneja la ingesta de datos desde archivos a Kafka.

```python
class CasesIngestion:
    """Ingesta de datos para el schema CASES"""

    def __init__(self, config_path: str = None):
        # Carga configuración
        self.config = self._load_config(config_path)

    def run(self, directory: str = None, file: str = None):
        # Ejecuta la ingesta
        # 1. Lee archivos CSV/Parquet
        # 2. Convierte a JSON
        # 3. Envía a Kafka
        pass
```

```mermaid
sequenceDiagram
    participant User
    participant Ingestion as CasesIngestion
    participant Processor as KafkaProcessor
    participant Kafka

    User->>Ingestion: run(file="data.csv")
    Ingestion->>Processor: process_csv()
    Processor->>Processor: Leer CSV con Polars
    loop Cada fila
        Processor->>Processor: Convertir a JSON
        Processor->>Kafka: Enviar mensaje
        Kafka-->>Processor: ACK
    end
    Processor-->>Ingestion: Completado
    Ingestion-->>User: N mensajes enviados
```

---

### `pipelines/demises/` - Schema de Fallecimientos

Estructura idéntica a `cases/` pero configurado para datos de fallecimientos COVID-19.

**Diferencias principales en configuración:**

| Parámetro | CASES | DEMISES |
|-----------|-------|---------|
| `window_size_seconds` | 60 | 120 |
| `batch_size` | 100 | 50 |
| `batching.strategy` | native | manual |
| `topic` | cases | demises |
| `collection` | cases | demises |

---

## Carpeta `src/`

Contiene el **código fuente reutilizable** compartido entre todos los schemas.

```mermaid
graph TB
    Src[src/] --> Init0[__init__.py]
    Src --> Common[common/]
    Src --> Ingestion[ingestion/]

    Common --> Init1[__init__.py]
    Common --> Sources[sources/]
    Common --> Transforms[transforms/]
    Common --> Batching[batching/]
    Common --> Sinks[sinks/]
    Common --> Utils[utils/]

    Ingestion --> Init2[__init__.py]
    Ingestion --> KP[kafka_processor.py]

    style Src fill:#f3e5f5
    style Common fill:#fff9c4
    style Ingestion fill:#e8f5e9
```

---

### `src/ingestion/` - Procesador de Ingesta

#### `kafka_processor.py`

Clase que lee archivos CSV/Parquet y envía mensajes a Kafka.

```python
class KafkaProcessor:
    """Procesa archivos y los envía a Kafka"""

    def __init__(self, bootstrap_servers: str, producer_config: dict):
        # Inicializa productor de Kafka
        self.producer = Producer(config)
        self.admin_client = AdminClient(config)

    def ensure_topic_exists(self, topic_name: str):
        # Crea el topic si no existe
        pass

    def process_csv(self, file_path: str, topic: str, schema_name: str):
        # Lee CSV con Polars y envía a Kafka
        pass

    def process_parquet(self, file_path: str, topic: str, schema_name: str):
        # Lee Parquet y envía a Kafka
        pass

    def process_directory(self, directory: str, topic: str, schema_name: str):
        # Procesa todos los archivos de un directorio
        pass
```

```mermaid
graph LR
    CSV[Archivo CSV] --> Polars[Polars<br/>DataFrame]
    Polars --> Row[Fila por fila]
    Row --> JSON[Convertir a JSON]
    JSON --> Producer[Kafka Producer]
    Producer --> Topic[Kafka Topic]

    style CSV fill:#e3f2fd
    style Topic fill:#fff3e0
```

**Funcionalidades:**
- ✅ Lee archivos CSV con Polars (eficiente para archivos grandes)
- ✅ Lee archivos Parquet
- ✅ Crea topics automáticamente si no existen
- ✅ Convierte cada fila a JSON
- ✅ Envía mensajes con key (para particionamiento)
- ✅ Manejo de errores y reintentos

---

### `src/common/sources/` - Fuentes de Datos

#### `kafka_source.py`

Configura la lectura desde Kafka en Apache Beam.

```python
def create_kafka_source(topic, bootstrap_servers, consumer_config):
    """Crea un ReadFromKafka configurado"""
    return ReadFromKafka(
        consumer_config={
            'bootstrap.servers': bootstrap_servers,
            **consumer_config
        },
        topics=[topic]
    )

class ParseKafkaMessage(beam.DoFn):
    """Parsea mensajes JSON de Kafka"""

    def process(self, element):
        try:
            key, value = element
            data = json.loads(value.decode('utf-8'))
            yield {
                'schema': self.schema_name,
                'data': data,
                'key': key
            }
        except Exception as e:
            # Error → DLQ
            yield beam.pvalue.TaggedOutput('dlq', {...})
```

---

#### `storage_source.py`

Lee archivos directamente sin pasar por Kafka.

```python
def create_storage_source(file_pattern, file_type, schema_name):
    """Crea source que lee desde archivos"""
    # Lee CSV/Parquet directamente
    # Útil para procesamiento batch
    pass
```

---

### `src/common/transforms/` - Transformaciones

Contiene todas las transformaciones del pipeline Apache Beam.

```mermaid
graph LR
    subgraph "Transformaciones"
        N[normalize.py] --> V[validate.py]
        V --> T[timestamp.py]
        T --> W[windowing.py]
        W --> M[metadata.py]
    end

    Input[Datos Raw] --> N
    M --> Output[Datos Procesados]

    V -.->|Error| DLQ[DLQ]
    T -.->|Error| DLQ

    style DLQ fill:#ffcdd2
```

---

#### `normalize.py`

Normaliza datos: maneja nulls, convierte tipos, estandariza formatos.

```python
class NormalizeRecord(beam.DoFn):
    """Normaliza registros"""

    def process(self, element):
        data = element.get('data', {})

        # Manejar nulls y valores vacíos
        normalized = {}
        for key, value in data.items():
            if value is None or value == '' or value == 'null':
                normalized[key] = None
            else:
                normalized[key] = value

        # Convertir tipos si es necesario
        # ...

        yield {**element, 'data': normalized}
```

**Operaciones:**
- ✅ Convertir `null`, `""`, `"null"` → `None`
- ✅ Strip de espacios en strings
- ✅ Conversión de tipos (string → int, etc.)
- ✅ Estandarización de formatos

---

#### `validate.py`

Valida registros contra el schema JSON.

```python
class ValidateSchema(beam.DoFn):
    """Valida registros contra su schema"""

    def __init__(self, schema_dir: str):
        self.schema_dir = schema_dir
        self.schema_loader = None

    def setup(self):
        self.schema_loader = SchemaLoader(self.schema_dir)

    def process(self, element):
        schema_name = element.get('schema')
        data = element.get('data', {})

        is_valid, error = self.schema_loader.validate_record(schema_name, data)

        if is_valid:
            yield element  # Continúa el pipeline
        else:
            # Error → DLQ
            yield beam.pvalue.TaggedOutput('dlq', {
                'error': error,
                'record': element,
                'error_type': 'validation_error',
                'schema': schema_name
            })
```

**Validaciones:**
- ✅ Campos requeridos presentes
- ✅ Tipos de datos correctos
- ✅ Si falla → envía a DLQ

---

#### `timestamp.py`

Asigna timestamps para el procesamiento temporal.

```python
class AssignTimestamp(beam.DoFn):
    """Asigna timestamp a los registros"""

    def __init__(self, timestamp_field: str):
        self.timestamp_field = timestamp_field

    def process(self, element):
        data = element.get('data', {})

        # Obtener valor del campo timestamp
        ts_value = data.get(self.timestamp_field)

        # Parsear timestamp (puede ser int YYYYMMDD o Unix timestamp)
        timestamp = self._parse_timestamp(ts_value)

        # Agregar timestamp al registro
        data['timestamp'] = timestamp

        yield {**element, 'data': data}
```

**Formatos soportados:**
- ✅ Unix timestamp (segundos)
- ✅ Formato YYYYMMDD (20200724)
- ✅ ISO 8601 strings

---

#### `windowing.py`

Aplica ventanas temporales para agrupar datos.

```python
def create_windowing_transform(window_size_seconds, allowed_lateness_seconds):
    """Crea transformación de windowing"""
    return beam.WindowInto(
        beam.window.FixedWindows(window_size_seconds),
        allowed_lateness=Duration(seconds=allowed_lateness_seconds)
    )

class LogWindow(beam.DoFn):
    """Log información de la ventana"""

    def process(self, element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime()
        window_end = window.end.to_utc_datetime()
        logger.info(f"Window: {window_start} - {window_end}")
        yield element
```

**Características:**
- ✅ Ventanas fijas (60s para CASES, 120s para DEMISES)
- ✅ Manejo de datos tardíos (allowed_lateness)
- ✅ Triggers configurables

---

#### `metadata.py`

Agrega metadata del pipeline a cada registro.

```python
class AddMetadata(beam.DoFn):
    """Agrega metadata del pipeline"""

    def __init__(self, pipeline_version: str):
        self.pipeline_version = pipeline_version

    def process(self, element, window=beam.DoFn.WindowParam):
        data = element.get('data', {})

        metadata = {
            'pipeline_version': self.pipeline_version,
            'processed_at': datetime.now().isoformat(),
            'worker_host': socket.gethostname(),
            'window_start': window.start.to_utc_datetime().isoformat(),
            'window_end': window.end.to_utc_datetime().isoformat(),
            'schema': element.get('schema')
        }

        data['metadata'] = metadata
        yield {**element, 'data': data}
```

**Metadata agregada:**
- ✅ `pipeline_version`: Versión del pipeline
- ✅ `processed_at`: Timestamp de procesamiento
- ✅ `worker_host`: Hostname del worker
- ✅ `window_start/end`: Límites de la ventana temporal
- ✅ `schema`: Nombre del schema

---

### `src/common/batching/` - Estrategias de Batching

#### `native_batch.py`

Usa el batching nativo de Apache Beam.

```python
class NativeBatcher:
    """Batching usando BatchElements de Beam"""

    def __init__(self, min_batch_size: int, max_batch_size: int):
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size

    def get_transform(self):
        return beam.BatchElements(
            min_batch_size=self.min_batch_size,
            max_batch_size=self.max_batch_size
        )
```

**Características:**
- ✅ Automático por Apache Beam
- ✅ Configurable min/max batch size
- ✅ Eficiente para la mayoría de casos

---

#### `manual_batch.py`

Batching manual con control total.

```python
class GroupIntoBatches(beam.DoFn):
    """Agrupa elementos en batches manualmente"""

    def __init__(self, batch_size: int, timeout_seconds: int):
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self.buffer = []

    def process(self, element):
        self.buffer.append(element)

        if len(self.buffer) >= self.batch_size:
            batch = self.buffer
            self.buffer = []
            yield batch

    def finish_bundle(self):
        # Emitir batch parcial al final
        if self.buffer:
            yield beam.pvalue.TaggedOutput('main', self.buffer)
```

**Características:**
- ✅ Control total sobre el batching
- ✅ Timeout configurable
- ✅ Manejo de batches parciales

---

### `src/common/sinks/` - Destinos de Datos

#### `mongo_sink.py`

Escribe datos a MongoDB con soporte time-series.

```python
class MongoDBSink(beam.DoFn):
    """Escribe batches a MongoDB"""

    def __init__(self, connection_string: str, database: str, collection_config: dict):
        self.connection_string = connection_string
        self.database_name = database
        self.collection_config = collection_config

    def setup(self):
        self.client = MongoClient(self.connection_string)
        self.db = self.client[self.database_name]
        self._ensure_timeseries_collection()

    def _ensure_timeseries_collection(self):
        """Crea colección time-series si no existe"""
        collection_name = self.collection_config.get('name')
        timeseries_config = self.collection_config.get('timeseries', {})

        self.db.create_collection(
            collection_name,
            timeseries={
                'timeField': timeseries_config.get('timeField', 'timestamp'),
                'metaField': timeseries_config.get('metaField', 'metadata'),
                'granularity': timeseries_config.get('granularity', 'hours')
            }
        )

    def process(self, batch):
        collection = self.db[self.collection_config['name']]

        try:
            # Bulk write
            result = collection.insert_many(batch)
            logger.info(f"Inserted {len(result.inserted_ids)} documents")
        except BulkWriteError as e:
            # Manejo de errores parciales
            logger.error(f"Bulk write error: {e}")
```

**Características:**
- ✅ Crea colección time-series automáticamente
- ✅ Bulk write para eficiencia
- ✅ Manejo de errores parciales (BulkWriteError)
- ✅ Logging detallado

---

#### `dlq_sink.py`

Escribe errores a Dead Letter Queue.

```python
class DLQSink(beam.DoFn):
    """Escribe errores a DLQ"""

    def __init__(self, connection_string: str, database: str, collection: str):
        self.connection_string = connection_string
        self.database_name = database
        self.collection_name = collection

    def setup(self):
        self.client = MongoClient(self.connection_string)
        self.collection = self.client[self.database_name][self.collection_name]

        # Crear índices
        self.collection.create_index('error_type')
        self.collection.create_index('schema')
        self.collection.create_index('timestamp')

    def process(self, error):
        doc = {
            'error': error.get('error'),
            'error_type': error.get('error_type'),
            'timestamp': datetime.now(timezone.utc),
            'schema': error.get('schema'),
            'record': error.get('record')
        }
        self.collection.insert_one(doc)

class CombineDLQErrors(beam.CombineFn):
    """Combina errores de múltiples fuentes"""
    pass
```

**Estructura del documento DLQ:**
```json
{
  "error": "Missing required field: resultado",
  "error_type": "validation_error",
  "timestamp": "2024-01-26T10:30:00Z",
  "schema": "cases",
  "record": { ... }
}
```

---

### `src/common/utils/` - Utilidades

#### `config_loader.py`

Carga y valida archivos de configuración YAML.

```python
class ConfigLoader:
    """Carga configuración desde YAML"""

    def load(self, config_path: str) -> dict:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def validate(self, config: dict) -> bool:
        # Validar estructura requerida
        pass
```

---

#### `schema_loader.py`

Carga schemas JSON y valida registros.

```python
class SchemaLoader:
    """Carga y valida schemas"""

    def __init__(self, schema_dir: str):
        self.schema_dir = schema_dir
        self.schemas = {}

    def load_schema(self, schema_name: str) -> dict:
        """Carga un schema desde JSON"""
        schema_path = Path(self.schema_dir) / f"{schema_name}.json"
        with open(schema_path, 'r') as f:
            return json.load(f)

    def validate_record(self, schema_name: str, record: dict) -> tuple:
        """Valida un registro contra el schema"""
        schema = self.get_schema(schema_name)

        # Verificar campos requeridos
        for field in schema.get('required_fields', []):
            if field not in record or record[field] is None:
                return False, f"Missing required field: {field}"

        # Verificar tipos de datos
        for field, expected_type in schema.get('field_types', {}).items():
            if field in record and record[field] is not None:
                if not self._check_type(record[field], expected_type):
                    return False, f"Invalid type for {field}"

        return True, None
```

---

## Carpeta `datasets/`

Contiene los archivos de datos organizados por schema.

```
datasets/
├── cases/
│   ├── file_0_cases.csv
│   ├── file_1_cases.csv
│   └── ...
└── demises/
    ├── file_0_demises.csv
    └── ...
```

### Ejemplo de datos (`cases/file_0_cases.csv`):

```csv
uuid,fecha_muestra,edad,sexo,institucion,ubigeo_paciente,departamento_paciente,...,resultado
12978089,20200724,26,FEMENINO,PRIVADO,140137,LIMA,...,NEGATIVO
19929263,20200901,27,MASCULINO,PRIVADO,250104,UCAYALI,...,NEGATIVO
```

---

## Diagrama de Dependencias

```mermaid
graph TB
    subgraph "CAPA DE EJECUCIÓN"
        User[Usuario] --> Orch[orchestrator.py]
        User --> Scripts[run_*.sh]
    end

    subgraph "CAPA DE PIPELINES"
        Orch --> CasesI[cases/ingestion.py]
        Orch --> CasesP[cases/pipeline.py]
        Orch --> DemisesI[demises/ingestion.py]
        Orch --> DemisesP[demises/pipeline.py]
    end

    subgraph "CAPA DE CONFIGURACIÓN"
        CasesP --> CConfig[cases/config.yaml]
        CasesP --> CSchema[cases/cases.json]
        CasesI --> CConfig
        DemisesP --> DConfig[demises/config.yaml]
        DemisesP --> DSchema[demises/demises.json]
    end

    subgraph "CAPA DE INGESTA"
        CasesI --> KProc[kafka_processor.py]
        DemisesI --> KProc
        KProc --> Data[datasets/*.csv]
    end

    subgraph "CAPA DE TRANSFORMACIONES"
        CasesP --> KSource[kafka_source.py]
        CasesP --> Norm[normalize.py]
        CasesP --> Val[validate.py]
        CasesP --> TS[timestamp.py]
        CasesP --> Win[windowing.py]
        CasesP --> Meta[metadata.py]
    end

    subgraph "CAPA DE BATCHING"
        CasesP --> NBatch[native_batch.py]
        DemisesP --> MBatch[manual_batch.py]
    end

    subgraph "CAPA DE SINKS"
        CasesP --> MSink[mongo_sink.py]
        CasesP --> DLQSink[dlq_sink.py]
    end

    subgraph "CAPA DE UTILIDADES"
        Val --> SLoader[schema_loader.py]
        CasesP --> CLoader[config_loader.py]
    end

    subgraph "INFRAESTRUCTURA"
        KProc --> Kafka[Kafka]
        KSource --> Kafka
        MSink --> MongoDB[MongoDB]
        DLQSink --> MongoDB
    end

    style Orch fill:#e3f2fd
    style CasesP fill:#f3e5f5
    style MSink fill:#e8f5e9
    style Kafka fill:#fff3e0
    style MongoDB fill:#c8e6c9
```

---

## Resumen de Componentes

| Componente | Archivo | Responsabilidad |
|------------|---------|-----------------|
| **Orquestador** | `orchestrator.py` | Descubre schemas, ejecuta ingestas y pipelines |
| **Ingesta Schema** | `pipelines/X/ingestion.py` | Lee CSV → envía a Kafka |
| **Pipeline Schema** | `pipelines/X/pipeline.py` | Lee Kafka → transforma → escribe MongoDB |
| **Config Schema** | `pipelines/X/config.yaml` | Configuración completa del pipeline |
| **Schema JSON** | `pipelines/X/X.json` | Definición de campos y validación |
| **Kafka Processor** | `src/ingestion/kafka_processor.py` | Lee archivos y produce a Kafka |
| **Kafka Source** | `src/common/sources/kafka_source.py` | Consume de Kafka en Beam |
| **Storage Source** | `src/common/sources/storage_source.py` | Lee archivos directamente |
| **Normalize** | `src/common/transforms/normalize.py` | Normaliza datos |
| **Validate** | `src/common/transforms/validate.py` | Valida contra schema |
| **Timestamp** | `src/common/transforms/timestamp.py` | Asigna timestamps |
| **Windowing** | `src/common/transforms/windowing.py` | Aplica ventanas temporales |
| **Metadata** | `src/common/transforms/metadata.py` | Agrega metadata |
| **Native Batch** | `src/common/batching/native_batch.py` | Batching automático |
| **Manual Batch** | `src/common/batching/manual_batch.py` | Batching manual |
| **MongoDB Sink** | `src/common/sinks/mongo_sink.py` | Escribe a MongoDB |
| **DLQ Sink** | `src/common/sinks/dlq_sink.py` | Escribe errores a DLQ |
| **Config Loader** | `src/common/utils/config_loader.py` | Carga configuración YAML |
| **Schema Loader** | `src/common/utils/schema_loader.py` | Carga y valida schemas |

---

## Flujo de Ejecución Completo

```mermaid
sequenceDiagram
    participant User as Usuario
    participant Orch as orchestrator.py
    participant Ing as ingestion.py
    participant KProc as kafka_processor.py
    participant Kafka as Kafka
    participant Pipe as pipeline.py
    participant Transforms as Transformaciones
    participant Mongo as MongoDB

    User->>Orch: python orchestrator.py --ingest cases
    Orch->>Ing: CasesIngestion.run()
    Ing->>KProc: process_csv()
    KProc->>KProc: Leer CSV con Polars
    loop Cada fila
        KProc->>Kafka: Enviar mensaje JSON
    end
    KProc-->>Ing: Completado
    Ing-->>Orch: N mensajes enviados
    Orch-->>User: ✓ Ingesta completada

    User->>Orch: python orchestrator.py --pipeline cases
    Orch->>Pipe: CasesPipeline.run()
    Pipe->>Kafka: ReadFromKafka
    Kafka-->>Pipe: Mensajes
    Pipe->>Transforms: Normalize → Validate → Timestamp → Window → Metadata → Batch
    Transforms-->>Pipe: Datos procesados
    Pipe->>Mongo: insert_many()
    Mongo-->>Pipe: Success
    Pipe-->>Orch: Pipeline completado
    Orch-->>User: ✓ Datos en MongoDB
```

---

**Última actualización:** 2026-01-26
**Versión:** 1.0.0
