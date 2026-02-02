# Pipeline de Procesamiento Multi-Schema con Apache Beam

Arquitectura de procesamiento de datos en tiempo real usando Apache Beam, Confluent Kafka y MongoDB con time series collections. **Cada schema se procesa de manera completamente independiente.**

## Arquitectura

```mermaid
flowchart TB
    subgraph Cases["SCHEMA: CASES"]
        CSV1["CSV Files"] --> Kafka1["Kafka Topic\ncases"]
        Kafka1 --> Beam1["Apache Beam Pipeline"]
        Beam1 --> Mongo1["MongoDB Collection\ncases (Time-Series)"]
    end

    subgraph Demises["SCHEMA: DEMISES"]
        CSV2["CSV Files"] --> Kafka2["Kafka Topic\ndemises"]
        Kafka2 --> Beam2["Apache Beam Pipeline"]
        Beam2 --> Mongo2["MongoDB Collection\ndemises (Time-Series)"]
    end

    subgraph NewSchema["SCHEMA: [TU SCHEMA]"]
        CSV3["CSV Files"] --> Kafka3["Kafka Topic\nmi_schema"]
        Kafka3 --> Beam3["Apache Beam Pipeline"]
        Beam3 --> Mongo3["MongoDB Collection\nmi_schema (Time-Series)"]
    end

    style Cases fill:#e3f2fd
    style Demises fill:#e8f5e9
    style NewSchema fill:#fff3e0
```

### Caractersticas Clave

- **Independencia por Schema**: Cada schema tiene su propio pipeline, configuracin y proceso
- **Ejecucin Paralela**: Mltiples schemas pueden procesarse simultneamente
- **Configuracin Aislada**: Cada schema puede tener diferentes ventanas, batching, etc.
- **Escalabilidad**: Agregar nuevos schemas es trivial

## Estructura del Proyecto

```mermaid
flowchart TB
    Root["tfm/"] --> Pipelines["pipelines/"]
    Root --> Src["src/"]
    Root --> Datasets["datasets/"]
    Root --> Orch["orchestrator.py"]
    Root --> Docker["docker-compose.yaml"]

    subgraph PipelinesDetail["Pipelines por Schema"]
        Pipelines --> Cases["cases/\nconfig.yaml, schema.json\npipeline.py, ingestion.py"]
        Pipelines --> Demises["demises/\nconfig.yaml, schema.json\npipeline.py, ingestion.py"]
    end

    subgraph SrcDetail["Cdigo Fuente"]
        Src --> Common["common/"]
        Common --> Sources["sources/\nkafka_source.py\nstorage_source.py"]
        Common --> Transforms["transforms/\nnormalize, validate\ntimestamp, windowing\nmetadata"]
        Common --> Batching["batching/\nnative_batch.py\nmanual_batch.py"]
        Common --> Sinks["sinks/\nmongo_sink.py\ndlq_sink.py"]
        Common --> Utils["utils/\nconfig_loader.py\nschema_loader.py"]
        Src --> Ingestion["ingestion/\nkafka_processor.py"]
    end

    subgraph DatasetsDetail["Datos"]
        Datasets --> DCases["cases/\nsample_data.csv"]
        Datasets --> DDemises["demises/\nsample_data.csv"]
    end

    style Root fill:#e3f2fd
    style Pipelines fill:#fff3e0
    style Src fill:#f3e5f5
    style Datasets fill:#e8f5e9
```

## Instalacin

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 2. Iniciar servicios compartidos

```bash
docker-compose up -d
```

```mermaid
flowchart LR
    subgraph Docker["Docker Compose"]
        Kafka["Kafka\n:9092"]
        MongoDB["MongoDB\n:27017"]
        KafkaUI["Kafka UI\n:8080"]
        MongoExpress["Mongo Express\n:8083"]
    end

    KafkaUI -.-> |monitor| Kafka
    MongoExpress -.-> |admin| MongoDB

    style Kafka fill:#fff3e0
    style MongoDB fill:#e8f5e9
    style KafkaUI fill:#f1f8e9
    style MongoExpress fill:#f1f8e9
```

## Uso

### Opciones de Ejecucin

```mermaid
flowchart TD
    Start(["Ejecutar"]) --> Choice{"Mtodo?"}

    Choice --> |"Opcin 1"| Scripts["Scripts Individuales\nrun_cases.sh\nrun_demises.sh"]
    Choice --> |"Opcin 2"| Direct["Ejecucin Directa\npython pipelines/X/..."]
    Choice --> |"Opcin 3"| Orchestrator["Orquestador\norchestrator.py"]

    Scripts --> S1["./run_cases.sh ingest"]
    Scripts --> S2["./run_cases.sh pipeline"]
    Scripts --> S3["./run_cases.sh both"]

    Direct --> D1["python pipelines/cases/ingestion.py"]
    Direct --> D2["python pipelines/cases/pipeline.py --mode streaming"]

    Orchestrator --> O1["--ingest cases"]
    Orchestrator --> O2["--pipeline cases demises --parallel"]
    Orchestrator --> O3["--pipeline-all --parallel"]

    style Start fill:#e3f2fd
    style Scripts fill:#fff3e0
    style Direct fill:#f3e5f5
    style Orchestrator fill:#e8f5e9
```

### Opcin 1: Scripts Individuales por Schema

```bash
# CASES
./run_cases.sh ingest      # Solo ingesta
./run_cases.sh pipeline    # Solo pipeline
./run_cases.sh both        # Ingesta + pipeline

# DEMISES
./run_demises.sh ingest
./run_demises.sh pipeline
./run_demises.sh both
```

### Opcin 2: Ejecucin Directa

```bash
# Ejecutar ingesta de cases
python pipelines/cases/ingestion.py

# Ejecutar pipeline de cases (streaming desde Kafka)
python pipelines/cases/pipeline.py --mode streaming

# Ejecutar pipeline de cases (batch desde archivos)
python pipelines/cases/pipeline.py --mode batch

# Ejecutar ingesta de demises
python pipelines/demises/ingestion.py

# Ejecutar pipeline de demises
python pipelines/demises/pipeline.py --mode streaming
```

### Opcin 3: Orquestador (Recomendado para mltiples schemas)

```bash
# Listar schemas disponibles
python orchestrator.py --list

# Ejecutar pipeline de un schema
python orchestrator.py --pipeline cases

# Ejecutar ingesta de un schema
python orchestrator.py --ingest cases

# Ejecutar mltiples pipelines EN PARALELO
python orchestrator.py --pipeline cases demises --parallel

# Ejecutar TODOS los pipelines en paralelo
python orchestrator.py --pipeline-all --parallel

# Ejecutar TODAS las ingests en paralelo
python orchestrator.py --ingest-all --parallel

# Ingestar archivo especfico
python orchestrator.py --ingest cases --file datasets/cases/data.csv
```

## Agregar un Nuevo Schema

### Proceso

```mermaid
flowchart TD
    Start(["Nuevo Schema"]) --> Step1["1. Crear directorios\nmkdir -p pipelines/mi_schema\nmkdir -p datasets/mi_schema"]
    Step1 --> Step2["2. Copiar plantilla\ncp pipelines/cases/* pipelines/mi_schema/"]
    Step2 --> Step3["3. Editar config.yaml\nname, topic, group.id, collection"]
    Step3 --> Step4["4. Editar schema.json\nCampos y tipos"]
    Step4 --> Step5["5. Editar pipeline.py\nCambiar nombre de clase"]
    Step5 --> Step6["6. Editar ingestion.py\nCambiar nombre de clase"]
    Step6 --> Step7["7. Agregar datos CSV\ndatasets/mi_schema/"]
    Step7 --> Step8["8. Ejecutar\norchestrator.py --ingest/--pipeline"]
    Step8 --> End(["Schema Listo"])

    style Start fill:#e3f2fd
    style End fill:#c8e6c9
```

### Opcin 1: Manual

1. **Crear directorio del schema**

```bash
mkdir -p pipelines/mi_schema
mkdir -p datasets/mi_schema
```

2. **Copiar plantilla desde un schema existente**

```bash
cp pipelines/cases/config.yaml pipelines/mi_schema/
cp pipelines/cases/schema.json pipelines/mi_schema/
cp pipelines/cases/pipeline.py pipelines/mi_schema/
cp pipelines/cases/ingestion.py pipelines/mi_schema/
```

3. **Editar archivos**

`pipelines/mi_schema/config.yaml`:
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
  storage:
    file_pattern: "datasets/mi_schema/*.csv"

# ... resto de configuracin
```

`pipelines/mi_schema/schema.json`:
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

4. **Actualizar nombres de clases en pipeline.py e ingestion.py**

```python
# En pipeline.py
class MiSchemaPipeline:
    """Pipeline para procesar datos de MI_SCHEMA"""
    ...

# En ingestion.py
class MiSchemaIngestion:
    """Ingesta de datos para el schema MI_SCHEMA"""
    ...
```

5. **Crear script de ejecucin (opcional)**

```bash
cp run_cases.sh run_mi_schema.sh
# Editar y cambiar referencias de CASES a MI_SCHEMA
chmod +x run_mi_schema.sh
```

6. **Agregar datos**

```bash
# Colocar archivos CSV/Parquet en datasets/mi_schema/
cp mi_data.csv datasets/mi_schema/
```

7. **Ejecutar**

```bash
python orchestrator.py --ingest mi_schema
python orchestrator.py --pipeline mi_schema
```

## Configuracin Independiente por Schema

Cada schema puede tener configuracin completamente diferente:

```mermaid
flowchart LR
    subgraph CasesConfig["cases/config.yaml"]
        C1["window: 60s"]
        C2["batch: native, 100"]
        C3["topic: cases"]
    end

    subgraph DemisesConfig["demises/config.yaml"]
        D1["window: 120s"]
        D2["batch: manual, 50"]
        D3["topic: demises"]
    end

    style CasesConfig fill:#e3f2fd
    style DemisesConfig fill:#e8f5e9
```

**Ejemplo: cases/config.yaml**
```yaml
windowing:
  window_size_seconds: 60      # Ventanas de 1 minuto
batching:
  strategy: "native"           # Batching nativo
  batch_size: 100
```

**Ejemplo: demises/config.yaml**
```yaml
windowing:
  window_size_seconds: 120     # Ventanas de 2 minutos (diferente!)
batching:
  strategy: "manual"           # Batching manual (diferente!)
  batch_size: 50               # Batches ms pequeos (diferente!)
```

## Ejecucin en Paralelo

```mermaid
flowchart LR
    Orch["orchestrator.py\n--parallel"] --> Cases["Pipeline CASES\nTopic: cases\nGroup: beam-pipeline-cases"]
    Orch --> Demises["Pipeline DEMISES\nTopic: demises\nGroup: beam-pipeline-demises"]

    Cases --> MongoDB1["MongoDB\ncases collection"]
    Demises --> MongoDB2["MongoDB\ndemises collection"]

    style Orch fill:#e3f2fd
    style Cases fill:#fff3e0
    style Demises fill:#e8f5e9
```

Para procesar mltiples schemas simultneamente:

```bash
# Terminal 1: Ingestar todos los schemas en paralelo
python orchestrator.py --ingest-all --parallel

# Terminal 2: Ejecutar todos los pipelines en paralelo
python orchestrator.py --pipeline-all --parallel
```

O ejecutar schemas especficos en paralelo:

```bash
python orchestrator.py --pipeline cases demises --parallel
```

## Monitoreo

```mermaid
flowchart TB
    subgraph Monitoring["Herramientas de Monitoreo"]
        KafkaUI["Kafka UI\nhttp://localhost:8080\nTopics, mensajes, consumer lag"]
        MongoExpress["Mongo Express\nhttp://localhost:8083\nColecciones, queries, DLQ"]
    end

    KafkaUI -.-> Kafka["Kafka\nTopics: cases, demises"]
    MongoExpress -.-> MongoDB["MongoDB\nCollections: cases, demises,\ndead_letter_queue"]

    style KafkaUI fill:#fff3e0
    style MongoExpress fill:#f3e5f5
```

### Kafka UI
- URL: http://localhost:8080
- Ver topics por schema: `cases`, `demises`, etc.

### Mongo Express
- URL: http://localhost:8083
- Usuario: admin
- Password: admin123
- Colecciones: `cases`, `demises`, `dead_letter_queue`

### Consultas MongoDB

```javascript
use("covid-db");

// Datos de cases
db.cases.find().limit(10);

// Datos de demises
db.demises.find().limit(10);

// Errores en DLQ por schema
db.dead_letter_queue.aggregate([
  {$group: {_id: "$schema", count: {$sum: 1}}}
]);
```

## Ventajas de esta Arquitectura

```mermaid
mindmap
  root((Arquitectura\nMulti-Schema))
    Aislamiento
      Un error en un schema no afecta a otros
      Testing independiente
    Configuracin
      Ventanas diferentes por schema
      Batching personalizado
      Topics exclusivos
    Escalabilidad
      Agregar schemas sin afectar existentes
      Workers independientes por schema
    Desarrollo
      Equipos paralelos
      Despliegue granular
```

1. **Aislamiento Total**: Un error en un schema no afecta a otros
2. **Configuracin Flexible**: Cada schema puede tener diferentes ventanas, batching, etc.
3. **Escalabilidad Horizontal**: Agregar schemas no afecta a los existentes
4. **Desarrollo Paralelo**: Mltiples equipos pueden trabajar en diferentes schemas
5. **Testing Independiente**: Probar un schema no requiere los dems
6. **Despliegue Granular**: Actualizar un schema sin tocar otros

## Ejemplo de Flujo Completo

```mermaid
sequenceDiagram
    actor Usuario
    participant Orch as orchestrator.py
    participant Kafka
    participant Pipeline as Apache Beam
    participant MongoDB

    Usuario->>Orch: --list
    Orch-->>Usuario: cases, demises

    Usuario->>Orch: --ingest cases demises --parallel
    Orch->>Kafka: Ingestar cases
    Orch->>Kafka: Ingestar demises
    Kafka-->>Orch: Datos en topics

    Usuario->>Orch: --pipeline cases demises --parallel
    Orch->>Pipeline: Pipeline cases
    Orch->>Pipeline: Pipeline demises

    Pipeline->>Kafka: Consumir mensajes
    Pipeline->>Pipeline: Transformaciones
    Pipeline->>MongoDB: Escribir datos

    Usuario->>MongoDB: Verificar en Mongo Express
    MongoDB-->>Usuario: Datos disponibles
```

```bash
# 1. Ver schemas disponibles
python orchestrator.py --list

# 2. Ingestar datos de cases y demises en paralelo
python orchestrator.py --ingest cases demises --parallel

# 3. En otra terminal, ejecutar los pipelines en paralelo
python orchestrator.py --pipeline cases demises --parallel

# 4. Monitorear en Mongo Express
# Abrir http://localhost:8083

# 5. Agregar un nuevo schema
mkdir -p pipelines/recovered datasets/recovered
cp -r pipelines/cases/* pipelines/recovered/
# Editar pipelines/recovered/*

# 6. Ejecutar el nuevo schema
python orchestrator.py --ingest recovered
python orchestrator.py --pipeline recovered
```

## Troubleshooting

```mermaid
flowchart TD
    Problem(["Problema"]) --> Type{"Tipo?"}

    Type --> |"Schema no encontrado"| S1["Verificar:\nls pipelines/mi_schema/\nDebe tener: config.yaml,\nschema.json, pipeline.py,\ningestion.py"]

    Type --> |"Kafka conexin"| S2["docker-compose ps\ndocker-compose restart kafka"]

    Type --> |"Logs"| S3["python orchestrator.py\n--pipeline cases 2>&1\n| tee cases.log"]

    style Problem fill:#ffcdd2
    style S1 fill:#c8e6c9
    style S2 fill:#c8e6c9
    style S3 fill:#c8e6c9
```

### Error: Schema no encontrado

```bash
# Verificar que existe el directorio
ls pipelines/

# Verificar que tiene los archivos necesarios
ls pipelines/mi_schema/
# Debe tener: config.yaml, schema.json, pipeline.py, ingestion.py
```

### Error: No se puede conectar a Kafka

```bash
# Verificar servicios
docker-compose ps

# Reiniciar servicios
docker-compose restart kafka
```

### Ver logs de un schema especfico

```bash
python orchestrator.py --pipeline cases 2>&1 | tee cases.log
```

## Prximos Pasos

- Configurar alertas por schema
- Agregar mtricas con Prometheus
- Implementar retry policies personalizadas
- Escalar con DataflowRunner en GCP
- Agregar tests por schema
