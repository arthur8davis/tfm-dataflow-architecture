# Pipeline de Procesamiento Multi-Schema con Apache Beam

Arquitectura de procesamiento de datos en tiempo real usando Apache Beam, Confluent Kafka (KRaft) y MongoDB con time series collections. **Cada schema se procesa de manera completamente independiente.**

## Arquitectura

```mermaid
flowchart TB
    subgraph Cases["SCHEMA: CASES"]
        CSV1["CSV Files<br/>13 archivos"] --> Kafka1["Kafka Topic\ncases"]
        Kafka1 --> Beam1["Apache Beam Pipeline"]
        Beam1 --> Mongo1["MongoDB Collection\ncases (Time-Series)"]
    end

    subgraph Demises["SCHEMA: DEMISES"]
        CSV2["CSV Files<br/>13 archivos"] --> Kafka2["Kafka Topic\ndemises"]
        Kafka2 --> Beam2["Apache Beam Pipeline"]
        Beam2 --> Mongo2["MongoDB Collection\ndemises (Time-Series)"]
    end

    subgraph Hospitalizations["SCHEMA: HOSPITALIZATIONS"]
        CSV3["CSV Files<br/>13 archivos"] --> Kafka3["Kafka Topic\nhospitalizations"]
        Kafka3 --> Beam3["Apache Beam Pipeline"]
        Beam3 --> Mongo3["MongoDB Collection\nhospitalizations (Time-Series)"]
    end

    subgraph NewSchema["SCHEMA: [TU SCHEMA]"]
        CSV4["CSV Files"] --> Kafka4["Kafka Topic\nmi_schema"]
        Kafka4 --> Beam4["Apache Beam Pipeline"]
        Beam4 --> Mongo4["MongoDB Collection\nmi_schema (Time-Series)"]
    end

    style Cases fill:#e3f2fd
    style Demises fill:#e8f5e9
    style Hospitalizations fill:#fff3e0
    style NewSchema fill:#f3e5f5
```

### Caracteristicas Clave

- **Independencia por Schema**: Cada schema tiene su propio pipeline, configuracion y proceso
- **Ejecucion Paralela**: Multiples schemas pueden procesarse simultaneamente
- **Configuracion Aislada**: Cada schema puede tener diferentes ventanas, batching, etc.
- **Escalabilidad**: Agregar nuevos schemas es trivial
- **Enriquecimiento Geografico**: Coordenadas lat/lon desde codigos UBIGEO peruanos
- **Dashboard en Tiempo Real**: Visualizaciones D3.js + Leaflet en `visualization/`

## Estructura del Proyecto

```mermaid
flowchart TB
    Root["tfm-dataflow-architecture/"] --> Pipelines["pipelines/"]
    Root --> Src["src/"]
    Root --> Datasets["datasets/"]
    Root --> Viz["visualization/"]
    Root --> Orch["orchestrator.py"]
    Root --> Docker["docker-compose.yaml"]

    subgraph PipelinesDetail["Pipelines por Schema"]
        Pipelines --> Cases["cases/\nconfig.yaml, cases.json\npipeline.py, ingestion.py"]
        Pipelines --> Demises["demises/\nconfig.yaml, demises.json\npipeline.py, ingestion.py"]
        Pipelines --> Hospitals["hospitalizations/\nconfig.yaml, hospitalizations.json\npipeline.py, ingestion.py"]
    end

    subgraph SrcDetail["Codigo Fuente"]
        Src --> Common["common/"]
        Common --> Sources["sources/\nkafka_source_native.py\nstorage_source.py"]
        Common --> Transforms["transforms/\nnormalize, validate\ntimestamp, windowing\nmetadata, enrich_geo"]
        Common --> Batching["batching/\nnative_batch.py\nmanual_batch.py"]
        Common --> Sinks["sinks/\nmongo_sink.py\ndlq_sink.py"]
        Common --> Utils["utils/\nconfig_loader.py\nschema_loader.py"]
        Common --> Data["data/\nubigeo_coords.py"]
        Src --> Ingestion["ingestion/\nkafka_processor.py"]
    end

    subgraph DatasetsDetail["Datos"]
        Datasets --> DCases["cases/\n13 archivos CSV"]
        Datasets --> DDemises["demises/\n13 archivos CSV"]
        Datasets --> DHospital["hospitalizations/\n13 archivos CSV"]
    end

    subgraph VizDetail["Visualizacion"]
        Viz --> VizApp["Flask + Socket.IO\nD3.js + Leaflet\nPort: 5006"]
    end

    style Root fill:#e3f2fd
    style Pipelines fill:#fff3e0
    style Src fill:#f3e5f5
    style Datasets fill:#e8f5e9
    style Viz fill:#e0f7fa
```

## Instalacion

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
        Kafka["Kafka KRaft\n:9092"]
        MongoDB["MongoDB 8.0\n:27017"]
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

> **Nota**: Kafka usa modo KRaft (sin Zookeeper). El cluster se auto-configura.

## Uso

### Opciones de Ejecucion

```mermaid
flowchart TD
    Start(["Ejecutar"]) --> Choice{"Metodo?"}

    Choice --> |"Opcion 1"| Scripts["Scripts Individuales\nrun_cases.sh\nrun_deaths.sh"]
    Choice --> |"Opcion 2"| Direct["Ejecucion Directa\npython pipelines/X/..."]
    Choice --> |"Opcion 3"| Orchestrator["Orquestador\norchestrator.py"]

    Scripts --> S1["./run_cases.sh ingest"]
    Scripts --> S2["./run_cases.sh pipeline"]
    Scripts --> S3["./run_cases.sh both"]

    Direct --> D1["python pipelines/cases/ingestion.py"]
    Direct --> D2["python pipelines/cases/pipeline.py --mode streaming"]

    Orchestrator --> O1["--ingest cases"]
    Orchestrator --> O2["--pipeline cases demises hospitalizations --parallel"]
    Orchestrator --> O3["--pipeline-all --parallel"]

    style Start fill:#e3f2fd
    style Scripts fill:#fff3e0
    style Direct fill:#f3e5f5
    style Orchestrator fill:#e8f5e9
```

### Opcion 1: Scripts Individuales por Schema

```bash
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

# Ejecutar ingesta de hospitalizations
python pipelines/hospitalizations/ingestion.py

# Ejecutar pipeline de hospitalizations
python pipelines/hospitalizations/pipeline.py --mode streaming
```

### Opcion 3: Orquestador (Recomendado para multiples schemas)

```bash
# Listar schemas disponibles
python orchestrator.py --list

# Ejecutar pipeline de un schema
python orchestrator.py --pipeline cases

# Ejecutar ingesta de un schema
python orchestrator.py --ingest cases

# Ejecutar multiples pipelines EN PARALELO
python orchestrator.py --pipeline cases demises hospitalizations --parallel

# Ejecutar TODOS los pipelines en paralelo
python orchestrator.py --pipeline-all --parallel

# Ejecutar TODAS las ingests en paralelo
python orchestrator.py --ingest-all --parallel

# Ingestar archivo especifico
python orchestrator.py --ingest cases --file datasets/cases/file_0_cases.csv
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

### Pasos Detallados

1. **Crear directorio del schema**

```bash
mkdir -p pipelines/mi_schema
mkdir -p datasets/mi_schema
```

2. **Copiar plantilla desde un schema existente**

```bash
cp pipelines/cases/config.yaml pipelines/mi_schema/
cp pipelines/cases/cases.json pipelines/mi_schema/mi_schema.json
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
```

`pipelines/mi_schema/mi_schema.json`:
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

5. **Agregar datos y ejecutar**

```bash
cp tus_datos.csv datasets/mi_schema/
python orchestrator.py --ingest mi_schema
python orchestrator.py --pipeline mi_schema
```

## Configuracion Independiente por Schema

Cada schema puede tener configuracion completamente diferente:

```mermaid
flowchart LR
    subgraph CasesConfig["cases/config.yaml"]
        C1["window: 60s"]
        C2["batch: native, 100"]
        C3["topic: cases"]
        C4["timestamp: fecha_muestra"]
    end

    subgraph DemisesConfig["demises/config.yaml"]
        D1["window: 60s"]
        D2["batch: native, 100"]
        D3["topic: demises"]
        D4["timestamp: fecha_fallecimiento"]
    end

    subgraph HospConfig["hospitalizations/config.yaml"]
        H1["window: 60s"]
        H2["batch: native, 100"]
        H3["topic: hospitalizations"]
        H4["timestamp: fecha_ingreso_hosp"]
    end

    style CasesConfig fill:#e3f2fd
    style DemisesConfig fill:#e8f5e9
    style HospConfig fill:#fff3e0
```

## Ejecucion en Paralelo

```mermaid
flowchart LR
    Orch["orchestrator.py\n--parallel"] --> Cases["Pipeline CASES\nTopic: cases"]
    Orch --> Demises["Pipeline DEMISES\nTopic: demises"]
    Orch --> Hospitals["Pipeline HOSPITALIZATIONS\nTopic: hospitalizations"]

    Cases --> MongoDB1["MongoDB\ncases collection"]
    Demises --> MongoDB2["MongoDB\ndemises collection"]
    Hospitals --> MongoDB3["MongoDB\nhospitalizations collection"]

    style Orch fill:#e3f2fd
    style Cases fill:#fff3e0
    style Demises fill:#e8f5e9
    style Hospitals fill:#f3e5f5
```

```bash
# Ingestar todos los schemas en paralelo
python orchestrator.py --ingest-all --parallel

# Ejecutar todos los pipelines en paralelo
python orchestrator.py --pipeline-all --parallel
```

## Monitoreo

```mermaid
flowchart TB
    subgraph Monitoring["Herramientas de Monitoreo"]
        KafkaUI["Kafka UI\nhttp://localhost:8080\nTopics, mensajes, consumer lag"]
        MongoExpress["Mongo Express\nhttp://localhost:8083\nColecciones, queries, DLQ"]
        Dashboard["Dashboard D3.js\nhttp://localhost:5006\nVisualizaciones en tiempo real"]
    end

    KafkaUI -.-> Kafka["Kafka\nTopics: cases, demises, hospitalizations"]
    MongoExpress -.-> MongoDB["MongoDB\nCollections: cases, demises,\nhospitalizations, dead_letter_queue"]
    Dashboard -.-> MongoDB

    style KafkaUI fill:#fff3e0
    style MongoExpress fill:#f3e5f5
    style Dashboard fill:#e0f7fa
```

### Kafka UI
- URL: http://localhost:8080
- Ver topics por schema: `cases`, `demises`, `hospitalizations`

### Mongo Express
- URL: http://localhost:8083
- Colecciones: `cases`, `demises`, `hospitalizations`, `dead_letter_queue`

### Dashboard D3.js
- URL: http://localhost:5006
- Visualizaciones en tiempo real con WebSockets
- Mapas de calor geograficos con Leaflet

### Consultas MongoDB

```javascript
use("covid-db");

// Datos de cases
db.cases.find().limit(10);

// Datos de demises
db.demises.find().limit(10);

// Datos de hospitalizations
db.hospitalizations.find().limit(10);

// Errores en DLQ por schema
db.dead_letter_queue.aggregate([
  {$group: {_id: "$schema", count: {$sum: 1}}}
]);
```

## Ejemplo de Flujo Completo

```mermaid
sequenceDiagram
    actor Usuario
    participant Orch as orchestrator.py
    participant Kafka
    participant Pipeline as Apache Beam
    participant MongoDB

    Usuario->>Orch: --list
    Orch-->>Usuario: cases, demises, hospitalizations

    Usuario->>Orch: --ingest cases demises hospitalizations --parallel
    Orch->>Kafka: Ingestar cases (13 CSVs)
    Orch->>Kafka: Ingestar demises (13 CSVs)
    Orch->>Kafka: Ingestar hospitalizations (13 CSVs)
    Kafka-->>Orch: Datos en topics

    Usuario->>Orch: --pipeline cases demises hospitalizations --parallel
    Orch->>Pipeline: Pipeline cases
    Orch->>Pipeline: Pipeline demises
    Orch->>Pipeline: Pipeline hospitalizations

    Pipeline->>Kafka: Consumir mensajes
    Pipeline->>Pipeline: Transformaciones + Enrich Geo
    Pipeline->>MongoDB: Escribir datos

    Usuario->>MongoDB: Verificar en Mongo Express / Dashboard
    MongoDB-->>Usuario: Datos disponibles
```

```bash
# 1. Ver schemas disponibles
python orchestrator.py --list

# 2. Ingestar datos en paralelo
python orchestrator.py --ingest-all --parallel

# 3. Ejecutar pipelines en paralelo
python orchestrator.py --pipeline-all --parallel

# 4. Monitorear en Mongo Express
# Abrir http://localhost:8083

# 5. Ver dashboard en tiempo real
cd visualization && python app.py
# Abrir http://localhost:5006
```

## Troubleshooting

### Error: Schema no encontrado

```bash
# Verificar que existe el directorio
ls pipelines/

# Verificar que tiene los archivos necesarios
ls pipelines/mi_schema/
# Debe tener: config.yaml, {schema}.json, pipeline.py, ingestion.py
```

### Error: No se puede conectar a Kafka

```bash
docker-compose ps
docker-compose restart kafka
docker-compose logs kafka
```

### Ver logs de un schema especifico

```bash
python orchestrator.py --pipeline cases 2>&1 | tee cases.log
```

---

**Ultima actualizacion:** 2026-02-10
**Version:** 2.0.0
