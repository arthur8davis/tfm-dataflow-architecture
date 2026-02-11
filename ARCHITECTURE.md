# Arquitectura Multi-Schema Independiente

## Diseno Actual (Independiente por Schema)

```mermaid
flowchart TB
    subgraph Ahora["Diseno Independiente por Schema"]
        subgraph CasesDir["pipelines/cases/"]
            CC["config.yaml"]
            CJ["cases.json"]
            CP["pipeline.py"]
            CI["ingestion.py"]
        end

        subgraph DemisesDir["pipelines/demises/"]
            DC["config.yaml"]
            DJ["demises.json"]
            DP["pipeline.py"]
            DI["ingestion.py"]
        end

        subgraph HospDir["pipelines/hospitalizations/"]
            HC["config.yaml"]
            HJ["hospitalizations.json"]
            HP["pipeline.py"]
            HI["ingestion.py"]
        end

        Common["src/common/\nsources, transforms\nsinks, batching, utils, data"]

        CasesDir --> Common
        DemisesDir --> Common
        HospDir --> Common
    end

    style Ahora fill:#c8e6c9
    style CasesDir fill:#e3f2fd
    style DemisesDir fill:#e8f5e9
    style HospDir fill:#fff3e0
```

**Ventajas:**
- Cada schema es completamente independiente
- Configuracion aislada por schema
- Se pueden ejecutar en paralelo sin interferencia
- Agregar schemas no afecta los existentes
- Facil de mantener y escalar

---

## Arquitectura Detallada

### Componentes por Schema

Cada schema tiene 4 componentes principales:

```mermaid
flowchart LR
    subgraph SchemaDir["pipelines/{schema}/"]
        ConfigYaml["config.yaml\nConfiguracion completa"]
        SchemaJson["{schema}.json\nValidacion de campos"]
        PipelinePy["pipeline.py\nPipeline Apache Beam"]
        IngestionPy["ingestion.py\nIngesta a Kafka"]
    end

    ConfigYaml --> PipelinePy
    ConfigYaml --> IngestionPy
    SchemaJson --> PipelinePy

    style SchemaDir fill:#e3f2fd
```

#### 1. config.yaml
```yaml
schema:
  name: "cases"

source:
  type: "kafka"
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

#### 2. {schema}.json
Define la estructura de datos especifica del schema:
```json
{
  "schema_name": "cases",
  "required_fields": ["fecha_muestra", "edad", "sexo", "resultado"],
  "field_types": {
    "uuid": "integer",
    "fecha_muestra": "integer",
    "edad": "integer",
    "sexo": "string",
    "resultado": "string"
  },
  "optional_fields": [
    "uuid", "institucion", "ubigeo_paciente",
    "departamento_paciente", "provincia_paciente", "distrito_paciente"
  ]
}
```

#### 3. pipeline.py
Pipeline Apache Beam dedicado:
```python
class CasesPipeline:
    """Pipeline especifico para CASES"""

    def __init__(self, config_path: str = None):
        self.config = self._load_config(config_path)

    def build(self):
        # Construye pipeline: Source -> Normalize -> EnrichGeo -> Validate
        # -> Timestamp -> Window -> Metadata -> Batch -> MongoDB Sink
        pass

    def run(self):
        pipeline = self.build()
        pipeline.run().wait_until_finish()
```

#### 4. ingestion.py
Ingesta dedicada a Kafka:
```python
class CasesIngestion:
    """Ingesta especifica para CASES"""

    def run(self):
        # Lee datos de datasets/cases/
        # Envia al topic "cases"
        # Usa KafkaProcessor (confluent_kafka)
        pass
```

### Componentes Compartidos

Los componentes en `src/common/` son librerias reutilizables sin configuracion:

```mermaid
flowchart TB
    subgraph CommonLib["src/common/"]
        Sources["sources/\nkafka_source_native.py\nstorage_source.py"]
        Transforms["transforms/\nnormalize.py\nvalidate.py\ntimestamp.py\nwindowing.py\nmetadata.py\nenrich_geo.py"]
        Batching["batching/\nmanual_batch.py\nnative_batch.py"]
        Sinks["sinks/\nmongo_sink.py\ndlq_sink.py"]
        Utils["utils/\nconfig_loader.py\nschema_loader.py"]
        Data["data/\nubigeo_coords.py"]
    end

    CasesP["cases/pipeline.py"] --> Sources
    CasesP --> Transforms
    CasesP --> Batching
    CasesP --> Sinks

    DemisesP["demises/pipeline.py"] --> Sources
    DemisesP --> Transforms
    DemisesP --> Batching
    DemisesP --> Sinks

    HospP["hospitalizations/pipeline.py"] --> Sources
    HospP --> Transforms
    HospP --> Batching
    HospP --> Sinks

    style CommonLib fill:#fff9c4
    style CasesP fill:#e3f2fd
    style DemisesP fill:#e8f5e9
    style HospP fill:#fff3e0
```

Estos componentes son invocados por cada pipeline con SU propia configuracion.

### Enriquecimiento Geografico

El transform `enrich_geo.py` usa `ubigeo_coords.py` para convertir codigos UBIGEO peruanos en coordenadas geograficas (lat/lon):

```mermaid
flowchart LR
    Record["Registro con\nubigeo: 150101"] --> EnrichGeo["EnrichGeoFromUbigeo\nenrich_geo.py"]
    EnrichGeo --> UbigeoData["ubigeo_coords.py\nDiccionario UBIGEO -> lat/lon"]
    UbigeoData --> Enriched["Registro enriquecido\nlat: -12.0464\nlon: -77.0428"]

    style Record fill:#e3f2fd
    style EnrichGeo fill:#e0f2f1
    style Enriched fill:#c8e6c9
```

---

## Flujo de Datos por Schema

```mermaid
flowchart TB
    subgraph CasesFlow["Schema CASES"]
        CC_CSV["datasets/cases/\n13 archivos CSV"] --> CC_Ing["Ingestion\ncases/ingestion.py"]
        CC_Ing --> CC_Kafka["Kafka Topic: cases"]
        CC_Kafka --> CC_Pipe["Pipeline\ncases/pipeline.py"]
        CC_Pipe --> CC_Trans["Transforms\nNormalize -> EnrichGeo\n-> Validate -> Timestamp\n-> Window -> Metadata\n-> Batch"]
        CC_Trans --> CC_Mongo["MongoDB: cases"]
    end

    subgraph DemisesFlow["Schema DEMISES"]
        DC_CSV["datasets/demises/\n13 archivos CSV"] --> DC_Ing["Ingestion\ndemises/ingestion.py"]
        DC_Ing --> DC_Kafka["Kafka Topic: demises"]
        DC_Kafka --> DC_Pipe["Pipeline\ndemises/pipeline.py"]
        DC_Pipe --> DC_Trans["Transforms\nNormalize -> EnrichGeo\n-> Validate -> Timestamp\n-> Window -> Metadata\n-> Batch"]
        DC_Trans --> DC_Mongo["MongoDB: demises"]
    end

    subgraph HospFlow["Schema HOSPITALIZATIONS"]
        HC_CSV["datasets/hospitalizations/\n13 archivos CSV"] --> HC_Ing["Ingestion\nhospitalizations/ingestion.py"]
        HC_Ing --> HC_Kafka["Kafka Topic: hospitalizations"]
        HC_Kafka --> HC_Pipe["Pipeline\nhospitalizations/pipeline.py"]
        HC_Pipe --> HC_Trans["Transforms\nNormalize -> EnrichGeo\n-> Validate -> Timestamp\n-> Window -> Metadata\n-> Batch"]
        HC_Trans --> HC_Mongo["MongoDB: hospitalizations"]
    end

    style CasesFlow fill:#e3f2fd
    style DemisesFlow fill:#e8f5e9
    style HospFlow fill:#fff3e0
```

**Nota**: Los tres pipelines corren simultaneamente sin afectarse.

---

## Orquestacion

El `orchestrator.py` descubre y gestiona schemas automaticamente:

```mermaid
flowchart TD
    Orch["orchestrator.py"] --> Discover["Descubrimiento automatico\nEscanear pipelines/"]
    Discover --> Check{"Por cada subdirectorio"}

    Check --> Verify["Verificar:\npipeline.py existe?\nconfig.yaml existe?"]
    Verify --> |"Si"| Register["Registrar como\nschema disponible"]
    Verify --> |"No"| Skip["Ignorar"]

    Register --> Execute{"Accion?"}
    Execute --> |"--list"| List["Listar schemas"]
    Execute --> |"--pipeline X"| RunPipe["Importar dinamicamente\ny ejecutar pipeline"]
    Execute --> |"--ingest X"| RunIng["Ejecutar ingesta"]
    Execute --> |"--parallel"| RunPar["Ejecutar en\nhilos paralelos"]

    style Orch fill:#e3f2fd
    style Register fill:#c8e6c9
```

### Proceso de Descubrimiento

1. Escanea directorio `pipelines/`
2. Por cada subdirectorio:
   - Verifica existencia de `pipeline.py`
   - Verifica existencia de `config.yaml`
   - Lo registra como schema disponible
3. Importa dinamicamente el modulo cuando se necesita
4. Busca clases con patron: `{Schema}Pipeline` y `{Schema}Ingestion`

---

## Infraestructura Docker

```mermaid
flowchart LR
    subgraph Docker["Docker Compose Services"]
        Kafka["Kafka KRaft\napache/kafka:4.1.1\n:9092"]
        MongoDB["MongoDB 8.0\nmongo:8.0.0\n:27017"]
        KafkaUI["Kafka UI\nkafbat/kafka-ui\n:8080"]
        MongoExpress["Mongo Express\n:8083"]
    end

    KafkaUI -.-> Kafka
    MongoExpress -.-> MongoDB

    style Kafka fill:#fff3e0
    style MongoDB fill:#e8f5e9
```

> **Nota**: Kafka usa modo KRaft (sin Zookeeper). El cluster se auto-configura con CLUSTER_ID.

| Servicio | Puerto | Imagen | Credenciales |
|----------|--------|--------|--------------|
| Kafka KRaft | 9092 | apache/kafka:4.1.1 | - |
| MongoDB | 27017 | mongo:8.0.0 | admin / admin123 |
| Kafka UI | 8080 | kafbat/kafka-ui | - |
| Mongo Express | 8083 | mongo-express | - |

---

## Despliegue y Escalamiento

### Despliegue por Schema

```mermaid
flowchart LR
    subgraph Server1["Servidor 1"]
        P1["Pipeline CASES"]
    end

    subgraph Server2["Servidor 2"]
        P2["Pipeline DEMISES"]
    end

    subgraph Server3["Servidor 3"]
        P3["Pipeline HOSPITALIZATIONS"]
    end

    subgraph Server4["Servidor 4"]
        P4["CASES + DEMISES +\nHOSPITALIZATIONS\n--parallel"]
    end

    Kafka["Kafka Cluster"] --> Server1
    Kafka --> Server2
    Kafka --> Server3
    Kafka --> Server4

    Server1 --> MongoDB["MongoDB"]
    Server2 --> MongoDB
    Server3 --> MongoDB
    Server4 --> MongoDB

    style Kafka fill:#fff3e0
    style MongoDB fill:#e8f5e9
```

### Escalamiento Horizontal

```mermaid
flowchart TB
    subgraph Workers["Workers CASES\ngroup.id: beam-pipeline-cases"]
        W1["Worker 1\ncases/pipeline.py"]
        W2["Worker 2\ncases/pipeline.py"]
    end

    subgraph WorkerD["Worker DEMISES\ngroup.id: beam-pipeline-demises"]
        W3["Worker 3\ndemises/pipeline.py"]
    end

    subgraph WorkerH["Worker HOSPITALIZATIONS\ngroup.id: beam-pipeline-hospitalizations"]
        W4["Worker 4\nhospitalizations/pipeline.py"]
    end

    Kafka["Kafka"] --> |"Balanceo\nautomatico"| Workers
    Kafka --> WorkerD
    Kafka --> WorkerH

    style Workers fill:#e3f2fd
    style WorkerD fill:#e8f5e9
    style WorkerH fill:#fff3e0
```

Kafka maneja el balanceo de carga entre workers del mismo consumer group.

---

## Ventajas de Independencia

```mermaid
flowchart TB
    subgraph Fault["1. Aislamiento de Fallos"]
        F1["CASES falla"] --> F2["Solo CASES se detiene"]
        F2 --> F3["DEMISES y HOSPITALIZATIONS\ncontinuan normalmente"]
    end

    subgraph Deploy["2. Despliegue Independiente"]
        D1["Actualizar config de CASES"] --> D2["Reiniciar solo CASES"]
        D2 --> D3["Otros schemas no afectados"]
    end

    subgraph Config["3. Configuracion Flexible"]
        CF1["cases: timestamp=fecha_muestra"]
        CF2["demises: timestamp=fecha_fallecimiento"]
        CF3["hospitalizations: timestamp=fecha_ingreso_hosp"]
    end

    subgraph Scale["4. Escalamiento Granular"]
        SC1["CASES: alto volumen -> 5 workers"]
        SC2["DEMISES: medio volumen -> 2 workers"]
        SC3["HOSPITALIZATIONS: bajo volumen -> 1 worker"]
    end

    style Fault fill:#ffcdd2
    style Deploy fill:#e3f2fd
    style Config fill:#fff3e0
    style Scale fill:#f3e5f5
```

---

## Monitoreo por Schema

Cada schema tiene sus propias metricas:

```mermaid
flowchart TB
    subgraph KafkaMetrics["Kafka - Metricas por Schema"]
        KT1["Topic: cases"]
        KT2["Topic: demises"]
        KT3["Topic: hospitalizations"]
        KG1["Group: beam-pipeline-cases"]
        KG2["Group: beam-pipeline-demises"]
        KG3["Group: beam-pipeline-hospitalizations"]
    end

    subgraph MongoMetrics["MongoDB - Metricas por Schema"]
        MC1["Collection: cases"]
        MC2["Collection: demises"]
        MC3["Collection: hospitalizations"]
        MDLQ["Collection: dead_letter_queue\nErrores por schema"]
    end

    style KafkaMetrics fill:#fff3e0
    style MongoMetrics fill:#e8f5e9
```

### Dead Letter Queue
```javascript
// Errores por schema
db.dead_letter_queue.aggregate([
  {$group: {
    _id: "$schema",
    count: {$sum: 1}
  }}
])
```

---

## Visualizacion en Tiempo Real

El directorio `visualization/` contiene un dashboard Flask + Socket.IO:

```mermaid
flowchart TB
    subgraph Dashboard["Dashboard (visualization/)")
        Flask["Flask + Socket.IO\nPort: 5006"]
        D3["D3.js Charts\nBarras, Area, Donut"]
        Leaflet["Leaflet.js Maps\nMapas de calor"]
        Polling["Polling cada 3s\nDetecta cambios"]
    end

    MongoDB["MongoDB\ncases, demises,\nhospitalizations"] --> Polling
    Polling --> Flask
    Flask --> D3
    Flask --> Leaflet

    style Dashboard fill:#e0f7fa
    style MongoDB fill:#e8f5e9
```

- 9 visualizaciones D3.js + Leaflet
- Actualizacion en tiempo real via WebSockets
- Filtros interactivos por departamento y sexo
- Sistema de alertas con umbrales configurables

---

**Ultima actualizacion:** 2026-02-10
**Version:** 2.0.0
