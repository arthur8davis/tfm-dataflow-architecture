# Arquitectura Multi-Schema Independiente

## Comparacin: Antes vs Ahora

### Diseo Anterior (Centralizado)

```mermaid
flowchart TB
    subgraph Antes["Diseo Centralizado"]
        Config["config/\nkafka_config.yaml\nmongo_config.yaml\nbeam_config.yaml"]
        Schemas["schemas/\ncases.json\ndeaths.json"]
        Pipeline["src/pipeline/\nmain.py (nico)"]

        Config --> Pipeline
        Schemas --> Pipeline
    end

    style Antes fill:#ffcdd2
```

**Problemas:**
- Un solo pipeline procesa todos los schemas
- Configuracin compartida entre schemas
- Difcil personalizar por schema
- Ejecutar un schema requiere el cdigo de todos
- No hay verdadera independencia

### Diseo Actual (Independiente por Schema)

```mermaid
flowchart TB
    subgraph Ahora["Diseo Independiente"]
        subgraph CasesDir["pipelines/cases/"]
            CC["config.yaml"]
            CJ["schema.json"]
            CP["pipeline.py"]
            CI["ingestion.py"]
        end

        subgraph DemisesDir["pipelines/demises/"]
            DC["config.yaml"]
            DJ["schema.json"]
            DP["pipeline.py"]
            DI["ingestion.py"]
        end

        Common["src/common/\nsources, transforms\nsinks, batching, utils"]

        CasesDir --> Common
        DemisesDir --> Common
    end

    style Ahora fill:#c8e6c9
    style CasesDir fill:#e3f2fd
    style DemisesDir fill:#e8f5e9
```

**Ventajas:**
- Cada schema es completamente independiente
- Configuracin aislada por schema
- Se pueden ejecutar en paralelo sin interferencia
- Agregar schemas no afecta los existentes
- Fcil de mantener y escalar

---

## Arquitectura Detallada

### Componentes por Schema

Cada schema tiene 4 componentes principales:

```mermaid
flowchart LR
    subgraph SchemaDir["pipelines/{schema}/"]
        ConfigYaml["config.yaml\nConfiguracin completa"]
        SchemaJson["schema.json\nValidacin de campos"]
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
    topic: "cases"           # Topic exclusivo
    consumer_config:
      group.id: "beam-pipeline-cases"  # Grupo exclusivo

transforms:
  windowing:
    window_size_seconds: 60  # Ventana especfica

batching:
  strategy: "native"         # Estrategia especfica
  batch_size: 100            # Tamao especfico

sink:
  mongodb:
    collection:
      name: "cases"          # Coleccin exclusiva
```

#### 2. schema.json
Define la estructura de datos especfica del schema:
```json
{
  "schema_name": "cases",
  "required_fields": ["id", "date", "country", "cases"],
  "field_types": {
    "id": "string",
    "date": "string",
    "country": "string",
    "cases": "integer"
  }
}
```

#### 3. pipeline.py
Pipeline Apache Beam dedicado:
```python
class CasesPipeline:
    """Pipeline especfico para CASES"""

    def __init__(self):
        # Carga su propia configuracin
        self.config = self._load_config("pipelines/cases/config.yaml")

    def build(self):
        # Construye pipeline usando su configuracin
        # Totalmente independiente de otros schemas
        pass
```

#### 4. ingestion.py
Ingesta dedicada a Kafka:
```python
class CasesIngestion:
    """Ingesta especfica para CASES"""

    def run(self):
        # Lee datos de datasets/cases/
        # Enva al topic "cases"
        # Usa configuracin de pipelines/cases/config.yaml
        pass
```

### Componentes Compartidos

Los componentes en `src/common/` son libreras reutilizables sin configuracin:

```mermaid
flowchart TB
    subgraph CommonLib["src/common/"]
        Sources["sources/\nkafka_source.py\nstorage_source.py"]
        Transforms["transforms/\nnormalize.py\nvalidate.py\ntimestamp.py\nwindowing.py\nmetadata.py"]
        Batching["batching/\nmanual_batch.py\nnative_batch.py"]
        Sinks["sinks/\nmongo_sink.py\ndlq_sink.py"]
        Utils["utils/\nconfig_loader.py\nschema_loader.py"]
    end

    CasesP["cases/pipeline.py"] --> Sources
    CasesP --> Transforms
    CasesP --> Batching
    CasesP --> Sinks

    DemisesP["demises/pipeline.py"] --> Sources
    DemisesP --> Transforms
    DemisesP --> Batching
    DemisesP --> Sinks

    style CommonLib fill:#fff9c4
    style CasesP fill:#e3f2fd
    style DemisesP fill:#e8f5e9
```

Estos componentes son invocados por cada pipeline con SU propia configuracin.

---

## Flujo de Datos por Schema

```mermaid
flowchart TB
    subgraph CasesFlow["Schema CASES"]
        CC_CSV["datasets/cases/*.csv"] --> CC_Ing["Ingestion\ncases/ingestion.py"]
        CC_Ing --> CC_Kafka["Kafka Topic: cases"]
        CC_Kafka --> CC_Pipe["Pipeline\ncases/pipeline.py"]
        CC_Pipe --> CC_Trans["Transforms\nWindow: 60s\nBatch: native, 100"]
        CC_Trans --> CC_Mongo["MongoDB: cases"]
    end

    subgraph DemisesFlow["Schema DEMISES (En Paralelo)"]
        DC_CSV["datasets/demises/*.csv"] --> DC_Ing["Ingestion\ndemises/ingestion.py"]
        DC_Ing --> DC_Kafka["Kafka Topic: demises"]
        DC_Kafka --> DC_Pipe["Pipeline\ndemises/pipeline.py"]
        DC_Pipe --> DC_Trans["Transforms\nWindow: 120s\nBatch: manual, 50"]
        DC_Trans --> DC_Mongo["MongoDB: demises"]
    end

    style CasesFlow fill:#e3f2fd
    style DemisesFlow fill:#e8f5e9
```

**Nota**: Ambos pipelines corren simultneamente sin afectarse.

---

## Orquestacin

El `orchestrator.py` descubre y gestiona schemas automticamente:

```mermaid
flowchart TD
    Orch["orchestrator.py"] --> Discover["Descubrimiento automtico\nEscanear pipelines/"]
    Discover --> Check{"Por cada subdirectorio"}

    Check --> Verify["Verificar:\npipeline.py existe?\nconfig.yaml existe?"]
    Verify --> |"S"| Register["Registrar como\nschema disponible"]
    Verify --> |"No"| Skip["Ignorar"]

    Register --> Execute{"Accin?"}
    Execute --> |"--list"| List["Listar schemas"]
    Execute --> |"--pipeline X"| RunPipe["Importar dinmicamente\ny ejecutar pipeline"]
    Execute --> |"--ingest X"| RunIng["Ejecutar ingesta"]
    Execute --> |"--parallel"| RunPar["Ejecutar en\nhilos paralelos"]

    style Orch fill:#e3f2fd
    style Register fill:#c8e6c9
```

```python
# Descubrimiento automtico
orchestrator = PipelineOrchestrator()
orchestrator.available_schemas  # ['cases', 'demises', ...]

# Ejecucin individual
orchestrator.run_pipeline('cases')

# Ejecucin paralela
orchestrator.run_multiple_pipelines(['cases', 'demises'], parallel=True)
```

### Proceso de Descubrimiento

1. Escanea directorio `pipelines/`
2. Por cada subdirectorio:
   - Verifica existencia de `pipeline.py`
   - Verifica existencia de `config.yaml`
   - Lo registra como schema disponible
3. Importa dinmicamente el mdulo cuando se necesita

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
        P3["CASES + DEMISES\n--parallel"]
    end

    Kafka["Kafka Cluster"] --> Server1
    Kafka --> Server2
    Kafka --> Server3

    Server1 --> MongoDB["MongoDB"]
    Server2 --> MongoDB
    Server3 --> MongoDB

    style Kafka fill:#fff3e0
    style MongoDB fill:#e8f5e9
```

Cada schema puede desplegarse independientemente:

```bash
# Servidor 1: Solo CASES
python orchestrator.py --pipeline cases

# Servidor 2: Solo DEMISES
python orchestrator.py --pipeline demises

# Servidor 3: Ambos en paralelo
python orchestrator.py --pipeline cases demises --parallel
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

    Kafka["Kafka"] --> |"Balanceo\nautomtico"| Workers
    Kafka --> WorkerD

    style Workers fill:#e3f2fd
    style WorkerD fill:#e8f5e9
```

```bash
# Worker 1: Procesa CASES con group.id=beam-pipeline-cases
python pipelines/cases/pipeline.py --mode streaming

# Worker 2: Procesa CASES con el MISMO group.id (escala horizontalmente)
python pipelines/cases/pipeline.py --mode streaming

# Worker 3: Procesa DEMISES independientemente
python pipelines/demises/pipeline.py --mode streaming
```

Kafka maneja el balanceo de carga entre workers del mismo consumer group.

### Configuracin por Ambiente

```mermaid
flowchart LR
    subgraph Configs["pipelines/cases/"]
        Default["config.yaml\n(Default)"]
        Dev["config.dev.yaml\n(Desarrollo)"]
        Prod["config.prod.yaml\n(Produccin)"]
    end

    Dev --> |"DirectRunner\nlocalhost"| Local["Entorno Local"]
    Prod --> |"DataflowRunner\nGCP"| Cloud["Google Cloud"]

    style Dev fill:#fff3e0
    style Prod fill:#e8f5e9
```

```python
# Cargar configuracin especfica
pipeline = CasesPipeline(config_path="pipelines/cases/config.prod.yaml")
```

---

## Testing por Schema

Cada schema se prueba independientemente:

```
tests/
  pipelines/
    test_cases_pipeline.py       # Tests solo para cases
    test_demises_pipeline.py     # Tests solo para demises
```

```python
# test_cases_pipeline.py
def test_cases_pipeline():
    pipeline = CasesPipeline()
    # Test especfico de cases
    ...

# test_demises_pipeline.py
def test_demises_pipeline():
    pipeline = DemisesPipeline()
    # Test especfico de demises
    ...
```

---

## Monitoreo por Schema

Cada schema tiene sus propias mtricas:

```mermaid
flowchart TB
    subgraph KafkaMetrics["Kafka - Mtricas por Schema"]
        KT1["Topic: cases\nMensajes, particiones"]
        KT2["Topic: demises\nMensajes, particiones"]
        KG1["Group: beam-pipeline-cases\nConsumer lag"]
        KG2["Group: beam-pipeline-demises\nConsumer lag"]
    end

    subgraph MongoMetrics["MongoDB - Mtricas por Schema"]
        MC1["Collection: cases\nDocumentos, ndices"]
        MC2["Collection: demises\nDocumentos, ndices"]
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
// Resultado:
// { _id: "cases", count: 5 }
// { _id: "demises", count: 2 }
```

---

## Ventajas de Independencia

```mermaid
flowchart TB
    subgraph Fault["1. Aislamiento de Fallos"]
        F1["CASES falla"] --> F2["Solo CASES se detiene"]
        F2 --> F3["DEMISES contina normalmente"]
    end

    subgraph Deploy["2. Despliegue Independiente"]
        D1["Actualizar config de CASES"] --> D2["Reiniciar solo CASES"]
        D2 --> D3["DEMISES no se ve afectado"]
    end

    subgraph Config["3. Configuracin Flexible"]
        CF1["cases: window=60s, batch=100, native"]
        CF2["demises: window=300s, batch=20, manual"]
    end

    subgraph Dev["4. Desarrollo Paralelo"]
        DV1["Equipo A: pipelines/cases/*"]
        DV2["Equipo B: pipelines/demises/*"]
        DV3["Sin conflictos ni dependencias"]
    end

    subgraph Scale["5. Escalamiento Granular"]
        SC1["CASES: 1M reg/da  5 workers"]
        SC2["DEMISES: 100k reg/da  1 worker"]
    end

    style Fault fill:#ffcdd2
    style Deploy fill:#e3f2fd
    style Config fill:#fff3e0
    style Dev fill:#e8f5e9
    style Scale fill:#f3e5f5
```

---

## Agregar Nuevo Schema: Paso a Paso

```bash
# 1. Crear estructura
mkdir -p pipelines/recovered
mkdir -p datasets/recovered

# 2. Copiar plantilla
cp pipelines/cases/* pipelines/recovered/

# 3. Editar config.yaml
# Cambiar: name, topic, group.id, collection

# 4. Editar schema.json
# Definir campos del schema

# 5. Editar pipeline.py
# Cambiar: CasesPipeline  RecoveredPipeline

# 6. Editar ingestion.py
# Cambiar: CasesIngestion  RecoveredIngestion

# 7. Agregar datos
cp mi_data.csv datasets/recovered/

# 8. Ejecutar
python orchestrator.py --ingest recovered
python orchestrator.py --pipeline recovered

# 9. El orquestador lo descubre automticamente
python orchestrator.py --list
# Output:
#   cases
#   demises
#   recovered  <-- Nuevo schema!
```

---

## Conclusin

```mermaid
mindmap
  root((Arquitectura\nMulti-Schema))
    Independencia total
      Aislamiento de fallos
      Testing independiente
      Despliegue granular
    Configuracin aislada
      Ventanas personalizadas
      Batching por schema
      Topics exclusivos
    Escalamiento
      Horizontal por schema
      Workers independientes
      Recursos segn necesidad
    Desarrollo paralelo
      Sin conflictos
      Equipos autnomos
      Fcil mantenimiento
```

Esta arquitectura proporciona independencia total entre schemas, configuracin aislada y personalizable, ejecucin paralela sin interferencia, escalamiento granular por schema, facilidad para agregar nuevos schemas, testing y despliegue independiente, y monitoreo especfico por schema.

Es la arquitectura ideal para sistemas multi-tenant o procesamiento de mltiples tipos de datos.
