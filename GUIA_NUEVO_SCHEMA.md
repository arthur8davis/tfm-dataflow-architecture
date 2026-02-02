# Gua Paso a Paso: Implementar Nuevo Schema

Esta gua ensea a crear un nuevo schema llamado **"vaccines"** que funciona en **batch** Y **streaming**.

---

## Tabla de Contenidos

1. [Preparacin](#1-preparacin)
2. [Crear Estructura](#2-crear-estructura)
3. [Definir Schema de Validacin](#3-definir-schema-de-validacin)
4. [Configurar el Pipeline](#4-configurar-el-pipeline)
5. [Crear Pipeline de Beam](#5-crear-pipeline-de-beam)
6. [Crear Script de Ingesta](#6-crear-script-de-ingesta)
7. [Preparar Datos de Prueba](#7-preparar-datos-de-prueba)
8. [Ejecutar en Modo Batch](#8-ejecutar-en-modo-batch)
9. [Ejecutar en Modo Streaming](#9-ejecutar-en-modo-streaming)
10. [Verificar Resultados](#10-verificar-resultados)

---

## Visin General del Proceso

```mermaid
flowchart TD
    Start(["Nuevo Schema:\nvaccines"]) --> Step1["1. Preparacin\nDefinir campos y tipos"]
    Step1 --> Step2["2. Crear estructura\npipelines/vaccines/\ndatasets/vaccines/"]
    Step2 --> Step3["3. schema.json\nCampos requeridos y tipos"]
    Step3 --> Step4["4. config.yaml\nKafka, windowing, batching, MongoDB"]
    Step4 --> Step5["5. pipeline.py\nCambiar nombre de clase"]
    Step5 --> Step6["6. ingestion.py\nCambiar nombre de clase"]
    Step6 --> Step7["7. sample_data.csv\nDatos de prueba"]
    Step7 --> Step8{"Modo?"}
    Step8 --> |"Batch"| Batch["8. source.type: storage\npython pipeline.py"]
    Step8 --> |"Streaming"| Stream["9. source.type: kafka\nIngesta + Pipeline"]
    Batch --> Step10["10. Verificar en MongoDB"]
    Stream --> Step10
    Step10 --> Done(["Schema Completo"])

    style Start fill:#e3f2fd
    style Done fill:#c8e6c9
    style Batch fill:#fff3e0
    style Stream fill:#e8f5e9
```

---

## 1. Preparacin

### Qu necesitas saber?

Antes de empezar, define:

```mermaid
flowchart LR
    subgraph Definir["Definiciones Previas"]
        Name["Nombre: vaccines"]
        Fields["Campos: id, date, country\nvaccine_name, doses\npeople_vaccinated"]
        Required["Requeridos: id, date\ncountry, doses"]
        Mode["Modo: Batch + Streaming"]
    end

    style Definir fill:#e3f2fd
```

### Verificar que el proyecto est funcionando

```bash
# 1. Verificar estructura
./verify_structure.sh

# 2. Servicios corriendo
docker-compose ps

# 3. Si no estn corriendo
docker-compose up -d
```

---

## 2. Crear Estructura

### Paso 2.1: Crear Carpetas

```bash
# Crear carpeta del pipeline
mkdir -p pipelines/vaccines

# Crear carpeta de datos
mkdir -p datasets/vaccines
```

### Paso 2.2: Copiar Plantilla desde CASES

```bash
# Copiar todos los archivos de cases como plantilla
cp pipelines/cases/config.yaml pipelines/vaccines/
cp pipelines/cases/schema.json pipelines/vaccines/
cp pipelines/cases/pipeline.py pipelines/vaccines/
cp pipelines/cases/ingestion.py pipelines/vaccines/
```

Resultado:

```mermaid
flowchart TB
    subgraph VaccinesDir["pipelines/vaccines/"]
        Config["config.yaml\n(a editar)"]
        Schema["schema.json\n(a editar)"]
        Pipeline["pipeline.py\n(a editar)"]
        Ingestion["ingestion.py\n(a editar)"]
    end

    style VaccinesDir fill:#fff3e0
```

---

## 3. Definir Schema de Validacin

### Paso 3.1: Editar `pipelines/vaccines/schema.json`

```json
{
  "schema_name": "vaccines",
  "version": "1.0.0",
  "description": "Schema para datos de vacunacin COVID-19",
  "required_fields": [
    "id",
    "date",
    "country",
    "doses"
  ],
  "field_types": {
    "id": "string",
    "date": "string",
    "country": "string",
    "vaccine_name": "string",
    "doses": "integer",
    "people_vaccinated": "integer",
    "timestamp": "number"
  },
  "optional_fields": [
    "vaccine_name",
    "people_vaccinated",
    "timestamp"
  ]
}
```

```mermaid
flowchart LR
    subgraph SchemaValidation["Validacin del Schema"]
        Required["required_fields\nSi faltan  DLQ"]
        Types["field_types\nTipo esperado de cada campo"]
        Optional["optional_fields\nPueden estar o no"]
    end

    Input["Registro"] --> Required
    Required --> |"Vlido"| Types
    Required --> |"Falta campo"| DLQ["Dead Letter Queue"]
    Types --> |"Tipo correcto"| Output["Registro vlido"]
    Types --> |"Tipo incorrecto"| DLQ

    style DLQ fill:#ffcdd2
    style Output fill:#c8e6c9
```

---

## 4. Configurar el Pipeline

### Paso 4.1: Editar `pipelines/vaccines/config.yaml`

```yaml
# Configuracin del pipeline para el schema VACCINES
schema:
  name: "vaccines"
  version: "1.0.0"
  description: "Pipeline para datos de vacunacin COVID-19"

# Source configuration
source:
  type: "kafka"  # "kafka" para streaming, "storage" para batch

  # Configuracin de Kafka (streaming)
  kafka:
    bootstrap_servers: "localhost:9092"
    topic: "vaccines"
    consumer_config:
      group.id: "beam-pipeline-vaccines"
      auto.offset.reset: "earliest"
      enable.auto.commit: false
      max.poll.records: 500

  # Configuracin de Storage (batch)
  storage:
    file_pattern: "datasets/vaccines/*.csv"
    file_type: "csv"  # "csv" o "parquet"

# Transform configuration
transforms:
  normalize:
    enabled: true

  validate:
    enabled: true
    schema_file: "pipelines/vaccines/schema.json"

  timestamp:
    enabled: true
    field: "date"  # Campo del cual extraer timestamp

  windowing:
    enabled: true
    window_size_seconds: 90  # Ventana de 90 segundos
    allowed_lateness_seconds: 300
    trigger: "default"

  metadata:
    enabled: true
    pipeline_version: "1.0.0"

# Batching configuration
batching:
  strategy: "native"  # "native" o "manual"
  batch_size: 150     # Batch ms grande que cases
  batch_timeout_seconds: 30

# Sink configuration
sink:
  mongodb:
    connection_string: "mongodb://admin:admin123@localhost:27017"
    database: "covid-db"
    collection:
      name: "vaccines"
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

### Puntos clave de configuracin

```mermaid
flowchart TB
    Config["config.yaml"] --> Source{"source.type"}
    Source --> |"kafka"| Streaming["Streaming\ntopic: vaccines\ngroup.id: beam-pipeline-vaccines"]
    Source --> |"storage"| Batch["Batch\nfile_pattern: datasets/vaccines/*.csv"]

    Config --> Windowing["windowing\nwindow_size: 90s\n(diferente a cases: 60s)"]
    Config --> BatchingConf["batching\nstrategy: native\nbatch_size: 150"]
    Config --> Sink["sink\ncollection: vaccines\ntimeseries: hours"]

    style Config fill:#e3f2fd
    style Streaming fill:#e8f5e9
    style Batch fill:#fff3e0
```

---

## 5. Crear Pipeline de Beam

### Paso 5.1: Editar `pipelines/vaccines/pipeline.py`

Cambios necesarios (solo nombres, la lgica permanece igual):

```mermaid
flowchart LR
    subgraph Cambios["Cambios en pipeline.py"]
        C1["Descripcin\n'CASES'  'VACCINES'"]
        C2["Clase\nCasesPipeline  VaccinesPipeline"]
        C3["Docstring\n'datos de CASES'  'datos de VACCINES'"]
        C4["main()\nCasesPipeline()  VaccinesPipeline()"]
    end

    style Cambios fill:#fff3e0
```

```python
"""
Pipeline especfico para el schema VACCINES
"""
# ... (resto de imports igual)

class VaccinesPipeline:
    """Pipeline para procesar datos de VACCINES"""

    def __init__(self, config_path: str = None):
        """
        Inicializa el pipeline de vaccines
        # ... resto igual
        """
        # ... resto del cdigo IGUAL

def main():
    pipeline = VaccinesPipeline()
    pipeline.run()
```

**IMPORTANTE**: El resto del cdigo permanece **IGUAL**. No cambies la lgica, solo los nombres.

---

## 6. Crear Script de Ingesta

### Paso 6.1: Editar `pipelines/vaccines/ingestion.py`

```mermaid
flowchart LR
    subgraph Cambios["Cambios en ingestion.py"]
        C1["Descripcin\n'CASES'  'VACCINES'"]
        C2["Clase\nCasesIngestion  VaccinesIngestion"]
        C3["Parser description\n'CASES'  'VACCINES'"]
        C4["main()\nCasesIngestion()  VaccinesIngestion()"]
    end

    style Cambios fill:#fff3e0
```

```python
"""
Script de ingesta para el schema VACCINES
"""

class VaccinesIngestion:
    """Ingesta de datos para el schema VACCINES"""
    ...

def main():
    parser = argparse.ArgumentParser(description='Ingesta de datos para VACCINES')
    ...
    ingestion = VaccinesIngestion()
    ...
```

---

## 7. Preparar Datos de Prueba

### Paso 7.1: Crear CSV de ejemplo

Archivo: `datasets/vaccines/sample_data.csv`

```csv
id,date,country,vaccine_name,doses,people_vaccinated
1,2023-01-01,USA,Pfizer,1000000,500000
2,2023-01-01,Spain,Moderna,800000,400000
3,2023-01-01,France,AstraZeneca,600000,300000
4,2023-01-02,USA,Pfizer,1100000,550000
5,2023-01-02,Spain,Moderna,850000,425000
6,2023-01-02,France,AstraZeneca,650000,325000
7,2023-01-03,USA,Pfizer,1200000,600000
8,2023-01-03,Spain,Moderna,900000,450000
9,2023-01-03,France,AstraZeneca,700000,350000
10,2023-01-04,USA,Pfizer,1300000,650000
```

---

## 8. Ejecutar en Modo BATCH

El modo batch lee archivos CSV directamente sin pasar por Kafka.

```mermaid
flowchart LR
    CSV["datasets/vaccines/\nsample_data.csv"] --> Pipeline["Pipeline\nsource.type: storage"]
    Pipeline --> Transforms["Transforms\nnormalize  validate\ntimestamp  window\nmetadata  batch"]
    Transforms --> MongoDB["MongoDB\ncollection: vaccines"]

    style CSV fill:#e3f2fd
    style MongoDB fill:#c8e6c9
```

### Paso 8.1: Configurar para Batch

En `pipelines/vaccines/config.yaml`:
```yaml
source:
  type: "storage"  # Cambiar de "kafka" a "storage"
```

### Paso 8.2: Ejecutar el Pipeline

```bash
python pipelines/vaccines/pipeline.py --mode batch
```

### Paso 8.3: Verificar Resultados

```bash
docker exec -it $(docker-compose ps -q mongodb) mongosh -u admin -p admin123

# En mongosh:
use covid-db
db.vaccines.countDocuments()  // Debe mostrar 10
db.vaccines.find().limit(3).pretty()
exit
```

---

## 9. Ejecutar en Modo STREAMING

El modo streaming lee de Kafka continuamente.

```mermaid
sequenceDiagram
    participant CSV as sample_data.csv
    participant Ing as ingestion.py
    participant Kafka as Kafka Topic: vaccines
    participant Pipe as pipeline.py
    participant Mongo as MongoDB: vaccines

    Note over Ing: Paso 1: Ingesta
    Ing->>CSV: Leer datos
    loop Cada fila
        Ing->>Kafka: Enviar mensaje JSON
    end

    Note over Pipe: Paso 2: Pipeline (otra terminal)
    loop Streaming continuo
        Pipe->>Kafka: Consumir mensajes
        Pipe->>Pipe: Transformaciones
        Pipe->>Mongo: Escribir batch
    end

    Note over Mongo: Datos disponibles en tiempo real
```

### Paso 9.1: Configurar para Streaming

En `pipelines/vaccines/config.yaml`:
```yaml
source:
  type: "kafka"  # Cambiar de "storage" a "kafka"
```

### Paso 9.2: Ingestar Datos a Kafka

```bash
python pipelines/vaccines/ingestion.py
```

Verificar en Kafka UI: http://localhost:8080
- Ir a Topics -> "vaccines"
- Ver que hay 10 mensajes

### Paso 9.3: Ejecutar Pipeline Streaming

En una **nueva terminal**:

```bash
python pipelines/vaccines/pipeline.py --mode streaming
```

### Paso 9.4: Ver Procesamiento en Tiempo Real

```bash
docker exec -it $(docker-compose ps -q mongodb) mongosh -u admin -p admin123

use covid-db
db.vaccines.countDocuments()  // Aumenta mientras procesa
```

Para **detener el pipeline**: `Ctrl + C` en la terminal donde corre

---

## 10. Verificar Resultados

### Opciones de Verificacin

```mermaid
flowchart TD
    Verify(["Verificar Resultados"]) --> Option1["MongoDB Shell\nmongosh"]
    Verify --> Option2["Mongo Express\nhttp://localhost:8083"]
    Verify --> Option3["Kafka UI\nhttp://localhost:8080"]

    Option1 --> Q1["db.vaccines.countDocuments()"]
    Option1 --> Q2["db.vaccines.find().limit(5)"]
    Option1 --> Q3["db.dead_letter_queue.find(\n{schema: 'vaccines'})"]

    Option2 --> G1["Database: covid-db\nCollection: vaccines"]

    Option3 --> K1["Topics  vaccines\nVer mensajes originales"]

    style Verify fill:#e3f2fd
```

### Opcin 1: MongoDB Shell

```javascript
use("covid-db");

// Contar documentos
db.vaccines.countDocuments();

// Ver documentos
db.vaccines.find().limit(5).pretty();

// Verificar estructura time series
db.vaccines.findOne();
// Debe tener: timestamp, metadata, y los campos de datos

// Ver errores (si hay)
db.dead_letter_queue.find({schema: "vaccines"}).pretty();

// Agregacin por pas
db.vaccines.aggregate([
  {$group: {
    _id: "$country",
    total_doses: {$sum: "$doses"},
    count: {$sum: 1}
  }}
]);
```

### Opcin 2: Mongo Express (GUI)

1. Abrir: http://localhost:8083
2. Login: `admin` / `admin123`
3. Database: `covid-db`
4. Collections:
   - `vaccines` - Ver datos procesados
   - `dead_letter_queue` - Ver errores (si hay)

### Opcin 3: Kafka UI (para Streaming)

1. Abrir: http://localhost:8080
2. Topics -> `vaccines`
3. Ver mensajes originales en Kafka

---

## Resumen de Comandos

### Modo BATCH vs STREAMING

```mermaid
flowchart LR
    subgraph BatchMode["Modo BATCH"]
        B1["source.type: storage"]
        B2["Lee CSV directamente"]
        B3["Termina al procesar todo"]
    end

    subgraph StreamMode["Modo STREAMING"]
        S1["source.type: kafka"]
        S2["Ingesta  Kafka  Pipeline"]
        S3["Procesamiento continuo"]
    end

    style BatchMode fill:#fff3e0
    style StreamMode fill:#e8f5e9
```

### Modo BATCH
```bash
# 1. Ejecutar
python pipelines/vaccines/pipeline.py --mode batch
# 2. Verificar
docker exec -it $(docker-compose ps -q mongodb) mongosh -u admin -p admin123
```

### Modo STREAMING
```bash
# 1. Ingestar a Kafka
python pipelines/vaccines/ingestion.py
# 2. Ejecutar pipeline (en otra terminal)
python pipelines/vaccines/pipeline.py --mode streaming
# 3. Verificar en tiempo real
```

---

## Usando el Orquestador

Una vez que todo funciona, puedes usar el orquestador:

```bash
# Listar schemas (debera aparecer vaccines)
python orchestrator.py --list

# Ingestar
python orchestrator.py --ingest vaccines

# Ejecutar pipeline
python orchestrator.py --pipeline vaccines

# Ejecutar junto con otros schemas en paralelo
python orchestrator.py --pipeline cases demises vaccines --parallel
```

---

## Troubleshooting

```mermaid
flowchart TD
    Problem(["Problema"]) --> Type{"Error?"}

    Type --> |"Schema file not found"| Fix1["Verificar:\nls pipelines/vaccines/schema.json\nSi no existe, crearlo"]
    Type --> |"Topic does not exist"| Fix2["Ejecutar ingesta:\npython pipelines/vaccines/ingestion.py\n(crea el topic automticamente)"]
    Type --> |"Datos no en MongoDB"| Fix3["Ver logs del pipeline:\npython pipeline.py 2>&1 | tee log\nVerificar DLQ:\ndb.dead_letter_queue.find({schema: 'vaccines'})"]
    Type --> |"Module not found"| Fix4["Ejecutar desde raz:\ncd /home/.../tfm\npython pipelines/vaccines/pipeline.py"]

    style Problem fill:#ffcdd2
    style Fix1 fill:#c8e6c9
    style Fix2 fill:#c8e6c9
    style Fix3 fill:#c8e6c9
    style Fix4 fill:#c8e6c9
```

---

## Checklist Final

| Paso | Verificacin |
|------|-------------|
| Estructura creada | `ls pipelines/vaccines/` y `ls datasets/vaccines/` |
| `schema.json` definido | Campos correctos |
| `config.yaml` configurado | topic, collection, ventanas |
| `pipeline.py` editado | Nombre de clase cambiado |
| `ingestion.py` editado | Nombre de clase cambiado |
| CSV de prueba | Datos en `datasets/vaccines/` |
| Modo BATCH probado | Datos en MongoDB |
| Modo STREAMING probado | Datos en MongoDB |
| Sin errores en DLQ | `db.dead_letter_queue.find({schema: "vaccines"})` vaco |
| Orquestador reconoce | `python orchestrator.py --list` muestra vaccines |

---

## Archivos Importantes

| Archivo | Propsito | Cundo Editar |
|---------|-----------|---------------|
| `config.yaml` | Configuracin del pipeline | Siempre (obligatorio) |
| `schema.json` | Validacin de datos | Siempre (obligatorio) |
| `pipeline.py` | Lgica del pipeline | Solo nombres de clase |
| `ingestion.py` | Ingesta a Kafka | Solo nombres de clase |
| `sample_data.csv` | Datos de prueba | Segn tus datos |

**NO edites**: Archivos en `src/` (son compartidos por todos los schemas)

---

## Conceptos Clave

```mermaid
flowchart TB
    subgraph Modes["Batch vs Streaming"]
        Batch["Batch\nProcesa archivos existentes\nTermina al completar"]
        Streaming["Streaming\nProceso continuo de Kafka\nNo termina"]
        Switch["Cambiar: solo editar\nsource.type en config.yaml"]
    end

    subgraph Independence["Independencia"]
        Ind1["Cada schema es independiente"]
        Ind2["10 schemas en paralelo"]
        Ind3["Error en vaccines\nno afecta a cases"]
    end

    subgraph Reuse["Reutilizacin"]
        R1["Todos usan src/common/"]
        R2["Solo cambia configuracin"]
        R3["Mejoras en src/ benefician a todos"]
    end

    style Modes fill:#e3f2fd
    style Independence fill:#e8f5e9
    style Reuse fill:#fff3e0
```
