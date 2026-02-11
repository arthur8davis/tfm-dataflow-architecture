# Guia Paso a Paso: Implementar Nuevo Schema

Esta guia ensena a crear un nuevo schema llamado **"vaccines"** que funciona en **batch** Y **streaming**.

---

## Tabla de Contenidos

1. [Preparacion](#1-preparacion)
2. [Crear Estructura](#2-crear-estructura)
3. [Definir Schema de Validacion](#3-definir-schema-de-validacion)
4. [Configurar el Pipeline](#4-configurar-el-pipeline)
5. [Crear Pipeline de Beam](#5-crear-pipeline-de-beam)
6. [Crear Script de Ingesta](#6-crear-script-de-ingesta)
7. [Preparar Datos de Prueba](#7-preparar-datos-de-prueba)
8. [Ejecutar en Modo Batch](#8-ejecutar-en-modo-batch)
9. [Ejecutar en Modo Streaming](#9-ejecutar-en-modo-streaming)
10. [Verificar Resultados](#10-verificar-resultados)

---

## Vision General del Proceso

```mermaid
flowchart TD
    Start(["Nuevo Schema:\nvaccines"]) --> Step1["1. Preparacion\nDefinir campos y tipos"]
    Step1 --> Step2["2. Crear estructura\npipelines/vaccines/\ndatasets/vaccines/"]
    Step2 --> Step3["3. vaccines.json\nCampos requeridos y tipos"]
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

## 1. Preparacion

### Que necesitas saber?

Antes de empezar, define:

- **Nombre del schema:** vaccines
- **Campos:** id, date, country, vaccine_name, doses, people_vaccinated
- **Campos requeridos:** id, date, country, doses
- **Modo:** Batch + Streaming

### Verificar que el proyecto este funcionando

```bash
# 1. Verificar estructura
./verify_structure.sh

# 2. Servicios corriendo
docker-compose ps

# 3. Si no estan corriendo
docker-compose up -d
```

### Schemas existentes como referencia

El proyecto ya tiene 3 schemas funcionando:
- **cases** - Datos de pacientes individuales (timestamp: fecha_muestra)
- **demises** - Datos de fallecimientos (timestamp: fecha_fallecimiento)
- **hospitalizations** - Datos de hospitalizaciones (timestamp: fecha_ingreso_hosp)

---

## 2. Crear Estructura

### Paso 2.1: Crear Carpetas

```bash
mkdir -p pipelines/vaccines
mkdir -p datasets/vaccines
```

### Paso 2.2: Copiar Plantilla desde CASES

```bash
cp pipelines/cases/config.yaml pipelines/vaccines/
cp pipelines/cases/cases.json pipelines/vaccines/vaccines.json
cp pipelines/cases/pipeline.py pipelines/vaccines/
cp pipelines/cases/ingestion.py pipelines/vaccines/
```

---

## 3. Definir Schema de Validacion

### Paso 3.1: Editar `pipelines/vaccines/vaccines.json`

```json
{
  "schema_name": "vaccines",
  "version": "1.0.0",
  "description": "Schema para datos de vacunacion COVID-19",
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

---

## 4. Configurar el Pipeline

### Paso 4.1: Editar `pipelines/vaccines/config.yaml`

```yaml
# Configuracion del pipeline para el schema VACCINES
schema:
  name: "vaccines"
  version: "1.0.0"
  description: "Pipeline para datos de vacunacion COVID-19"

# Source configuration
source:
  type: "kafka"  # "kafka" para streaming, "storage" para batch

  kafka:
    bootstrap_servers: "localhost:9092"
    topic: "vaccines"
    consumer_config:
      group.id: "beam-pipeline-vaccines"
      auto.offset.reset: "earliest"
      enable.auto.commit: "true"

  storage:
    file_pattern: "datasets/vaccines/*.csv"
    file_type: "csv"

# Transform configuration
transforms:
  normalize:
    enabled: true

  validate:
    enabled: true
    schema_file: "pipelines/vaccines/vaccines.json"

  timestamp:
    enabled: true
    field: "date"

  windowing:
    enabled: true
    window_size_seconds: 90
    allowed_lateness_seconds: 300
    trigger: "default"

  metadata:
    enabled: true
    pipeline_version: "1.0.0"

# Batching configuration
batching:
  strategy: "native"
  batch_size: 150
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

### Puntos clave de configuracion

```mermaid
flowchart TB
    Config["config.yaml"] --> Source{"source.type"}
    Source --> |"kafka"| Streaming["Streaming\ntopic: vaccines\ngroup.id: beam-pipeline-vaccines"]
    Source --> |"storage"| Batch["Batch\nfile_pattern: datasets/vaccines/*.csv"]

    Config --> Windowing["windowing\nwindow_size: 90s"]
    Config --> BatchingConf["batching\nstrategy: native\nbatch_size: 150"]
    Config --> Sink["sink\ncollection: vaccines\ntimeseries: hours"]

    style Config fill:#e3f2fd
    style Streaming fill:#e8f5e9
    style Batch fill:#fff3e0
```

---

## 5. Crear Pipeline de Beam

### Paso 5.1: Editar `pipelines/vaccines/pipeline.py`

Cambios necesarios (solo nombres, la logica permanece igual):

```python
"""
Pipeline especifico para el schema VACCINES
"""
# ... (resto de imports igual)

class VaccinesPipeline:
    """Pipeline para procesar datos de VACCINES"""

    def __init__(self, config_path: str = None):
        """
        Inicializa el pipeline de vaccines
        """
        # ... resto del codigo IGUAL

def main():
    pipeline = VaccinesPipeline()
    pipeline.run()
```

**IMPORTANTE**: El resto del codigo permanece **IGUAL**. No cambies la logica, solo los nombres.

---

## 6. Crear Script de Ingesta

### Paso 6.1: Editar `pipelines/vaccines/ingestion.py`

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
    Pipeline --> Transforms["Transforms\nnormalize -> validate\ntimestamp -> window\nmetadata -> batch"]
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
docker exec -it mongodb mongosh -u admin -p admin123

use covid-db
db.vaccines.countDocuments()
db.vaccines.find().limit(3).pretty()
exit
```

---

## 9. Ejecutar en Modo STREAMING

El modo streaming lee de Kafka continuamente.

### Paso 9.1: Configurar para Streaming

En `pipelines/vaccines/config.yaml`:
```yaml
source:
  type: "kafka"
```

### Paso 9.2: Ingestar Datos a Kafka

```bash
python pipelines/vaccines/ingestion.py
```

Verificar en Kafka UI: http://localhost:8080
- Ir a Topics -> "vaccines"
- Ver que hay mensajes

### Paso 9.3: Ejecutar Pipeline Streaming

En una **nueva terminal**:

```bash
python pipelines/vaccines/pipeline.py --mode streaming
```

### Paso 9.4: Ver Procesamiento en Tiempo Real

```bash
docker exec -it mongodb mongosh -u admin -p admin123

use covid-db
db.vaccines.countDocuments()
```

Para **detener el pipeline**: `Ctrl + C` en la terminal donde corre

---

## 10. Verificar Resultados

### Opcion 1: MongoDB Shell

```javascript
use("covid-db");

db.vaccines.countDocuments();
db.vaccines.find().limit(5).pretty();
db.vaccines.findOne();
db.dead_letter_queue.find({schema: "vaccines"}).pretty();

// Agregacion por pais
db.vaccines.aggregate([
  {$group: {
    _id: "$country",
    total_doses: {$sum: "$doses"},
    count: {$sum: 1}
  }}
]);
```

### Opcion 2: Mongo Express (GUI)

1. Abrir: http://localhost:8083
2. Database: `covid-db`
3. Collections:
   - `vaccines` - Ver datos procesados
   - `dead_letter_queue` - Ver errores (si hay)

### Opcion 3: Kafka UI (para Streaming)

1. Abrir: http://localhost:8080
2. Topics -> `vaccines`
3. Ver mensajes originales en Kafka

---

## Usando el Orquestador

Una vez que todo funciona, puedes usar el orquestador:

```bash
# Listar schemas (deberia aparecer vaccines)
python orchestrator.py --list

# Ingestar
python orchestrator.py --ingest vaccines

# Ejecutar pipeline
python orchestrator.py --pipeline vaccines

# Ejecutar junto con otros schemas en paralelo
python orchestrator.py --pipeline cases demises hospitalizations vaccines --parallel
```

---

## Checklist Final

| Paso | Verificacion |
|------|-------------|
| Estructura creada | `ls pipelines/vaccines/` y `ls datasets/vaccines/` |
| `vaccines.json` definido | Campos correctos |
| `config.yaml` configurado | topic, collection, ventanas |
| `pipeline.py` editado | Nombre de clase: VaccinesPipeline |
| `ingestion.py` editado | Nombre de clase: VaccinesIngestion |
| CSV de prueba | Datos en `datasets/vaccines/` |
| Modo BATCH probado | Datos en MongoDB |
| Modo STREAMING probado | Datos en MongoDB |
| Sin errores en DLQ | `db.dead_letter_queue.find({schema: "vaccines"})` vacio |
| Orquestador reconoce | `python orchestrator.py --list` muestra vaccines |

---

## Archivos Importantes

| Archivo | Proposito | Cuando Editar |
|---------|-----------|---------------|
| `config.yaml` | Configuracion del pipeline | Siempre (obligatorio) |
| `vaccines.json` | Validacion de datos | Siempre (obligatorio) |
| `pipeline.py` | Logica del pipeline | Solo nombres de clase |
| `ingestion.py` | Ingesta a Kafka | Solo nombres de clase |
| `sample_data.csv` | Datos de prueba | Segun tus datos |

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
        Ind2["Multiples schemas en paralelo"]
        Ind3["Error en vaccines\nno afecta a cases"]
    end

    subgraph Reuse["Reutilizacion"]
        R1["Todos usan src/common/"]
        R2["Solo cambia configuracion"]
        R3["Mejoras en src/ benefician a todos"]
    end

    style Modes fill:#e3f2fd
    style Independence fill:#e8f5e9
    style Reuse fill:#fff3e0
```

---

**Ultima actualizacion:** 2026-02-10
