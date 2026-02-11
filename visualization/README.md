# COVID-19 Peru - Dashboard en Tiempo Real

Dashboard interactivo para visualizacion de datos COVID-19 usando **D3.js**, **Leaflet.js**, **WebSockets** y **MongoDB**.

## Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CAPA DE VISUALIZACION                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    FRONTEND (Browser)                                  │  │
│  │                                                                        │  │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │  │
│  │  │                    Visualizaciones D3.js                         │  │  │
│  │  │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────────┐   │  │  │
│  │  │  │ Barras    │ │ Timeline  │ │ Piramide  │ │ Donut (Sexo)  │   │  │  │
│  │  │  │ (Depto)   │ │ (Fechas)  │ │ (Edad)    │ │               │   │  │  │
│  │  │  └───────────┘ └───────────┘ └───────────┘ └───────────────┘   │  │  │
│  │  │  ┌─────────────────────────────────────────────────────────┐   │  │  │
│  │  │  │         Mapas de Calor (Leaflet.js + Heatmap)           │   │  │  │
│  │  │  │   Casos  |  Fallecidos  |  Hospitalizaciones            │   │  │  │
│  │  │  └─────────────────────────────────────────────────────────┘   │  │  │
│  │  └─────────────────────────────────────────────────────────────────┘  │  │
│  │                                                                        │  │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │  │
│  │  │                    Modulos JavaScript                            │  │  │
│  │  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐               │  │  │
│  │  │  │ alerts  │ │ filters │ │ config  │ │  utils  │               │  │  │
│  │  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘               │  │  │
│  │  └─────────────────────────────────────────────────────────────────┘  │  │
│  │                              │                                         │  │
│  │                      Socket.IO Client                                  │  │
│  └──────────────────────────────┼────────────────────────────────────────┘  │
│                                 │                                           │
│                         WebSocket + HTTP                                    │
│                                 │                                           │
│  ┌──────────────────────────────┼────────────────────────────────────────┐  │
│  │                    BACKEND (Flask + SocketIO)                          │  │
│  │                              │                                         │  │
│  │  ┌─────────────┐    ┌───────┴───────┐    ┌─────────────────────────┐  │  │
│  │  │   app.py    │    │   routes/     │    │      handlers/          │  │  │
│  │  │             │───▶│   api.py      │───▶│  ┌─────────────────┐    │  │  │
│  │  │  - Polling  │    │  (REST API)   │    │  │    queries/     │    │  │  │
│  │  │  - Cache    │    └───────────────┘    │  │  - cases.py     │    │  │  │
│  │  │  - Refresh  │                         │  │  - demises.py   │    │  │  │
│  │  └─────────────┘    ┌───────────────┐    │  │  - hospital.py  │    │  │  │
│  │                     │  websocket/   │    │  │  - summary.py   │    │  │  │
│  │                     │  events.py    │───▶│  └─────────────────┘    │  │  │
│  │                     │ (15+ eventos) │    │  ┌─────────────────┐    │  │  │
│  │                     └───────────────┘    │  │   alerts.py     │    │  │  │
│  │                                          │  │  (Umbrales)     │    │  │  │
│  │                                          │  └─────────────────┘    │  │  │
│  │                                          └─────────────────────────┘  │  │
│  │                                                      │                │  │
│  │                                          ┌───────────┴───────────┐    │  │
│  │                                          │    services/          │    │  │
│  │                                          │    database.py        │    │  │
│  │                                          │   (PyMongo Client)    │    │  │
│  │                                          └───────────┬───────────┘    │  │
│  └──────────────────────────────────────────────────────┼────────────────┘  │
│                                                         │                   │
└─────────────────────────────────────────────────────────┼───────────────────┘
                                                          │
                                                    Polling (3s)
                                                          │
┌─────────────────────────────────────────────────────────┼───────────────────┐
│                              CAPA DE DATOS              │                    │
├─────────────────────────────────────────────────────────┼───────────────────┤
│                                                         ▼                   │
│   ┌─────────────────┐                         ┌─────────────────┐           │
│   │  Apache Kafka   │────Pipeline────────────▶│    MongoDB      │           │
│   │  (KRaft mode)   │   (Apache Beam)         │   (mongo:8.0)   │           │
│   │  + Apache Beam  │                         │                 │           │
│   └─────────────────┘                         │  Colecciones:   │           │
│                                               │  - cases        │           │
│                                               │  - demises      │           │
│                                               │  - hospitaliz.  │           │
│                                               └─────────────────┘           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Estructura del Proyecto

```
visualization/
|-- app.py                        # Servidor Flask + SocketIO + Polling
|-- config.py                     # Configuracion (MongoDB, puertos, coords Peru)
|
|-- services/
|   |-- __init__.py
|   +-- database.py               # Cliente PyMongo
|
|-- routes/
|   |-- __init__.py
|   +-- api.py                    # 14 endpoints REST
|
|-- handlers/
|   |-- __init__.py
|   |-- alerts.py                 # Sistema de alertas por umbral
|   |-- queries/
|   |   |-- __init__.py
|   |   |-- cases.py              # Queries: casos, heatmap, filtros
|   |   |-- demises.py            # Queries: fallecidos, heatmap
|   |   |-- hospitalizations.py   # Queries: hospitalizaciones
|   |   +-- summary.py            # Queries: totales, conteos
|   +-- websocket/
|       |-- __init__.py
|       +-- events.py             # 15+ handlers WebSocket
|
|-- static/
|   |-- css/
|   |   +-- style.css             # Tema oscuro responsive
|   +-- js/
|       |-- main.js               # Entry point, conexion SocketIO
|       |-- charts/
|       |   |-- index.js          # Exporta todos los graficos
|       |   |-- department.js     # Barras horizontales (D3.js)
|       |   |-- timeline.js       # Area chart temporal (D3.js)
|       |   |-- age.js            # Piramide poblacional (D3.js)
|       |   |-- sex.js            # Donut chart (D3.js)
|       |   +-- heatmaps.js       # Mapas Leaflet + leaflet.heat
|       +-- modules/
|           |-- alerts.js         # UI alertas, notificaciones
|           |-- filters.js        # Filtros departamento/sexo
|           |-- config.js         # Constantes frontend
|           +-- utils.js          # Formateo numeros, fechas
|
+-- templates/
    +-- index.html                # SPA con todas las visualizaciones
```

## Visualizaciones Incluidas

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

## Como Funciona

### Comunicacion en Tiempo Real

El sistema implementa un mecanismo de actualizacion en tiempo real basado en **WebSockets y Polling** que permite reflejar los cambios en los datos con baja latencia.

#### Flujo de Actualizacion de Datos

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    FLUJO DE ACTUALIZACION EN TIEMPO REAL                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────┐         ┌─────────────┐         ┌─────────────────────────┐   │
│  │   Apache    │         │             │         │      Flask Server       │   │
│  │    Beam     │────────▶│   MongoDB   │◀────────│    (app.py)             │   │
│  │  Pipeline   │  INSERT │             │ POLLING │                         │   │
│  └─────────────┘         └─────────────┘  (3s)   │  ┌───────────────────┐  │   │
│        │                       │                  │  │  refresh_loop()   │  │   │
│        │                       │                  │  │                   │  │   │
│        ▼                       ▼                  │  │ count_anterior    │  │   │
│  ┌─────────────┐         ┌─────────────┐         │  │ count_actual      │  │   │
│  │  Kafka      │         │ Colecciones │         │  │                   │  │   │
│  │  Topics     │         │  - cases    │         │  │ if cambio:        │  │   │
│  │             │         │  - demises  │         │  │   emit(datos)     │  │   │
│  └─────────────┘         │  - hospital │         │  └─────────┬─────────┘  │   │
│                          └─────────────┘                      │            │   │
│                                                               │            │   │
│                                                               ▼            │   │
│                                                    ┌───────────────────┐   │   │
│                                                    │    Socket.IO      │   │   │
│                                                    │    Server         │   │   │
│                                                    └─────────┬─────────┘   │   │
│                                                              │             │   │
└──────────────────────────────────────────────────────────────┼─────────────┘   │
                                                               │                 │
                                                        WebSocket                │
                                                         emit()                  │
                                                               │                 │
┌──────────────────────────────────────────────────────────────┼─────────────────┘
│                                                              │
│  ┌───────────────────────────────────────────────────────────┼───────────────┐
│  │                      CLIENTES (Browsers)                  │               │
│  │                                                           ▼               │
│  │  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────────┐   │
│  │  │  Cliente 1  │    │  Cliente 2  │    │  Socket.IO Client           │   │
│  │  │             │    │             │    │                             │   │
│  │  │ ┌─────────┐ │    │ ┌─────────┐ │    │  socket.on('update_*')     │   │
│  │  │ │  D3.js  │ │    │ │  D3.js  │ │    │         │                   │   │
│  │  │ │ Charts  │ │    │ │ Charts  │ │    │         ▼                   │   │
│  │  │ └─────────┘ │    │ └─────────┘ │    │  updateChart(data)          │   │
│  │  │ ┌─────────┐ │    │ ┌─────────┐ │    │         │                   │   │
│  │  │ │ Leaflet │ │    │ │ Leaflet │ │    │         ▼                   │   │
│  │  │ │  Maps   │ │    │ │  Maps   │ │    │  D3.js transition()         │   │
│  │  │ └─────────┘ │    │ └─────────┘ │    │  (animacion suave)          │   │
│  │  └─────────────┘    └─────────────┘    └─────────────────────────────┘   │
│  │                                                                           │
│  └───────────────────────────────────────────────────────────────────────────┘
│
└─────────────────────────────────────────────────────────────────────────────────┘
```

#### Pasos del Flujo

1. **Pipeline inserta datos**: Apache Beam procesa eventos de Kafka e inserta/actualiza documentos en MongoDB
2. **Polling detecta cambios**: El servidor Flask consulta `estimated_document_count()` cada 3 segundos
3. **Comparacion de conteos**: Si el conteo actual difiere del anterior, hay datos nuevos
4. **Ejecucion de queries**: Se ejecutan las agregaciones para obtener datos actualizados
5. **Emision WebSocket**: `socketio.emit()` envia los datos a todos los clientes conectados
6. **Actualizacion UI**: Los clientes reciben el evento y D3.js actualiza los graficos con transiciones

#### Eventos WebSocket Emitidos al Detectar Cambios

```python
# Cuando cambian los casos
socketio.emit('data_changed', {'collection': 'cases', 'count': nuevo_total})
socketio.emit('update_summary', summary_data)
socketio.emit('update_department', department_data)
socketio.emit('update_timeline', timeline_data)
socketio.emit('update_heatmap', heatmap_data)

# Cuando cambian los fallecidos
socketio.emit('data_changed', {'collection': 'demises', 'count': nuevo_total})
socketio.emit('update_demises_dept', demises_data)
socketio.emit('update_demises_heatmap', heatmap_data)
```

#### Configuracion del Intervalo de Polling

El intervalo es configurable via variable de entorno o WebSocket:

```bash
# Variable de entorno
export REFRESH_EVERY="3.0"

# O en caliente via WebSocket
socket.emit('set_refresh_interval', {seconds: 1.0})
```

### Sistema de Alertas

El sistema monitorea umbrales configurables y genera alertas:
- **Warning**: Valor >= umbral
- **Critical**: Valor >= umbral * 1.5

| Metrica | Umbral Default |
|---------|----------------|
| Total Casos | 1,000,000 |
| Casos Positivos | 1,000 |
| Total Fallecidos | 1,000 |
| Casos por Depto | 2,000 |
| Fallecidos por Depto | 1,000 |
| Hospitalizaciones | 500 |

## Instalacion

### Dependencias Python

```bash
pip install flask flask-socketio flask-cors pymongo
```

### Dependencias Frontend (CDN)

Las librerias se cargan via CDN en `index.html`:
- D3.js v7
- Socket.IO Client 4.7.2
- Leaflet.js 1.9.4
- Leaflet.heat 0.2.0

## Ejecucion

```bash
cd visualization
python app.py
```

Salida esperada:
```
==================================================
COVID-19 Dashboard - Real-time con WebSockets
==================================================
[Mongo] Conectado a mongodb://admin:admin123@localhost:27017/...
[Info] Usando polling para detectar cambios en colecciones time-series
[Polling] Loop iniciado cada 3.0s
[Polling] Conteo inicial - Cases: 4000000, Demises: 220000
```

Abrir: **http://localhost:5006**

## REST API

### Resumen
| Endpoint | Descripcion |
|----------|-------------|
| `GET /api/summary` | Totales: casos, positivos, fallecidos, hospitalizaciones |

### Casos
| Endpoint | Descripcion |
|----------|-------------|
| `GET /api/cases/by-department` | Agregado por departamento |
| `GET /api/cases/by-date` | Serie temporal |
| `GET /api/cases/by-age-group` | Por grupo etario |
| `GET /api/cases/by-sex` | Por genero |
| `GET /api/heatmap` | Datos para mapa de calor |

### Fallecidos
| Endpoint | Descripcion |
|----------|-------------|
| `GET /api/demises/by-department` | Por departamento |
| `GET /api/demises/by-sex` | Por genero |
| `GET /api/heatmap/demises` | Mapa de calor |

### Hospitalizaciones
| Endpoint | Descripcion |
|----------|-------------|
| `GET /api/heatmap/hospitalizations` | Mapa de calor |

### Alertas
| Endpoint | Descripcion |
|----------|-------------|
| `GET /api/alerts/config` | Configuracion umbrales |
| `POST /api/alerts/config` | Actualizar umbral |
| `GET /api/alerts/active` | Alertas activas |
| `GET /api/alerts/history` | Historial |

## Eventos WebSocket

### Server → Client
| Evento | Datos |
|--------|-------|
| `update_summary` | Totales actualizados |
| `update_department` | Casos por departamento |
| `update_sex` | Distribucion por sexo |
| `update_timeline` | Serie temporal casos |
| `update_age` | Casos por edad |
| `update_heatmap` | Datos mapa casos |
| `update_demises_dept` | Fallecidos por depto |
| `update_demises_sex` | Fallecidos por sexo |
| `update_demises_timeline` | Serie temporal fallecidos |
| `update_demises_heatmap` | Mapa fallecidos |
| `update_hospitalizations_heatmap` | Mapa hospitalizaciones |
| `alerts_config` | Config alertas |
| `active_alerts` | Alertas activas |
| `filtered_*` | Datos con filtros aplicados |
| `data_changed` | Notificacion de cambio |

### Client → Server
| Evento | Descripcion |
|--------|-------------|
| `request_refresh` | Forzar actualizacion |
| `request_filtered_data` | Datos filtrados |
| `set_refresh_interval` | Cambiar intervalo polling |
| `update_alert_threshold` | Modificar umbral |
| `set_alerts_global_date` | Filtrar alertas por fecha |
| `dismiss_alert` | Descartar alerta |
| `clear_all_alerts` | Limpiar alertas |

## Conexion con los Pipelines

Este dashboard se conecta a las colecciones MongoDB que son alimentadas por los pipelines de Apache Beam:

| Coleccion | Pipeline Origen | Descripcion |
|-----------|-----------------|-------------|
| `cases` | `pipelines/cases/pipeline.py` | Casos individuales COVID-19 |
| `demises` | `pipelines/demises/pipeline.py` | Datos de fallecimientos |
| `hospitalizations` | `pipelines/hospitalizations/pipeline.py` | Datos de hospitalizaciones |

Los datos son enriquecidos con coordenadas geograficas (UBIGEO -> lat/lon) por el transform `enrich_geo.py` antes de ser almacenados en MongoDB.

## Variables de Entorno

```bash
MONGO_URI="mongodb://admin:admin123@localhost:27017/?authSource=admin&directConnection=true"
MONGO_DB="covid-db"
REFRESH_EVERY="3.0"    # Segundos entre polls
PORT="5006"
```

## Mapas de Calor Georreferenciados

El sistema incluye tres mapas de calor interactivos que visualizan la distribucion geografica de los datos COVID-19 en Peru. Estos mapas utilizan **Leaflet.js** con el plugin **leaflet.heat** para renderizar puntos de intensidad basados en coordenadas geograficas.

### Arquitectura de los Mapas

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      MAPAS DE CALOR - ARQUITECTURA                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────────┐ │
│  │   MongoDB   │    │   Flask     │    │      Frontend (Browser)      │ │
│  │             │    │   Server    │    │                              │ │
│  │ Colecciones │───▶│  Queries    │───▶│  ┌───────────────────────┐  │ │
│  │ - cases     │    │  con coords │    │  │     Leaflet.js        │  │ │
│  │ - demises   │    │             │    │  │  + leaflet.heat       │  │ │
│  │ - hospital  │    │  Agrega por │    │  │                       │  │ │
│  │             │    │  ubicacion  │    │  │  Renderiza:           │  │ │
│  │ + ubigeo    │    │  + lat/lon  │    │  │  - Capa de calor      │  │ │
│  │   coords    │    │             │    │  │  - Marcadores         │  │ │
│  └─────────────┘    └─────────────┘    │  │  - Popups/Tooltips    │  │ │
│                                         │  └───────────────────────┘  │ │
│                                         └─────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

---

### Mapa de Calor de Casos

Visualiza la concentracion geografica de **casos positivos** de COVID-19 en el territorio peruano. Permite identificar los focos de contagio y su evolucion temporal.

| Propiedad | Valor |
|-----------|-------|
| **Evento WebSocket** | `update_heatmap` |
| **Endpoint REST** | `GET /api/heatmap` |
| **Coleccion MongoDB** | `cases` |
| **Filtro** | `resultado: "POSITIVO"` |
| **Agrupacion** | Por departamento/provincia/distrito |

**Query MongoDB (Backend):**

```python
# handlers/queries/cases.py - get_heatmap_data()
pipeline = [
    {"$match": {"sexo": {"$nin": [None, "", "Sin especificar"]}}},
    {
        "$group": {
            "_id": {
                "departamento": "$departamento_paciente",
                "provincia": "$provincia_paciente",
                "distrito": "$distrito_paciente"
            },
            "total": {"$sum": 1},
            "positivos": {"$sum": {"$cond": [{"$eq": ["$resultado", "POSITIVO"]}, 1, 0]}}
        }
    },
    {"$sort": {"total": -1}},
    {"$limit": 200}
]
# Las coordenadas se obtienen de ubigeo_coords o DEPARTAMENTO_COORDS
```

**Renderizado Frontend (heatmaps.js):**

```javascript
// Gradiente de colores (azul → amarillo → rojo)
gradient: {
    0.0: '#313695',  // Azul oscuro (baja intensidad)
    0.2: '#4575b4',
    0.4: '#74add1',
    0.5: '#fee090',  // Amarillo (media)
    0.7: '#f46d43',
    0.9: '#d73027',
    1.0: '#a50026'   // Rojo oscuro (alta intensidad)
}
// Marcadores: color #df4634 (rojo)
```

**Datos del popup:**
- Departamento, Provincia, Distrito
- Total de casos
- Casos positivos
- Coordenadas (lat, lon)

![Mapa de Calor - Casos COVID-19](../doc_tfm/images/mapa-calor-casos.png)

---

### Mapa de Calor de Fallecidos

Muestra la distribucion geografica de **fallecidos** por COVID-19, permitiendo identificar las zonas con mayor mortalidad y correlacionar con factores como acceso a salud.

| Propiedad | Valor |
|-----------|-------|
| **Evento WebSocket** | `update_demises_heatmap` |
| **Endpoint REST** | `GET /api/heatmap/demises` |
| **Coleccion MongoDB** | `demises` |
| **Campo fecha** | `fecha_fallecimiento` |
| **Agrupacion** | Por departamento |

**Configuracion visual:**

```javascript
// Gradiente de colores (gris → purpura → rojo oscuro)
gradient: {
    0.0: '#2c3e50',  // Gris oscuro (baja intensidad)
    0.2: '#8e44ad',  // Purpura
    0.4: '#9b59b6',
    0.6: '#e74c3c',  // Rojo
    0.8: '#c0392b',
    1.0: '#7b241c'   // Rojo muy oscuro (alta intensidad)
}
// Marcadores: color #b65959 (rojo apagado)
```

**Datos del popup:**
- Departamento, Provincia, Distrito
- Total de fallecidos
- Coordenadas (lat, lon)

![Mapa de Calor - Fallecidos COVID-19](../doc_tfm/images/mapa-calor-fallecidos.png)

---

### Mapa de Calor de Hospitalizaciones

Representa la concentracion de **hospitalizaciones** por COVID-19, util para analizar la presion sobre el sistema de salud y la capacidad hospitalaria por region.

| Propiedad | Valor |
|-----------|-------|
| **Evento WebSocket** | `update_hospitalizations_heatmap` |
| **Endpoint REST** | `GET /api/heatmap/hospitalizations` |
| **Coleccion MongoDB** | `hospitalizations` |
| **Campo fecha** | `fecha_ingreso_hosp` |
| **Agrupacion** | Por departamento |

**Configuracion visual:**

```javascript
// Gradiente de colores (azul → naranja)
gradient: {
    0.0: '#1a5276',  // Azul oscuro (baja intensidad)
    0.2: '#2874a6',
    0.4: '#3498db',  // Azul claro
    0.6: '#f39c12',  // Amarillo/naranja
    0.8: '#e67e22',
    1.0: '#d35400'   // Naranja oscuro (alta intensidad)
}
// Marcadores: color #e67e22 (naranja)
```

**Datos del popup:**
- Departamento, Provincia, Distrito
- Total de hospitalizaciones
- Coordenadas (lat, lon)

![Mapa de Calor - Hospitalizaciones COVID-19](../doc_tfm/images/mapa-calor-hospitalizaciones.png)

---

### Configuracion Comun de los Mapas

Todos los mapas comparten la siguiente configuracion base:

```javascript
// Mapa base (CartoDB Dark)
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png')

// Centro del mapa (Peru)
map.setView([-9.19, -75.0152], 5)

// Configuracion de la capa de calor
L.heatLayer(data, {
    radius: 45,        // Radio de cada punto
    blur: 35,          // Difuminado
    minOpacity: 0.25,  // Opacidad minima
    maxZoom: 12,       // Zoom maximo para interpolacion
    max: 1.0           // Intensidad maxima
})

// Calculo de intensidad (normalizacion con raiz cuadrada)
intensity = Math.pow(total / maxTotal, 0.5)
```

### Interactividad

| Accion | Descripcion |
|--------|-------------|
| **Zoom** | Scroll del mouse o botones +/- |
| **Pan** | Arrastrar el mapa |
| **Click en marcador** | Muestra popup con datos detallados |
| **Hover en marcador** | Muestra tooltip con ubicacion |
| **Filtros globales** | Los mapas responden a filtros de departamento y sexo |

### Coordenadas de Departamentos

Las coordenadas geograficas de cada departamento estan definidas en `config.py`:

```python
DEPARTAMENTO_COORDS = {
    "LIMA": {"lat": -12.0464, "lon": -77.0428},
    "AREQUIPA": {"lat": -16.3988, "lon": -71.5350},
    "CUSCO": {"lat": -13.5226, "lon": -71.9674},
    "LA LIBERTAD": {"lat": -8.1150, "lon": -79.0300},
    # ... 25 departamentos del Peru
}
```

## Caracteristicas

- Actualizaciones en tiempo real via polling + WebSockets
- Filtros interactivos por departamento y sexo
- Sistema de alertas con umbrales configurables
- 9 visualizaciones D3.js + Leaflet
- Mapas de calor geograficos de Peru
- Intervalo de refresco ajustable en caliente
- Indicador de estado de conexion
- Notificaciones de cambios en datos
- Tema oscuro
- Diseño responsive
