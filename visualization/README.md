# COVID-19 Dashboard - Real-time D3.js + MongoDB

Visualización interactiva en **tiempo real** usando D3.js, WebSockets y MongoDB Change Streams.

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                         REAL-TIME FLOW                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌─────────┐    Change     ┌─────────────┐    WebSocket       │
│   │ MongoDB │───Streams────▶│  Flask +    │◀─────────────────┐ │
│   │         │               │  SocketIO   │                  │ │
│   └─────────┘               └──────┬──────┘                  │ │
│        ▲                          │                          │ │
│        │                          │ emit()                   │ │
│        │                          ▼                          │ │
│   ┌─────────┐               ┌─────────────┐    ┌───────────┐ │ │
│   │ Kafka   │───Pipeline───▶│   Browser   │◀───│  D3.js    │ │ │
│   │ + Beam  │               │   Client    │    │  Charts   │ │ │
│   └─────────┘               └─────────────┘    └───────────┘ │ │
│                                                              │ │
└──────────────────────────────────────────────────────────────┘ │
```

## Cómo funciona

1. **MongoDB Change Streams**: El servidor escucha cambios en las colecciones `cases` y `demises`
2. **WebSockets (Socket.IO)**: Cuando hay un cambio, el servidor emite los datos actualizados a todos los clientes
3. **D3.js**: El frontend recibe los datos y actualiza las gráficas con animaciones suaves

**Sin polling** - Las actualizaciones son instantáneas cuando hay cambios en la base de datos.

## Instalación

```bash
cd visualization
pip install -r requirements.txt
```

## Ejecución

```bash
python app.py
```

```
==================================================
COVID-19 Dashboard - Real-time con WebSockets
==================================================
[ChangeStream] Thread iniciado para 'cases'
[ChangeStream] Thread iniciado para 'demises'
[ChangeStream] Escuchando cambios en 'cases'...
[ChangeStream] Escuchando cambios en 'demises'...
```

Abrir en navegador: **http://localhost:5002**

## Eventos WebSocket

| Evento (Server → Client) | Descripción |
|--------------------------|-------------|
| `data_changed` | Notifica que hubo un cambio en una colección |
| `update_summary` | Datos de resumen actualizados |
| `update_department` | Casos por departamento |
| `update_sex` | Distribución por sexo |
| `update_timeline` | Serie temporal |
| `update_age` | Casos por grupo de edad |
| `update_demises_dept` | Fallecidos por departamento |

| Evento (Client → Server) | Descripción |
|--------------------------|-------------|
| `request_refresh` | Solicitar actualización manual |

## REST Endpoints (fallback)

Los endpoints REST siguen disponibles si se necesita acceder a los datos sin WebSocket:

| Endpoint | Descripción |
|----------|-------------|
| `GET /api/summary` | Resumen total |
| `GET /api/cases/by-department` | Por departamento |
| `GET /api/cases/by-date` | Por fecha |
| `GET /api/cases/by-age-group` | Por edad |
| `GET /api/cases/by-sex` | Por sexo |
| `GET /api/demises/by-department` | Fallecidos |

## Variables de Entorno

```bash
export MONGO_URI="mongodb://localhost:27017"
export MONGO_DB="covid_pipeline"
```

## Requisitos MongoDB

Para que Change Streams funcione, MongoDB debe estar configurado como **Replica Set**:

```bash
# Si usas Docker, el replica set ya debería estar configurado
# Si es instalación local, inicializa el replica set:
mongosh --eval "rs.initiate()"
```

## Características

- Actualizaciones en tiempo real (sin polling)
- Indicador de conexión WebSocket
- Notificaciones de cambios
- Animaciones D3.js suaves
- Diseño responsive
- Tema oscuro
