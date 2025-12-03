# Sistema de Procesamiento Distribuido

Un sistema de procesamiento distribuido implementado en Go que soporta trabajos por lotes (batch) y streaming con arquitectura Master-Worker.

## Arquitectura

### Master
- Registro de workers y heartbeats
- Recepción de jobs (Batch) o topologías (Streaming)
- Planificador con política round-robin + awareness de carga
- Persistencia del estado en SQLite local

### Workers
- Ejecución de tareas aisladas
- Manejo de particiones de datos (Batch) o buffers de eventos (Streaming)
- Reintentos automáticos y reportes de estado
- Métricas de rendimiento

### Cliente CLI
- Envío de jobs vía API REST
- Consulta de estado y progreso
- Descarga de resultados

## Instalación

1. Clona el repositorio
2. Instala las dependencias:
   ```bash
   go mod tidy
   ```

## Uso

### Construcción
```bash
scripts\build.bat
```

### Inicio del sistema
```bash
scripts\start.bat
```

### Uso del cliente

El cliente CLI permite interactuar con el sistema distribuido mediante comandos simples.

#### Comandos disponibles
- `submit-job` - Enviar un trabajo para procesamiento
- `status` - Consultar estado de un trabajo
- `results` - Obtener resultados de un trabajo completado  
- `watch` - Monitorear progreso en tiempo real

#### Ejemplos de uso

**Enviar un trabajo:**
```bash
bin\client.exe submit-job <argumentos>
```

**Consultar estado:**
```bash
bin\client.exe status <job_id>
```

**Obtener resultados:**
```bash
bin\client.exe results <job_id>
```

**Monitorear progreso:**
```bash
bin\client.exe watch <job_id>
```

## API del Cliente

### URL Base
```
http://localhost:8080
```

### Content Type
Todas las requests y responses usan `application/json`.

### Códigos de Estado HTTP
- `200 OK` - Éxito
- `201 Created` - Recurso creado
- `400 Bad Request` - Request inválido  
- `404 Not Found` - Recurso no encontrado
- `500 Internal Server Error` - Error del servidor

### Endpoints

#### 1. Enviar Trabajo Batch
**Endpoint:** `POST /api/v1/jobs`

**Request Body:**
```json
{
  "name": "string",
  "dag": {
    "nodes": [
      {
        "id": "string",
        "op": "string", 
        "path": "string (opcional)",
        "partitions": "integer (opcional)",
        "fn": "string (opcional)",
        "key": "string (opcional)",
        "args": {}
      }
    ],
    "edges": [["string", "string"]]
  },
  "parallelism": "integer",
  "config": {}
}
```

**Response:** `201 Created`
```json
{
  "id": "job-abc123",
  "name": "wordcount-batch", 
  "status": "ACCEPTED",
  "progress": 0.0,
  "created_at": "2024-12-02T10:00:00Z",
  "metrics": {}
}
```

#### 2. Obtener Estado del Trabajo
**Endpoint:** `GET /api/v1/jobs/{id}`

**Response:** `200 OK`
```json
{
  "id": "job-abc123",
  "name": "wordcount-batch",
  "status": "RUNNING|ACCEPTED|SUCCEEDED|FAILED",
  "progress": 45.5,
  "created_at": "2024-12-02T10:00:00Z", 
  "completed_at": "2024-12-02T10:05:30Z",
  "metrics": {
    "tasks_completed": 18,
    "tasks_total": 40,
    "tasks_failed": 0,
    "throughput": 1500,
    "cpu_usage": 65.3,
    "memory_usage": 512000000
  },
  "error": "string (si falló)"
}
```

#### 3. Obtener Resultados del Trabajo
**Endpoint:** `GET /api/v1/jobs/{id}/results`

**Response:** `200 OK`
```json
{
  "job_id": "job-abc123",
  "paths": [
    "/output/job-abc123/part-00000.csv",
    "/output/job-abc123/part-00001.csv"
  ],
  "format": "csv|jsonl",
  "size": 1048576
}
```

### Estados de Trabajo
- `ACCEPTED` - Trabajo recibido y en cola
- `RUNNING` - Trabajo ejecutándose
- `SUCCEEDED` - Trabajo completado exitosamente
- `FAILED` - Trabajo falló con error

### Operadores Batch
| Operador | Descripción | Campos Requeridos |
|----------|-------------|-------------------|
| `read_csv` | Leer archivos CSV | `path`, `partitions` |
| `read_jsonl` | Leer archivos JSONL | `path`, `partitions` |
| `map` | Transformar elementos | `fn` |
| `filter` | Filtrar elementos | `fn` |
| `flat_map` | Transformación flat map | `fn` |
| `reduce` | Operación reduce | `fn` |
| `reduce_by_key` | Reduce por clave | `key`, `fn` |
| `join` | Unir dos datasets | `key` |
| `aggregate` | Agregación personalizada | `fn` |

## Estructura del Proyecto

```
.
├── driver/          # Servidor Master
├── worker/          # Servidor Worker
├── client/          # Cliente CLI
├── pkg/
│   ├── types/       # Tipos de datos compartidos
│   └── utils/       # Utilidades comunes
├── scripts/         # Scripts de construcción y ejecución
└── bin/            # Binarios compilados
```

## Características

- **Tolerancia a fallos**: Reintentos automáticos y detección de workers inactivos
- **Balanceeo de carga**: Algoritmo round-robin con awareness de carga
- **Persistencia**: Estado guardado en SQLite
- **Monitoreo**: Heartbeats y métricas de rendimiento
- **Escalabilidad**: Soporte para múltiples workers
- **API REST**: Interfaz HTTP para todas las operaciones
