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

#### Enviar un trabajo batch
```bash
bin\client.exe localhost:8080 submit batch "Mi Trabajo"
bin\client.exe localhost:8080 submit batch "Multiplicar" '{"operation":"multiply","factor":3,"task_count":5}'
```

#### Consultar estado de un trabajo
```bash
bin\client.exe localhost:8080 status <job_id>
```

#### Esperar completación de un trabajo
```bash
bin\client.exe localhost:8080 wait <job_id>
```

## API REST

### Master (puerto 8080)

#### Registrar Worker
```http
POST /api/workers/register
Content-Type: application/json

{
  "address": "localhost:8081",
  "max_tasks": 10
}
```

#### Heartbeat
```http
POST /api/workers/heartbeat
Content-Type: application/json

{
  "worker_id": "worker-id",
  "status": "active",
  "active_tasks": 2,
  "metrics": {},
  "timestamp": "2024-01-01T12:00:00Z"
}
```

#### Enviar Trabajo
```http
POST /api/jobs
Content-Type: application/json

{
  "name": "Mi Trabajo",
  "type": "batch",
  "config": {
    "operation": "multiply",
    "factor": 2,
    "task_count": 3
  }
}
```

#### Consultar Estado del Trabajo
```http
GET /api/jobs/{job_id}
```

### Worker (puerto 808X)

#### Estado del Worker
```http
GET /api/status
```

## Estructura del Proyecto

```
.
├── master/          # Servidor Master
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
