# Cliente Go-Mini-Spark - Estructura del Código

El cliente ha sido refactorizado en una estructura modular para mejorar la mantenibilidad y organización del código.

## Estructura de Archivos

```
client/
├── main.go         # Punto de entrada, parsing de argumentos y dispatch de comandos
├── types.go        # Definiciones de tipos y estructuras de datos (API models)
├── client.go       # Cliente HTTP y métodos de API
├── handlers.go     # Handlers de comandos CLI
└── utils.go        # Funciones auxiliares y utilidades
```

## Descripción de Archivos

### `main.go`
- **Responsabilidad**: Punto de entrada de la aplicación
- **Contenido**:
  - Parsing de flags y argumentos CLI
  - Dispatch de comandos a handlers correspondientes
  - Manejo de errores global

### `types.go`
- **Responsabilidad**: Definiciones de tipos de datos
- **Contenido**:
  - `JobNode`, `JobDAG`, `JobRequest` - Tipos para batch jobs
  - `TopologyOperator`, `TopologyRequest` - Tipos para streaming
  - `JobResponse`, `ResultsResponse` - Tipos de respuesta
  - `IngestRequest` - Tipo para ingesta de eventos

### `client.go`
- **Responsabilidad**: Cliente HTTP y comunicación con el API
- **Contenido**:
  - Struct `Client` con configuración HTTP
  - `NewClient()` - Constructor del cliente
  - `SubmitJob()` - Enviar jobs batch
  - `SubmitTopology()` - Enviar topologías streaming
  - `GetJobStatus()` - Consultar estado de job
  - `GetTopologyStatus()` - Consultar estado de topología
  - `GetJobResults()` - Obtener resultados
  - `IngestEvents()` - Ingerir eventos

### `handlers.go`
- **Responsabilidad**: Lógica de comandos CLI
- **Contenido**:
  - `submitJobCommand()` - Handler para submit-job
  - `submitTopologyCommand()` - Handler para submit-topology
  - `statusCommand()` - Handler para status
  - `resultsCommand()` - Handler para results
  - `watchCommand()` - Handler para watch
  - `ingestCommand()` - Handler para ingest

### `utils.go`
- **Responsabilidad**: Funciones auxiliares
- **Contenido**:
  - `copyFile()` - Copiar archivos de resultados
  - `printUsage()` - Mostrar ayuda del CLI

## Compilación

El código sigue siendo un solo paquete (`main`), por lo que la compilación es igual:

```bash
# Compilar cliente
go build -o bin/client.exe client/*.go

# O desde el directorio raíz
go build -o bin/client.exe ./client
```

## Ventajas de esta Estructura

### ✅ Mantenibilidad
- Código separado por responsabilidades
- Fácil de encontrar y modificar funcionalidad específica

### ✅ Legibilidad
- Archivos más pequeños y enfocados
- Menos scroll para encontrar código

### ✅ Escalabilidad
- Fácil agregar nuevos comandos en `handlers.go`
- Nuevos tipos en `types.go`
- Nuevos métodos API en `client.go`

### ✅ Testing
- Más fácil escribir tests unitarios por archivo
- Mejor aislamiento de responsabilidades

### ✅ Colaboración
- Menos conflictos en Git
- Cambios más granulares y fáciles de revisar

## Agregar Nuevas Funcionalidades

### Agregar un Nuevo Comando

1. **Definir handler en `handlers.go`**:
```go
func newCommand(client *Client, args []string) error {
    // Implementación
    return nil
}
```

2. **Agregar al switch en `main.go`**:
```go
case "new-command":
    err = newCommand(client, commandArgs)
```

3. **Actualizar ayuda en `utils.go`**:
```go
func printUsage() {
    // Agregar documentación del nuevo comando
}
```

### Agregar un Nuevo Tipo API

1. **Definir en `types.go`**:
```go
type NewRequest struct {
    Field1 string `json:"field1"`
    Field2 int    `json:"field2"`
}
```

2. **Agregar método en `client.go`**:
```go
func (c *Client) NewAPICall(req NewRequest) (*Response, error) {
    // Implementación
}
```

### Agregar Nueva Utilidad

Agregar función en `utils.go`:
```go
func newUtility() error {
    // Implementación
    return nil
}
```

## Dependencias entre Archivos

```
main.go
  ├── importa → types.go (implícitamente, mismo package)
  ├── usa → client.go (NewClient)
  ├── usa → handlers.go (submitJobCommand, etc.)
  └── usa → utils.go (printUsage)

handlers.go
  ├── usa → types.go (JobRequest, JobResponse, etc.)
  ├── usa → client.go (métodos del Client)
  └── usa → utils.go (copyFile)

client.go
  └── usa → types.go (todos los tipos de request/response)

utils.go
  └── independiente (funciones auxiliares puras)
```

## Ejemplo de Flujo

1. Usuario ejecuta: `client submit-job example.json`
2. `main.go` parsea argumentos y flags
3. `main.go` crea `Client` usando `NewClient()` de `client.go`
4. `main.go` llama `submitJobCommand()` de `handlers.go`
5. `submitJobCommand()` lee archivo y crea `JobRequest` de `types.go`
6. `submitJobCommand()` llama `client.SubmitJob()` de `client.go`
7. `client.SubmitJob()` hace HTTP POST y devuelve `JobResponse` de `types.go`
8. `submitJobCommand()` imprime resultado al usuario

## Testing (Futuro)

Con esta estructura, puedes crear tests separados:

```
client/
├── client_test.go     # Tests del cliente HTTP
├── handlers_test.go   # Tests de handlers CLI
├── utils_test.go      # Tests de utilidades
```

## Notas

- Todos los archivos están en el mismo paquete `main`
- No se necesitan imports entre archivos (mismo package)
- El compilador de Go unifica todos los archivos `.go` del directorio
- La estructura facilita futuras refactorizaciones a paquetes separados
