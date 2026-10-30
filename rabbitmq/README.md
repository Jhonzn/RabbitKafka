# Pruebas de Integración — RabbitMQ

**Asignatura:** Aseguramiento de la Calidad del Software  
**Arquitectura:** Basada en mensajes con RabbitMQ  
**Entorno:** Windows 11, PowerShell, Docker Compose y Node.js

## 1. Objetivo

Validar la integración asíncrona entre una API de pedidos, RabbitMQ y un consumidor de notificaciones mediante pruebas de publicación, consumo, trazabilidad, desacoplamiento, tolerancia a fallos, distribución de mensajes y recuperación.

## 2. Arquitectura

```text
Cliente / PowerShell
        |
        | HTTP POST /orders
        v
   orders-api :3000
        |
        | Publicación AMQP
        v
 RabbitMQ / cola "pedidos"
        |
        +-----------------------+
        |                       |
        v                       v
notification-worker-1   notification-worker-2
```

La API publica mensajes en RabbitMQ. Los workers consumen de la cola `pedidos` y procesan los pedidos. La comunicación entre la API y los consumidores se realiza mediante el broker, sin necesidad de una llamada directa entre ambos servicios.

## 3. Requisitos previos

- Windows 10 u 11.
- Docker Desktop instalado y ejecutándose.
- Docker Compose disponible mediante `docker compose`.
- PowerShell.
- Navegador web.

Si Node.js y las dependencias están dentro de las imágenes de Docker, no es necesario instalar Node.js localmente para ejecutar la arquitectura.

## 4. Estructura del proyecto

```text
rabbitmq/
├── docker-compose.yml
├── orders-api/
│   ├── Dockerfile
│   ├── package.json
│   ├── package-lock.json
│   └── index.js
└── notification-worker/
    ├── Dockerfile
    ├── package.json
    ├── package-lock.json
    └── index.js
```

Las capturas de las pruebas se almacenan en `evidencias/rabbitmq`.

## 5. Levantar el entorno

### 5.1. Entrar a la carpeta correcta

Abre PowerShell y ejecuta:

```powershell
cd "C:\Users\jhonf\OneDrive\Desktop\semestre 8\aseguramiento de la calidad del software\Arquitecturas Basadas en Mensajes y Eventos\rabbitmq"
```

Comprueba que el archivo de configuración exista:

```powershell
Test-Path .\docker-compose.yml
```

El resultado debe ser `True`.

> Importante: ejecuta los comandos de Docker Compose desde la carpeta del proyecto, no desde `evidencias\rabbitmq`.

### 5.2. Construir e iniciar los servicios

```powershell
docker compose up -d --build
```

Comprueba el estado:

```powershell
docker compose ps
```

Servicios y puertos esperados:

| Servicio | Puerto | Función |
|---|---:|---|
| `orders-api` | 3000 | API HTTP |
| `rabbitmq` | 5672 | Comunicación AMQP |
| `rabbitmq` | 15672 | Consola web |
| `notification-worker` | Sin puerto publicado | Consumidor de mensajes |

RabbitMQ debe aparecer como `healthy` y los servicios deben estar activos.

Si hay problemas, consulta los registros:

```powershell
docker compose logs --tail 100
```

### 5.3. Abrir la consola de RabbitMQ

Accede a:

http://localhost:15672

Credenciales utilizadas en el laboratorio:

- Usuario: `admin`
- Contraseña: `admin`

Entra a **Queues and Streams** y selecciona la cola `pedidos`.

Estas credenciales son para el entorno de práctica; no deben reutilizarse en producción.

### 5.4. Verificar la API

Si la API tiene implementada la ruta `/health`, ejecuta:

```powershell
Invoke-RestMethod -Uri "http://localhost:3000/health"
```

Si no existe esa ruta, verifica la API mediante el CP-01.

### 5.5. Detener y reiniciar servicios

Detener:

```powershell
docker compose stop
```

Volver a iniciar:

```powershell
docker compose start
```

Eliminar contenedores y redes de Compose:

```powershell
docker compose down
```

No uses `docker compose down -v` si necesitas conservar los volúmenes de datos de la práctica.

---

# 6. Casos de prueba

## CP-01 — Crear pedido mediante la API

**Objetivo:** comprobar que la API acepta un pedido válido y devuelve un identificador.

**Precondiciones:** API y RabbitMQ activos.

### Procedimiento

```powershell
$pedido = @{
    customerName = "Prueba CP01"
    product      = "Pedido de integracion"
    quantity     = 1
} | ConvertTo-Json

$respuesta = Invoke-RestMethod `
    -Uri "http://localhost:3000/orders" `
    -Method Post `
    -ContentType "application/json" `
    -Body $pedido

$respuesta | ConvertTo-Json -Depth 5
```

### Validación

- La solicitud HTTP termina correctamente.
- La respuesta contiene un estado de aceptación.
- La respuesta incluye un `orderId`.
- Los datos devueltos coinciden con los enviados.
- El registro de la API muestra la publicación del pedido.

**Evidencia:** captura de la solicitud y respuesta JSON.

**Estado:** falta identificar o confirmar la captura específica de creación. La captura de CP-07 puede servir también si muestra la solicitud y la respuesta completas.

## CP-02 — Enviar mensaje a RabbitMQ

**Objetivo:** verificar que la API publica un mensaje en la cola `pedidos`.

### Procedimiento

Envía un pedido con el comando del CP-01 y consulta:

```powershell
docker compose logs --since 2m orders-api
```

Abre la consola RabbitMQ, entra a la cola `pedidos` y revisa sus mensajes.

### Validación

- La API registra la publicación.
- La cola existe.
- El `orderId` coincide entre la respuesta de la API y el registro.
- Si el consumidor está apagado, el mensaje puede permanecer visible como pendiente.

Con el consumidor activo, el mensaje puede ser procesado inmediatamente y dejar de aparecer como pendiente.

**Evidencias:**

- `EVIDENCIA_RMQ_02_Cola_Pedidos.png`
- `EVIDENCIA_RMQ_CODIGO_01_Publicacion_API.png`

**Estado:** ejecutada; confirmar que las capturas demuestran la publicación.

## CP-03 — Consumir y procesar un mensaje

**Objetivo:** comprobar que el worker recibe y procesa mensajes de la cola.

### Procedimiento

Envía un pedido y ejecuta:

```powershell
docker compose logs --since 2m notification-worker
```

Para consultar API y workers juntos:

```powershell
docker compose logs --since 2m orders-api notification-worker
```

### Validación

- El worker se conecta a RabbitMQ.
- El worker escucha la cola `pedidos`.
- Aparece el mensaje recibido con su `orderId`.
- Se muestran los datos del pedido.
- Aparece `Orden procesada correctamente` con el mismo identificador.

**Evidencias:**

- `EVIDENCIA_RMQ_03_Orden_Procesada.png`
- `EVIDENCIA_RMQ_CODIGO_02_Consumo_Worker.png`

**Estado:** ejecutada; existen registros de pedidos procesados correctamente.

## CP-04 — Publicación de evento en Kafka

Este caso corresponde a la arquitectura Kafka, no a RabbitMQ.

**Estado:** pendiente de documentar en el proyecto Kafka.

## CP-05 — Consumo de evento en Kafka

Este caso corresponde a la arquitectura Kafka, no a RabbitMQ.

**Estado:** pendiente de documentar en el proyecto Kafka.

## CP-06 — Trazabilidad del pedido mediante `orderId`

**Objetivo:** comprobar que el mismo pedido puede rastrearse desde la API hasta el consumidor.

### Procedimiento

Envía un pedido nuevo y conserva el `orderId` de la respuesta. Después ejecuta:

```powershell
docker compose logs --since 2m orders-api notification-worker
```

### Validación

- La API registra la publicación con un identificador.
- El worker registra la recepción y el procesamiento con ese mismo identificador.
- No se deben comparar pedidos diferentes.

**Evidencias:**

- `EVIDENCIA_RMQ_06_Trazabilidad_OrderId.png`
- `EVIDENCIA_RMQ_CP07_02_Logs_Procesamiento.png`, si muestra el mismo identificador.

**Estado:** evidencia disponible; confirmar la coincidencia exacta del `orderId`.

## CP-07 — Respuesta asíncrona de la API

**Objetivo:** verificar que la API acepta el pedido sin esperar a que el worker termine todo el procesamiento.

### Procedimiento

```powershell
$tiempo = Measure-Command {
    $respuesta = Invoke-RestMethod `
        -Uri "http://localhost:3000/orders" `
        -Method Post `
        -ContentType "application/json" `
        -Body '{"customerName":"Prueba CP07 Final","product":"Medicion respuesta asincrona","quantity":1}'
}

Write-Host "`nRESPUESTA DE LA API:"
$respuesta | ConvertTo-Json -Depth 5

Write-Host "`nTIEMPO DE RESPUESTA:"
Write-Host "$([math]::Round($tiempo.TotalMilliseconds, 2)) ms"
```

Consulta los registros:

```powershell
docker compose logs --since 2m orders-api notification-worker
```

### Validación

- La API responde con el pedido aceptado.
- La API registra la publicación.
- El worker procesa el mismo `orderId`.
- Los registros permiten comprobar que la API no espera a que termine el procesamiento del worker.

El tiempo de `Measure-Command` mide la duración de la solicitud HTTP desde PowerShell. Por sí solo no demuestra que el procesamiento sea asíncrono; debe contrastarse con los registros.

Una comprobación adicional consiste en detener los consumidores, enviar un pedido y verificar que la API responde mientras RabbitMQ continúa disponible.

**Evidencias:**

- `EVIDENCIA_RMQ_07_Respuesta_Asincrona.png`
- `EVIDENCIA_RMQ_CP07_01_Respuesta_API.png`
- `EVIDENCIA_RMQ_CP07_02_Logs_Procesamiento.png`

**Estado:** se realizaron capturas; confirmar que muestran el tiempo real y la secuencia de respuesta y procesamiento.

## CP-08 — Comunicación mediante mensajes

**Objetivo:** verificar que la API y el worker están desacoplados y utilizan RabbitMQ como intermediario.

### Validación del código

Revisa `orders-api/index.js` y `notification-worker/index.js`.

Comprueba que:

- La API publica mensajes en RabbitMQ.
- El worker consume de la cola `pedidos`.
- La API no realiza una llamada HTTP directa al worker para procesar el pedido.
- Ambos servicios utilizan el broker para intercambiar los mensajes de negocio.

La prueba de consumidores apagados también ayuda a demostrar que la API puede publicar sin que un worker esté activo, siempre que RabbitMQ esté disponible.

**Evidencias:**

- `EVIDENCIA_RMQ_CODIGO_01_Publicacion_API.png`
- `EVIDENCIA_RMQ_CODIGO_02_Consumo_Worker.png`

**Estado:** evidencia de código disponible; confirmar la revisión de ambos archivos y la ausencia de llamadas directas.

## CP-09 — Tolerancia a la caída del consumidor

**Objetivo:** comprobar que los pedidos pueden publicarse mientras los consumidores están apagados y procesarse cuando vuelven a estar disponibles.

### Procedimiento

Detén los consumidores:

```powershell
docker compose stop notification-worker
docker compose ps
```

RabbitMQ y la API deben seguir activos.

Envía un pedido mediante el comando del CP-01 y revisa la cola `pedidos` en la consola web.

Después reinicia los consumidores:

```powershell
docker compose up -d --scale notification-worker=2
docker compose logs --since 2m notification-worker
```

### Validación

- La API acepta el pedido mientras los consumidores están apagados.
- El mensaje permanece pendiente en RabbitMQ.
- Al volver a iniciar los consumidores, el pedido se procesa con el mismo `orderId`.

**Evidencias:**

- `EVIDENCIA_RMQ_04_Mensaje_Pendiente_Consumidor_Apagado.png`
- `EVIDENCIA_RMQ_05_Recuperacion_Consumidor-1.png`
- `EVIDENCIA_RMQ_05_Recuperacion_Consumidor-2.png`

**Estado:** escenario de consumidor apagado y recuperación ejecutado.

## CP-10 — Distribución con múltiples consumidores

**Objetivo:** comprobar que varios consumidores pueden procesar mensajes de una misma cola.

### Procedimiento ejecutado

Se iniciaron dos instancias:

```powershell
docker compose up -d --scale notification-worker=2
docker compose ps
```

Se enviaron seis pedidos:

```powershell
1..6 | ForEach-Object {
    $pedido = @{
        customerName = "Prueba CP10"
        product      = "Pedido de distribucion $_"
        quantity     = 1
    } | ConvertTo-Json

    Invoke-RestMethod `
        -Uri "http://localhost:3000/orders" `
        -Method Post `
        -ContentType "application/json" `
        -Body $pedido
}
```

Se consultaron los registros:

```powershell
docker compose logs --since 2m notification-worker
```

### Resultado observado

Los registros mostraron que:

- `notification-worker-1` procesó los pedidos 1, 3 y 5.
- `notification-worker-2` procesó los pedidos 2, 4 y 6.
- Los seis pedidos finalizaron correctamente.

**Evidencia:** `EVIDENCIA_RMQ_10_Distribucion_Multiples_Consumidores.png`

**Estado:** aprobada con base en los registros obtenidos.

## CP-11 — Desacoplamiento temporal

**Objetivo:** verificar que los mensajes pueden publicarse mientras no hay consumidores y procesarse cuando estos vuelven a estar disponibles.

### Procedimiento ejecutado

1. Se detuvieron los consumidores.
2. Se envió un pedido mediante la API.
3. Se observó el mensaje pendiente en la cola.
4. Se reiniciaron los consumidores.
5. Se comprobó el procesamiento del pedido.

Comandos principales:

```powershell
docker compose stop notification-worker
```

Envía el pedido con el procedimiento del CP-01 y revisa la cola en http://localhost:15672.

Después:

```powershell
docker compose up -d --scale notification-worker=2
docker compose logs --since 2m notification-worker
```

### Validación

El mensaje debe permanecer en la cola cuando no hay consumidores y procesarse al volver a iniciarlos.

**Evidencias:**

- `EVIDENCIA_RMQ_04_Mensaje_Pendiente_Consumidor_Apagado.png`
- `EVIDENCIA_RMQ_05_Recuperacion_Consumidor-1.png`
- `EVIDENCIA_RMQ_05_Recuperacion_Consumidor-2.png`

**Estado:** escenario ejecutado. Conviene verificar que el mismo `orderId` se vea en las capturas.

## CP-12 — Recuperación de mensajes pendientes

**Objetivo:** comprobar que un mensaje pendiente se procesa cuando se reinician los consumidores.

### Procedimiento ejecutado

Se detuvieron las dos instancias:

```powershell
docker compose stop notification-worker
docker compose ps
```

La salida mostró solo la API y RabbitMQ activos.

Se envió el pedido:

```powershell
$pedido = @{
    customerName = "Prueba CP12"
    product      = "Pedido de recuperacion"
    quantity     = 1
} | ConvertTo-Json

$respuesta = Invoke-RestMethod `
    -Uri "http://localhost:3000/orders" `
    -Method Post `
    -ContentType "application/json" `
    -Body $pedido

$respuesta | ConvertTo-Json -Depth 5
```

La API respondió con `accepted` y el siguiente identificador:

```text
f59c85df-020a-4a2a-bb94-3e49e4bcd0ed
```

Se reiniciaron los dos consumidores:

```powershell
docker compose up -d --scale notification-worker=2
docker compose logs --since 2m notification-worker
```

### Resultado observado

`notification-worker-2` recibió y procesó el pedido `f59c85df-020a-4a2a-bb94-3e49e4bcd0ed`.

**Evidencias:**

- `EVIDENCIA_RMQ_12_01_Mensaje_Pendiente.png`
- `EVIDENCIA_RMQ_12_02_Recuperacion_Mensaje.png`

**Estado:** escenario de recuperación de mensaje pendiente ejecutado.

**Limitación:** esta prueba demuestra que un mensaje pendiente se procesó cuando volvieron los consumidores. No demuestra por sí sola la recuperación de un mensaje entregado pero sin ACK ni la recuperación después de reiniciar RabbitMQ. Si la guía exige esos escenarios, deben ejecutarse por separado.

---

# 7. Inventario de evidencias

Ubicación: `evidencias/rabbitmq/`

| Archivo | CP relacionado |
|---|---|
| `EVIDENCIA_RMQ_01_Contenedores_Activos.png` | Preparación del entorno |
| `EVIDENCIA_RMQ_02_Cola_Pedidos.png` | CP-02 |
| `EVIDENCIA_RMQ_03_Orden_Procesada.png` | CP-03 |
| `EVIDENCIA_RMQ_04_Mensaje_Pendiente_Consumidor_Apagado.png` | CP-09 y CP-11 |
| `EVIDENCIA_RMQ_05_Recuperacion_Consumidor-1.png` | CP-09 y CP-11 |
| `EVIDENCIA_RMQ_05_Recuperacion_Consumidor-2.png` | CP-09 y CP-11 |
| `EVIDENCIA_RMQ_06_Trazabilidad_OrderId.png` | CP-06 |
| `EVIDENCIA_RMQ_07_Respuesta_Asincrona.png` | CP-07 |
| `EVIDENCIA_RMQ_10_Distribucion_Multiples_Consumidores.png` | CP-10 |
| `EVIDENCIA_RMQ_12_01_Mensaje_Pendiente.png` | CP-12 |
| `EVIDENCIA_RMQ_12_02_Recuperacion_Mensaje.png` | CP-12 |
| `EVIDENCIA_RMQ_CODIGO_01_Publicacion_API.png` | Código de CP-02 y CP-08 |
| `EVIDENCIA_RMQ_CODIGO_02_Consumo_Worker.png` | Código de CP-03 y CP-08 |
| `EVIDENCIA_RMQ_CP07_01_Respuesta_API.png` | CP-07; CP-01 si muestra la creación completa |
| `EVIDENCIA_RMQ_CP07_02_Logs_Procesamiento.png` | CP-06 y CP-07 si muestra el mismo `orderId` |

Las capturas de código respaldan la implementación, pero no reemplazan las evidencias de ejecución. Antes de entregar, verifica el contenido real de cada captura.

# 8. Comandos rápidos de diagnóstico

Estado de los servicios:

```powershell
docker compose ps
```

Logs de todos los servicios:

```powershell
docker compose logs --tail 100
```

Logs recientes de API y consumidores:

```powershell
docker compose logs --since 2m orders-api notification-worker
```

Logs de RabbitMQ:

```powershell
docker compose logs --tail 100 rabbitmq
```

Levantar dos consumidores:

```powershell
docker compose up -d --scale notification-worker=2
```

Detener los consumidores:

```powershell
docker compose stop notification-worker
```

Si aparece `no configuration file provided`, comprueba la ruta:

```powershell
Get-Location
Test-Path .\docker-compose.yml
```

El segundo comando debe devolver `True`.

# 9. Conclusiones

Completa esta sección después de cerrar las verificaciones pendientes. Describe, a partir de las evidencias, qué se comprobó sobre publicación y consumo, trazabilidad, respuesta asíncrona, desacoplamiento y distribución entre consumidores. Documenta también las limitaciones observadas.

