# Informe de casos de prueba exitosos — Kafka

**Proyecto:** Taller 4 – Pruebas de integración en arquitecturas basadas en mensajes y eventos (RabbitMQ y Kafka)
**Fecha de ejecución:** 30/10/2026
**Alcance de este informe:** casos de la sección Kafka con evidencia real y resultado satisfactorio. Las evidencias están en `evidencias/kafka/`.

## 1. Entorno de prueba

| Elemento | Detalle |
|---|---|
| Orquestación | Docker Compose (kafka, kafka-init, kafka-ui, orders-api, 3 consumidores) |
| Broker | Apache Kafka 3.7 (imagen bitnami), 1 broker, replication-factor 1 |
| Tópico | `orders.events`, 2 particiones (creado por `kafka-init`) |
| Productor | `orders-api` (Node.js 20, Express, KafkaJS) — `POST http://localhost:3000/orders` |
| Consumidores | `inventory-consumer` (`inventory-service-group`), `billing-consumer` (`billing-service-group`), `notification-consumer` (`notification-service-group`) |
| Suscripción | `fromBeginning: true`, un *consumer group* distinto por servicio |
| Herramientas | Postman, PowerShell (`crear-orden.ps1`), Docker Desktop, Kafka UI (puerto 8080) |

Evidencia de infraestructura: [EVIDENCIA_KAFKA_01_Contenedores_Infraestructura.png](../kafka/EVIDENCIA_KAFKA_01_Contenedores_Infraestructura.png)

## 2. Resumen de resultados

| ID | Caso de prueba | Estado | Evidencia |
|---|---|---|---|
| CP-01 | Creación de pedido vía API | ✅ Exitoso | `EVIDENCIA_KAFKA_CP01_01_Respuesta_API.jpg` |
| CP-04 | Publicación de evento en Kafka | ✅ Exitoso | `EVIDENCIA_KAFKA_CP04_01_Evento_Publicado.jpg` |
| CP-05 | Consumo de evento | ✅ Exitoso | `EVIDENCIA_KAFKA_CP05_01_Consumidores.jpg` |
| CP-06 | Trazabilidad del flujo | ✅ Exitoso | `EVIDENCIA_KAFKA_CP06_01_Trazabilidad.jpg` |
| CP-07 | Responsive (respuesta sin esperar al consumidor) | ✅ Exitoso | `EVIDENCIA_KAFKA_CP07_01_Respuesta_Asincrona.jpg` |
| CP-08 | Message-driven (sin comunicación directa) | ✅ Exitoso | `EVIDENCIA_KAFKA_CP08_01_Desacoplamiento.jpg` |
| CP-09 | Resiliencia ante caída de un consumidor | ✅ Exitoso | `EVIDENCIA_KAFKA_CP09_01_…`, `CP09_02_…` |
| CP-10 | Elastic (distribución entre instancias) | ✅ Exitoso | `EVIDENCIA_KAFKA_CP10_01_…` a `CP10_03_…` |
| CP-11 | Desacoplamiento temporal | ✅ Exitoso | `EVIDENCIA_KAFKA_CP11_01_…`, `CP11_02_…` |
| CP-12 | Recuperación tras reinicio de consumidor y broker | ✅ Exitoso | `EVIDENCIA_KAFKA_CP12_01_…`, `CP12_02_…` |

**10 de 10 casos de Kafka ejecutados fueron exitosos.** CP-02 y CP-03 corresponden a RabbitMQ y no forman parte de este informe.

---

## 3. Detalle por caso

### CP-01 — Creación de pedido vía API

- **Descripción:** validar que el endpoint recibe y responde correctamente.
- **Precondición:** contenedores en ejecución, `orders-api` escuchando en el puerto 3000.
- **Entrada:** `POST /orders` con `{"customerName":"Pedro","product":"Libro de arquitectura","quantity":1,"unitPrice":85000}`.
- **Resultado esperado:** respuesta de aceptación con `orderId`.
- **Resultado observado:** `202 Accepted` en 40 ms con `{"status":"accepted","message":"Pedido recibido y evento OrderCreated publicado en Kafka","orderId":"ORD-19dd5b90-cff2-45ed-95c0-4fe4424976aa"}`. El log de `orders-api` registra `OrderCreated publicado` con el mismo `orderId`.
- **Evidencia:** ![CP-01](../kafka/EVIDENCIA_KAFKA_CP01_01_Respuesta_API.jpg)
- **Estado:** ✅ Exitoso

### CP-04 — Publicación de evento en Kafka

- **Descripción:** verificar que se genera el evento `OrderCreated`.
- **Resultado esperado:** el evento queda almacenado en `orders.events`.
- **Resultado observado:** Kafka UI muestra el mensaje en el tópico (partición 0, offset 3) con clave = `orderId`, `eventType: "OrderCreated"`, `eventVersion: "1.0"`, `source: "orders-api"`, `correlationId` y el bloque `data` (cliente, producto, cantidad 1, precio unitario 85000, total 85000). Timestamp 30/10/2026 8:36:31.
- **Evidencia:** ![CP-04](../kafka/EVIDENCIA_KAFKA_CP04_01_Evento_Publicado.jpg)
- **Estado:** ✅ Exitoso

### CP-05 — Consumo de evento

- **Descripción:** validar que los consumidores reciben el evento.
- **Resultado esperado:** los tres servicios procesan el mismo evento.
- **Resultado observado:** `inventory-consumer`, `billing-consumer` y `notification-consumer` registran `Evento recibido: OrderCreated`, cada uno con su acción simulada (reserva de inventario, generación de factura, envío de notificación) y `topic=orders.events partition=0 offset=3`. Cada servicio usa un grupo distinto, por lo que todos reciben el mismo evento.
- **Evidencia:** ![CP-05](../kafka/EVIDENCIA_KAFKA_CP05_01_Consumidores.jpg)
- **Estado:** ✅ Exitoso

### CP-06 — Trazabilidad del flujo

- **Descripción:** seguir un mismo pedido por API, broker y consumidores.
- **Resultado esperado:** los identificadores coinciden en todos los puntos.
- **Resultado observado:**

| Punto | orderId | eventId | correlationId |
|---|---|---|---|
| API (log) | `ORD-5cafa4a4-4553-4cd9-8d70-9159df96badf` | `56eafd5c-a9d7-405e-b71f-db14ae2a0b33` | `3b962513-5bcc-400b-98a2-69b397ada4be` |
| Broker (Kafka UI) | igual | igual | igual |
| inventory / billing / notification | igual | igual | igual |

- **Evidencia:** ![CP-06](../kafka/EVIDENCIA_KAFKA_CP06_01_Trazabilidad.jpg)
- **Estado:** ✅ Exitoso

### CP-07 — Responsive

- **Descripción:** la API responde sin esperar al procesamiento de los consumidores.
- **Resultado esperado:** respuesta inmediata `202` independiente del consumo.
- **Resultado observado:** la API responde `202 Accepted` en 40 ms, tras confirmar la publicación en Kafka (`producer.send`), sin invocar a los consumidores. El procesamiento se registra después, de forma independiente, en los logs de cada servicio.
- **Evidencia:** ![CP-07](../kafka/EVIDENCIA_KAFKA_CP07_01_Respuesta_Asincrona.jpg)
- **Nota:** el tiempo de 40 ms es consistente con la tesis pero, por sí solo, no la demuestra; la prueba más contundente es que `inventory-consumer` estaba detenido (CP-09) y la API siguió respondiendo `202`.
- **Estado:** ✅ Exitoso

### CP-08 — Message-driven

- **Descripción:** no existe comunicación directa entre servicios.
- **Resultado esperado:** los servicios solo se comunican a través de Kafka.
- **Resultado observado:** el productor únicamente ejecuta `producer.send` al tópico `orders.events` (con cabeceras `eventType`, `eventId`, `correlationId`). Cada consumidor se configura en `docker-compose.yml` solo con `KAFKA_BROKER`, `KAFKA_TOPIC` y `KAFKA_GROUP_ID`, y se suscribe con `consumer.subscribe`. No hay URLs ni llamadas HTTP entre servicios.
- **Evidencia:** ![CP-08](../kafka/EVIDENCIA_KAFKA_CP08_01_Desacoplamiento.jpg)
- **Estado:** ✅ Exitoso

### CP-09 — Resiliencia ante caída de un consumidor

- **Descripción:** el sistema soporta la caída de un consumidor.
- **Procedimiento:** `docker compose stop inventory-consumer`; se envían 3 pedidos con `New-Order 3`.
- **Resultado esperado:** la API sigue aceptando pedidos y los demás consumidores siguen procesando.
- **Resultado observado:** el contenedor `inventory-consumer` queda detenido (Docker Desktop lo muestra apagado y el resto activo). Los 3 pedidos devuelven `accepted`. `billing-consumer` y `notification-consumer` procesan los 3 eventos (offsets 1959 y 1960 en partición 1, 2002 en partición 0).
- **Evidencia:**
  ![CP-09-01](../kafka/EVIDENCIA_KAFKA_CP09_01_Consumidor_Detenido.png)
  ![CP-09-02](../kafka/EVIDENCIA_KAFKA_CP09_02_Recuperacion.png)
- **Estado:** ✅ Exitoso

### CP-10 — Elastic: distribución de carga entre instancias del mismo grupo

- **Descripción:** distribución de carga con múltiples consumidores.
- **Procedimiento:** `docker compose up -d --scale inventory-consumer=2` (instancias `kafka-inventory-consumer-1` y `-2`, mismo grupo `inventory-service-group`); luego `New-Order 12`.
- **Resultado esperado:** las dos instancias se reparten las particiones de `orders.events` y procesan eventos distintos.
- **Resultado observado:** las 12 solicitudes devuelven `accepted`. Los logs de inventario muestran que cada pedido lo procesa una sola instancia, y que cada una trabaja una partición distinta:

| Instancia | Partición | Offsets | Pedidos |
|---|---|---|---|
| `inventory-consumer-2` | 1 | 1965 – 1969 | Pedro 1, 3, 6, 7, 8 (5 pedidos) |
| `inventory-consumer-1` | 0 | 2011 – 2017 | Pedro 2, 4, 5, 9, 10, 11, 12 (7 pedidos) |

  Los 12 pedidos quedaron procesados exactamente una vez dentro del grupo (5 + 7). La distribución no es 6/6 porque Kafka asigna particiones completas y la clave `orderId` reparte los mensajes entre ellas.
- **Evidencia:**
  ![CP-10-01](../kafka/EVIDENCIA_KAFKA_CP10_01_Dos_Instancias.png)
  ![CP-10-02](../kafka/EVIDENCIA_KAFKA_CP10_02_Envio_Pedidos.png)
  ![CP-10-03a](../kafka/EVIDENCIA_KAFKA_CP10_03_Distribucion-1.png)
  ![CP-10-03b](../kafka/EVIDENCIA_KAFKA_CP10_03_Distribucion-2.png)
- **Estado:** ✅ Exitoso

### CP-11 — Desacoplamiento temporal

- **Descripción:** los eventos publicados con el consumidor apagado se procesan cuando este se inicia.
- **Procedimiento:** con `inventory-consumer` detenido y 3 eventos publicados, `docker compose start inventory-consumer`.
- **Resultado esperado:** el consumidor procesa los eventos pendientes sin pérdida.
- **Resultado observado:** el consumidor se reconecta, se une al grupo `inventory-service-group` (asignación de las particiones `[0,1]`) y procesa los 3 eventos publicados durante su ausencia: `orderId` ORD-da952a8a… (p1/offset 1959), ORD-51521764… (p1/offset 1960) y ORD-569e8887… (p0/offset 2002). Coinciden con los procesados por billing y notification en CP-09.
- **Evidencia:**
  ![CP-11-1](../kafka/EVIDENCIA_KAFKA_CP11_01_Evento_Consumidor_Apagado.png)
  ![CP-11-2](../kafka/EVIDENCIA_KAFKA_CP11_02_Consumo_Posterior.png)
- **Estado:** ✅ Exitoso

### CP-12 — Recuperación tras reinicio de consumidor y broker

- **Descripción:** tras reiniciar el consumidor o el broker, los eventos pendientes se procesan sin pérdida.
- **Parte A — reinicio del consumidor:** `docker compose restart billing-consumer` y, mientras se reinicia, `New-Order 1` (`ORD-43762979-764f-4b6c-a491-8cfeb88d2b83`, `accepted`). Los logs muestran los reintentos de conexión, `Cerrando consumidor...`, el nuevo arranque, la reincorporación al grupo `billing-service-group` (particiones `[0,1]`) y el procesamiento del evento (`partition=0 offset=2019`) con el mismo `orderId`.
- **Parte B — reinicio del broker:** estado del grupo `billing-service-group` antes y después de `docker compose restart kafka` y de un nuevo pedido (`ORD-f9078235-8cbb-489a-885f-f7b81352b779`):

| Momento | Partición 0 (current / log-end) | Partición 1 (current / log-end) | LAG |
|---|---|---|---|
| Antes del reinicio de Kafka | 2020 / 2020 | 1970 / 1970 | 0 |
| Después del reinicio + 1 pedido | 2021 / 2021 | 1970 / 1970 | 0 |

  Tras reiniciar el broker, los offsets continúan desde donde estaban (no vuelven a 0), lo que confirma que Kafka conservó los eventos y los offsets del grupo. El consumidor se reconectó (nuevo `CONSUMER-ID`), procesó el nuevo evento y el lag quedó en 0. `docker compose ps` muestra todos los servicios activos y Kafka `healthy`.
- **Resultado esperado:** sin pérdida de eventos ni de offsets tras el reinicio.
- **Resultado observado:** cumplido en ambas partes.
- **Evidencia:**
  ![CP-12-01](../kafka/EVIDENCIA_KAFKA_CP12_01_Reinicio_Consumidor.png)
  ![CP-12-02](../kafka/EVIDENCIA_KAFKA_CP12_02_Reinicio_Broker_Offsets.png)
- **Estado:** ✅ Exitoso

---

## 4. Conclusiones

- Los 10 casos de Kafka ejecutados fueron **exitosos**: la API acepta el pedido, publica `OrderCreated` en `orders.events` y los tres consumidores lo procesan de forma independiente.
- **Trazabilidad:** `orderId`, `eventId` y `correlationId` se conservan de extremo a extremo.
- **Desacoplamiento y resiliencia:** la caída o el reinicio de un consumidor no afectó a la API ni a los demás servicios, y Kafka retuvo los eventos hasta que el consumidor volvió, sin pérdida.
- **Elasticidad:** dos instancias del mismo grupo se repartieron las dos particiones; con 2 particiones, una tercera instancia quedaría inactiva.
- **Persistencia:** el reinicio del broker conservó los eventos y los offsets de los grupos.
- **Limitaciones:** el broker es único (replication-factor 1) y el procesamiento es simulado, por lo que las conclusiones aplican al comportamiento de integración, no a tolerancia a fallos del broker ni a carga real.
