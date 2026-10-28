# Taller de Pruebas de Integración: Kafka

## 1. Descripción del proyecto

Implementación de una arquitectura orientada a eventos utilizando Apache Kafka, Docker Compose, Node.js y Kafka UI.

La API `orders-api` recibe solicitudes HTTP para crear pedidos y publica eventos `OrderCreated` en el tópico `orders.events`. Tres consumidores independientes reaccionan al evento:

- `inventory-consumer`: simula la reserva de inventario.
- `billing-consumer`: simula la generación de una factura.
- `notification-consumer`: simula el envío de una notificación.

Cada consumidor utiliza un *consumer group* diferente, de modo que todos puedan recibir el mismo evento.

## 2. Tecnologías utilizadas

- Docker y Docker Compose.
- Apache Kafka 3.7.
- Kafka UI.
- Node.js 20.
- Express.
- KafkaJS.

## 3. Estructura del proyecto

```text
kafka/
├── docker-compose.yml
├── orders-api/
│   ├── Dockerfile
│   ├── package.json
│   └── index.js
├── inventory-consumer/
│   ├── Dockerfile
│   ├── package.json
│   └── index.js
├── billing-consumer/
│   ├── Dockerfile
│   ├── package.json
│   └── index.js
├── notification-consumer/
│   ├── Dockerfile
│   ├── package.json
│   └── index.js
└── evidencias/
```

## 4. Puertos utilizados

| Puerto | Uso |
|---|---|
| 3000 | API REST |
| 8080 | Kafka UI |
| 9092 | Comunicación interna con Kafka |
| 29092 | Acceso externo a Kafka desde el equipo anfitrión |

## 5. Requisitos previos

- Docker Desktop instalado y en ejecución.
- PowerShell.
- Conexión a Internet para descargar las imágenes y dependencias.
- Archivos del proyecto creados según la guía operativa Kafka.

## 6. Cómo ejecutar el proyecto

### 6.1. Entrar a la carpeta

```powershell
cd "$HOME\OneDrive\Desktop\semestre 8\aseguramiento de la calidad del software\Arquitecturas Basadas en Mensajes y Eventos\kafka"
```

### 6.2. Validar Docker Compose

```powershell
docker compose config --quiet
```

Si el comando no muestra errores, la configuración YAML es válida.

### 6.3. Construir e iniciar los servicios

```powershell
docker compose up -d --build
```

### 6.4. Verificar los contenedores

```powershell
docker compose ps -a
```

El entorno debe incluir:

- `kafka`
- `kafka-ui`
- `orders-api`
- `inventory-consumer`
- `billing-consumer`
- `notification-consumer`

El servicio `kafka-init` puede aparecer como finalizado correctamente, porque su función es crear el tópico.

### 6.5. Consultar los registros

```powershell
docker compose logs --tail 60 kafka kafka-ui kafka-init orders-api inventory-consumer billing-consumer notification-consumer
```

Para observar los registros en tiempo real:

```powershell
docker compose logs -f orders-api inventory-consumer billing-consumer notification-consumer
```

Para salir de la visualización, presionar `Ctrl + C`. Esto no detiene los contenedores.

## 7. Verificar el tópico

Consultar los tópicos disponibles:

```powershell
docker compose exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Consultar las particiones de `orders.events`:

```powershell
docker compose exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders.events
```

Resultado esperado: el tópico `orders.events` existe y tiene dos particiones.

## 8. Kafka UI

Abrir en el navegador:

http://localhost:8080

Verificar:

1. Existencia del tópico `orders.events`.
2. Número de particiones.
3. Mensajes publicados y contenido de los eventos.
4. Grupos `inventory-service-group`, `billing-service-group` y `notification-service-group`.
5. Avance de offsets después del consumo.

## 9. Crear un pedido de prueba

Una vez que la API esté funcionando, ejecutar desde PowerShell:

```powershell
$body = @{
    customerName = "Pedro"
    product      = "Libro de arquitectura"
    quantity     = 1
    unitPrice    = 85000
} | ConvertTo-Json

$response = Invoke-RestMethod `
    -Uri "http://localhost:3000/orders" `
    -Method Post `
    -ContentType "application/json" `
    -Body $body

$response | ConvertTo-Json -Depth 5
```

Respuesta esperada:

```json
{
  "status": "accepted",
  "message": "Pedido recibido y evento OrderCreated publicado en Kafka",
  "orderId": "ORD-..."
}
```

El identificador concreto lo genera la aplicación y cambiará en cada pedido.

Después, verificar los registros:

```powershell
docker compose logs --since 2m orders-api inventory-consumer billing-consumer notification-consumer
```

Los tres consumidores deberían mostrar el mismo `orderId` para ese evento, junto con el tópico, la partición y el offset.

## 10. Pruebas de integración para Kafka

Ejecutar y documentar los siguientes casos:

| Caso | Prueba |
|---|---|
| CP-01 | Creación del pedido mediante la API |
| CP-04 | Publicación del evento en Kafka |
| CP-05 | Consumo del evento |
| CP-06 | Trazabilidad del pedido mediante identificadores |
| CP-07 | Respuesta de la API sin esperar al procesamiento de los consumidores |
| CP-08 | Comunicación desacoplada mediante Kafka |
| CP-09 | Comportamiento ante la caída de un consumidor |
| CP-10 | Distribución de carga entre instancias del mismo grupo |
| CP-11 | Publicación mientras un consumidor está apagado |
| CP-12 | Recuperación después de reiniciar un consumidor o broker |

Cada caso debe registrar precondiciones, datos de entrada, pasos, resultado esperado, resultado observado, evidencia, estado y análisis técnico.

## 11. Pruebas especiales

### 11.1. Consumidor apagado

Detener temporalmente el consumidor de inventario:

```powershell
docker compose stop inventory-consumer
```

Enviar un pedido desde la API y comprobar que el evento queda disponible en el tópico. Después, iniciar el consumidor:

```powershell
docker compose start inventory-consumer
```

Verificar si procesa el evento publicado mientras estaba apagado.

**Importante:** Kafka conserva los eventos en el tópico, pero el resultado del consumo depende del grupo, sus offsets y la configuración del consumidor. La prueba debe comprobarse en los registros.

### 11.2. Múltiples instancias del mismo consumidor

La guía exige dos o más instancias del mismo consumidor, con el mismo grupo, para probar distribución de carga.

Antes de escalar `inventory-consumer`, retirar del servicio su propiedad `container_name` si existe. Después ejecutar:

```powershell
docker compose up -d --scale inventory-consumer=2
```

Enviar varios pedidos y revisar si las instancias procesan registros de las particiones disponibles.

Dos particiones permiten como máximo dos consumidores activos de ese grupo asignados a esas particiones simultáneamente.

### 11.3. Recuperación

Para probar la recuperación, detener el consumidor, publicar un evento y volver a iniciarlo. Verificar que se procesa sin perder el registro.

Para una prueba de reinicio del broker, primero confirmar que los eventos están publicados y definir cómo se comprobarán los offsets y la persistencia antes y después del reinicio.

## 12. Nomenclatura de las evidencias

Guardar las capturas en `kafka/evidencias/`.

| Archivo sugerido | Evidencia |
|---|---|
| `EVIDENCIA_KAFKA_01_Contenedores_Infraestructura.png` | Contenedores e infraestructura |
| `EVIDENCIA_KAFKA_02_Topico_Particiones.png` | Tópico `orders.events` y dos particiones |
| `EVIDENCIA_KAFKA_03_Kafka_UI.png` | Interfaz Kafka UI |
| `EVIDENCIA_KAFKA_CP01_01_Respuesta_API.png` | Respuesta de creación del pedido |
| `EVIDENCIA_KAFKA_CP01_02_Log_API.png` | Registro de publicación de la API |
| `EVIDENCIA_KAFKA_CP04_01_Evento_Publicado.png` | Evento visible en el tópico |
| `EVIDENCIA_KAFKA_CP05_01_Consumidores.png` | Consumo por los tres servicios |
| `EVIDENCIA_KAFKA_CP06_01_Trazabilidad.png` | Identificadores coincidentes en los registros |
| `EVIDENCIA_KAFKA_CP07_01_Respuesta_Asincrona.png` | Respuesta de la API y procesamiento posterior |
| `EVIDENCIA_KAFKA_CP08_01_Desacoplamiento.png` | Código y configuración que demuestran ausencia de llamadas directas |
| `EVIDENCIA_KAFKA_CP09_01_Consumidor_Detenido.png` | Consumidor detenido durante la prueba |
| `EVIDENCIA_KAFKA_CP09_02_Recuperacion.png` | Resultado observado después de reiniciar el consumidor |
| `EVIDENCIA_KAFKA_CP10_01_Dos_Instancias.png` | Dos instancias del mismo grupo |
| `EVIDENCIA_KAFKA_CP10_02_Distribucion.png` | Registros que evidencian la distribución |
| `EVIDENCIA_KAFKA_CP11_01_Evento_Consumidor_Apagado.png` | Evento publicado durante la ausencia del consumidor |
| `EVIDENCIA_KAFKA_CP11_02_Consumo_Posterior.png` | Evento procesado al volver a ejecutarlo |
| `EVIDENCIA_KAFKA_CP12_01_Antes_Recuperacion.png` | Estado previo al reinicio |
| `EVIDENCIA_KAFKA_CP12_02_Despues_Recuperacion.png` | Estado y registros posteriores al reinicio |

Las capturas deben mostrar resultados reales. Si una prueba no se ha ejecutado, no se debe crear una captura que aparente demostrarla.

## 13. Detener el entorno

Para detener los servicios sin eliminar los datos persistentes:

```powershell
docker compose down
```

Para eliminar también el volumen persistente de Kafka, solo cuando sea necesario y después de guardar las evidencias:

```powershell
docker compose down -v
```

**Advertencia:** el segundo comando elimina los datos guardados en el volumen, incluidos los eventos del broker.

## 14. Conclusión

La evaluación final debe comparar RabbitMQ y Kafka a partir de los resultados obtenidos. Debe considerar la entrega de mensajes, la persistencia de eventos, el desacoplamiento, la recuperación, el comportamiento de los consumidores y la trazabilidad.

Las conclusiones se redactarán después de ejecutar las pruebas y revisar sus evidencias, evitando atribuir resultados que no hayan sido observados.
