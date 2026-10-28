
const express = require("express");
const { Kafka } = require("kafkajs");
const { randomUUID } = require("node:crypto");

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const BROKER = process.env.KAFKA_BROKER || "localhost:29092";
const TOPIC = process.env.KAFKA_TOPIC || "orders.events";

const kafka = new Kafka({
  clientId: "orders-api",
  brokers: [BROKER],
  retry: {
    retries: 10
  }
});

const producer = kafka.producer();
let connected = false;

app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    service: "orders-api",
    kafkaConnected: connected
  });
});

app.post("/orders", async (req, res) => {
  try {
    const { customerName, product, quantity, unitPrice } = req.body || {};

    if (
      typeof customerName !== "string" ||
      !customerName.trim() ||
      typeof product !== "string" ||
      !product.trim() ||
      !Number.isInteger(quantity) ||
      quantity < 1 ||
      typeof unitPrice !== "number" ||
      !Number.isFinite(unitPrice) ||
      unitPrice < 0
    ) {
      return res.status(400).json({
        status: "error",
        message: "Envía customerName, product, quantity y unitPrice válidos."
      });
    }

    if (!connected) {
      return res.status(503).json({
        status: "unavailable",
        message: "Kafka no está disponible."
      });
    }

    const orderId = `ORD-${randomUUID()}`;
    const eventId = randomUUID();
    const correlationId = randomUUID();
    const occurredAt = new Date().toISOString();

    const event = {
      eventId,
      eventType: "OrderCreated",
      eventVersion: "1.0",
      occurredAt,
      source: "orders-api",
      correlationId,
      data: {
        orderId,
        customerName: customerName.trim(),
        product: product.trim(),
        quantity,
        unitPrice,
        total: quantity * unitPrice
      }
    };

    await producer.send({
      topic: TOPIC,
      acks: -1,
      messages: [
        {
          key: orderId,
          value: JSON.stringify(event),
          headers: {
            eventType: "OrderCreated",
            eventId,
            correlationId
          }
        }
      ]
    });

    console.log(
      `[API] OrderCreated publicado: orderId=${orderId} eventId=${eventId} correlationId=${correlationId}`
    );

    return res.status(202).json({
      status: "accepted",
      message: "Pedido recibido y evento OrderCreated publicado en Kafka",
      orderId
    });
  } catch (error) {
    console.error("[API] Error publicando evento:", error.message);
    return res.status(500).json({
      status: "error",
      message: "No fue posible publicar el evento."
    });
  }
});

async function start() {
  let delay = 2000;

  while (!connected) {
    try {
      console.log(`[API] Conectando con Kafka en ${BROKER}...`);
      await producer.connect();
      connected = true;
      console.log(`[API] Productor conectado. Tópico: ${TOPIC}`);
    } catch (error) {
      console.error(`[API] Kafka no disponible: ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, delay));
      delay = Math.min(delay * 2, 15000);
    }
  }

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`[API] Escuchando en puerto ${PORT}`);
  });
}

async function shutdown() {
  console.log("[API] Cerrando productor...");
  try {
    if (connected) await producer.disconnect();
  } catch (error) {
    console.error("[API] Error cerrando productor:", error.message);
  }
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

start().catch(error => {
  console.error("[API] Error fatal:", error);
  process.exit(1);
});
