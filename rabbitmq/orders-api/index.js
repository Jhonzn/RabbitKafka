
const express = require("express");
const amqp = require("amqplib");
const { randomUUID } = require("node:crypto");

const app = express();

app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const RABBITMQ_URL =
  process.env.RABBITMQ_URL || "amqp://admin:admin@localhost:5672";

const QUEUE = "pedidos";

let channel;
let connection;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function connectRabbitMQ() {
  while (!channel) {
    try {
      console.log("Conectando con RabbitMQ...");

      connection = await amqp.connect(RABBITMQ_URL);
      channel = await connection.createConfirmChannel();

      await channel.assertQueue(QUEUE, {
        durable: true,
      });

      connection.on("error", (error) => {
        console.error("Error en la conexión RabbitMQ:", error.message);
      });

      connection.on("close", () => {
        console.error("Conexión RabbitMQ cerrada. Reintentando...");
        channel = undefined;
        connection = undefined;
      });

      console.log(`Conectado a RabbitMQ. Cola: ${QUEUE}`);
      return;
    } catch (error) {
      console.error(
        "RabbitMQ todavía no está disponible:",
        error.message
      );

      channel = undefined;
      connection = undefined;

      await sleep(3000);
    }
  }
}

app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    service: "orders-api",
    rabbitmqConnected: Boolean(channel),
  });
});

app.post("/orders", async (req, res) => {
  try {
    const { customerName, product, quantity } = req.body || {};

    if (
      typeof customerName !== "string" ||
      !customerName.trim() ||
      typeof product !== "string" ||
      !product.trim() ||
      !Number.isInteger(quantity) ||
      quantity < 1
    ) {
      return res.status(400).json({
        error:
          "Envía customerName, product y quantity (entero mayor que cero).",
      });
    }

    if (!channel) {
      return res.status(503).json({
        error: "RabbitMQ no está disponible. Intenta de nuevo.",
      });
    }

    const orderId = randomUUID();

    const order = {
      orderId,
      customerName: customerName.trim(),
      product: product.trim(),
      quantity,
      createdAt: new Date().toISOString(),
    };

    const message = Buffer.from(JSON.stringify(order));

    // Publicamos un mensaje persistente y esperamos la confirmación
    // del broker, pero no esperamos al trabajador.
    await channel.waitForConfirms();

    channel.sendToQueue(QUEUE, message, {
      persistent: true,
      contentType: "application/json",
      messageId: orderId,
      correlationId: orderId,
      timestamp: Date.now(),
      type: "OrderCreated",
    });

    await channel.waitForConfirms();

    console.log(
      `[API] Orden publicada: orderId=${orderId}`
    );

    return res.status(202).json({
      message: "Orden aceptada para procesamiento.",
      order,
      status: "accepted",
    });
  } catch (error) {
    console.error("Error creando la orden:", error.message);

    return res.status(500).json({
      error: "No fue posible registrar la orden.",
    });
  }
});

async function start() {
  await connectRabbitMQ();

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`orders-api escuchando en el puerto ${PORT}`);
  });
}

start().catch((error) => {
  console.error("No fue posible iniciar la API:", error);
  process.exit(1);
});

async function shutdown() {
  console.log("Cerrando API...");

  try {
    if (channel) await channel.close();
    if (connection) await connection.close();
  } catch (error) {
    console.error("Error cerrando RabbitMQ:", error.message);
  }

  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
