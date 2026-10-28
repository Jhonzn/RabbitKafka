
const amqp = require("amqplib");

const RABBITMQ_URL =
  process.env.RABBITMQ_URL || "amqp://admin:admin@localhost:5672";

const QUEUE = "pedidos";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function startWorker() {
  console.log("[WORKER] Iniciando trabajador de notificaciones...");

  let connection;

  while (!connection) {
    try {
      connection = await amqp.connect(RABBITMQ_URL);
      console.log("[WORKER] Conectado a RabbitMQ.");
    } catch (error) {
      console.error(
        "[WORKER] RabbitMQ no está disponible:",
        error.message
      );
      await sleep(3000);
    }
  }

  connection.on("error", (error) => {
    console.error("[WORKER] Error de conexión:", error.message);
  });

  connection.on("close", () => {
    console.error(
      "[WORKER] Conexión cerrada. El contenedor deberá reiniciarse."
    );
      process.exit(1);
  });

  const channel = await connection.createChannel();

  await channel.assertQueue(QUEUE, {
    durable: true,
  });

  // Cada instancia procesa un mensaje no confirmado a la vez.
  channel.prefetch(1);

  console.log(`[WORKER] Escuchando la cola "${QUEUE}".`);

  await channel.consume(
    QUEUE,
    async (message) => {
      if (message === null) {
        console.log("[WORKER] El consumidor fue cancelado.");
        return;
      }

      let order;

      try {
        order = JSON.parse(message.content.toString());

        console.log(
          `[WORKER] Mensaje recibido: orderId=${order.orderId}`
        );

        console.log(
          `[WORKER] Cliente=${order.customerName}, ` +
          `producto=${order.product}, cantidad=${order.quantity}`
        );

        // Simula el tiempo de procesamiento de una notificación.
        await sleep(2000);

        console.log(
          `[WORKER] Orden procesada correctamente: ${order.orderId}`
        );

        // Confirma el mensaje solamente después de procesarlo.
        channel.ack(message);
      } catch (error) {
        console.error(
          "[WORKER] Error procesando mensaje:",
          error.message
        );

        // Evita un ciclo infinito si el mensaje tiene datos inválidos.
        channel.nack(message, false, false);
      }
    },
    {
      noAck: false,
    }
  );
}

startWorker().catch((error) => {
  console.error("[WORKER] Error fatal:", error);
  process.exit(1);
});
