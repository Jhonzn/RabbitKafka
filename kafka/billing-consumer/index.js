const { Kafka } = require("kafkajs");

const NAME = process.env.CONSUMER_NAME || "billing-consumer";
const BROKER = process.env.KAFKA_BROKER || "localhost:29092";
const TOPIC = process.env.KAFKA_TOPIC || "orders.events";
const GROUP_ID = process.env.KAFKA_GROUP_ID || "billing-service-group";

const kafka = new Kafka({
  clientId: NAME,
  brokers: [BROKER],
  retry: { retries: 10 }
});

const consumer = kafka.consumer({ groupId: GROUP_ID });

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function start() {
  let connected = false;
  let delay = 2000;

  while (!connected) {
    try {
      console.log(`[${NAME}] Conectando a Kafka en ${BROKER}...`);
      await consumer.connect();
      connected = true;
    } catch (error) {
      console.error(`[${NAME}] Kafka no disponible: ${error.message}`);
      await sleep(delay);
      delay = Math.min(delay * 2, 15000);
    }
  }

  await consumer.subscribe({
    topic: TOPIC,
    fromBeginning: true
  });

  console.log(
    `[${NAME}] Escuchando ${TOPIC}; group=${GROUP_ID}`
  );

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const event = JSON.parse(message.value.toString());

        if (event.eventType !== "OrderCreated") {
          console.log(`[${NAME}] Evento ignorado: ${event.eventType}`);
          return;
        }

        const order = event.data;

        console.log(`[${NAME}] Evento recibido: ${event.eventType}`);
        console.log(
          `[${NAME}] orderId=${order.orderId} eventId=${event.eventId} correlationId=${event.correlationId}`
        );
        console.log(
          `[${NAME}] Cliente=${order.customerName}; producto=${order.product}; cantidad=${order.quantity}; total=${order.total}`
        );
        console.log(
          `[${NAME}] ACCION: Simulando generacion de factura`
        );
        console.log(
          `[${NAME}] topic=${topic} partition=${partition} offset=${message.offset}`
        );
        console.log(`[${NAME}] Procesamiento simulado completado.`);
      } catch (error) {
        console.error(`[${NAME}] Error procesando evento:`, error.message);
        throw error;
      }
    }
  });
}

async function shutdown() {
  console.log(`[${NAME}] Cerrando consumidor...`);
  try {
    await consumer.disconnect();
  } catch (error) {
    console.error(`[${NAME}] Error cerrando:`, error.message);
  }
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

start().catch(error => {
  console.error(`[${NAME}] Error fatal:`, error);
  process.exit(1);
});
