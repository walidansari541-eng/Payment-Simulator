const amqp = require("amqplib");
const logger = require("./logger");

const AMQP_URL = process.env.AMQP_URL || "amqp://rabbitmq:5672";

// One queue per logical service, plus the single event bus the orchestrator
// listens on. Commands flow orchestrator -> service; outcome events flow
// service -> orchestrator.
const QUEUES = {
  INVENTORY_COMMANDS: "inventory_commands",
  PAYMENT_COMMANDS: "payment_commands",
  NOTIFICATION_COMMANDS: "notification_commands",
  SAGA_EVENTS: "saga_events",
};

const retryQueueOf = (queue) => `${queue}_retry`;
const dlqOf = (queue) => `${queue}_dlq`;

let connection;

// Each work queue gets two siblings:
//
//   <q>_retry  holds a message for its per-message TTL then dead-letters it back
//              onto <q>. Backoff without parking a prefetch slot in a timer.
//   <q>_dlq    terminal parking for poison messages, so an unparseable or
//              unexpectedly-failing message is inspectable instead of dropped.
async function assertQueueSet(channel, queue) {
  await channel.assertQueue(queue, { durable: true });
  await channel.assertQueue(dlqOf(queue), { durable: true });
  await channel.assertQueue(retryQueueOf(queue), {
    durable: true,
    arguments: {
      "x-dead-letter-exchange": "",
      "x-dead-letter-routing-key": queue,
    },
  });
}

async function assertTopology(channel) {
  for (const queue of Object.values(QUEUES)) {
    await assertQueueSet(channel, queue);
  }
}

async function connectQueue({ assertAll = true } = {}) {
  connection = await amqp.connect(AMQP_URL, {
    recovery: { initialDelay: 500, maxDelay: 10000 },
  });

  connection.on("error", (err) => logger.error("AMQP connection error", { err }));
  connection.on("close", () => logger.warn("AMQP connection closed"));

  if (assertAll) {
    const channel = await connection.createChannel();
    await assertTopology(channel);
    await channel.close();
  }

  logger.info("Connected to RabbitMQ", { url: AMQP_URL });
  return connection;
}

function getConnection() {
  if (!connection) throw new Error("RabbitMQ connection not initialized");
  return connection;
}

async function closeQueue() {
  if (!connection) return;
  try {
    await connection.close();
  } catch {
    // already gone
  }
  connection = null;
}

module.exports = {
  QUEUES,
  retryQueueOf,
  dlqOf,
  assertQueueSet,
  assertTopology,
  connectQueue,
  getConnection,
  closeQueue,
};
