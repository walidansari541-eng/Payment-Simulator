// ---------------------------------------------------------------------------
// Notification service — the fourth saga step.
//
// Its compensation is deliberately a no-op, and that is a design statement:
// a sent notification cannot be unsent. If the saga had to unwind after this
// point the correct compensation would be to send a *correction* message, not
// to pretend the first one never happened. Not every step has a symmetric undo.
// ---------------------------------------------------------------------------

require("dotenv").config();
process.env.SERVICE_NAME = process.env.SERVICE_NAME || "notification-service";

const { QUEUES, connectQueue } = require("../queue");
const { startConsumer } = require("../consumerRunner");
const { startOutboxRelay, stopOutboxRelay } = require("../outboxRelay");
const { emitSagaEvent } = require("../outbox");
const { withInbox } = require("../inbox");
const { startMetricsServer } = require("../metricsServer");
const { installSignalHandlers, onShutdown, waitForDependencies } = require("../lifecycle");
const logger = require("../logger");
const metrics = require("../metrics");

const CONSUMER = "notification-service";

async function handleCommand(msg, { log }) {
  const { eventId, eventType, correlationId, orderId, amount, currency, paymentId } = msg;

  if (eventType !== "SEND_NOTIFICATION") {
    log.warn("Unknown notification command ignored", { eventType });
    return;
  }

  const outcome = await withInbox(CONSUMER, eventId, async (client) => {
    const body = `Payment confirmed for order ${orderId}: ${amount} ${currency}`;

    await client.query(
      `INSERT INTO notifications (correlation_id, order_id, channel, body)
       VALUES ($1, $2, 'EMAIL', $3)
       ON CONFLICT (correlation_id) DO NOTHING`,
      [correlationId, orderId, body],
    );

    await emitSagaEvent(client, {
      aggregateType: "NOTIFICATION",
      aggregateId: paymentId || orderId,
      eventType: "NOTIFICATION_SENT",
      correlationId,
      payload: { orderId },
    });

    return { body };
  });

  if (outcome.duplicate) {
    metrics.inc("events_duplicate_total", { consumer: CONSUMER });
    return;
  }

  metrics.inc("notification_sent_total");
  log.info("Notification sent", { orderId });
}

async function main() {
  installSignalHandlers();
  await waitForDependencies();
  await connectQueue();

  const consumer = await startConsumer({
    queue: QUEUES.NOTIFICATION_COMMANDS,
    consumerName: CONSUMER,
    prefetch: Number(process.env.NOTIFICATION_PREFETCH || 5),
    handler: handleCommand,
  });

  await startOutboxRelay();
  const metricsServer = startMetricsServer();
  onShutdown("metrics", metricsServer.stop);
  onShutdown("outbox-relay", stopOutboxRelay);
  onShutdown("consumer", consumer.stop);
  logger.info("Notification service ready");
}

main().catch((err) => {
  logger.error("Fatal startup error", { err });
  process.exit(1);
});
