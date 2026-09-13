const { getConnection, retryQueueOf, dlqOf, assertQueueSet } = require("./queue");
const { TransientError } = require("./errors");
const logger = require("./logger");
const metrics = require("./metrics");

const MAX_RETRIES = Number(process.env.CONSUMER_MAX_RETRIES || 3);

// Shared consume loop for every service. Handlers only express business logic;
// acking, backoff, poison-message parking and graceful shutdown live here.
//
//   handler(message, ctx) where message is the parsed body
//     resolves            -> ack
//     throws Transient    -> requeue via the delay queue, up to MAX_RETRIES,
//                            then park on the DLQ
//     throws anything else-> park on the DLQ immediately (a bug or bad data;
//                            retrying it just burns the same failure in a loop)
//
// Nothing is ever dropped silently: an unhandled message ends up on <queue>_dlq
// where it can be inspected and replayed.
async function startConsumer({ queue, consumerName, handler, prefetch = 5 }) {
  const channel = await getConnection().createChannel();
  await assertQueueSet(channel, queue);
  await channel.prefetch(prefetch);

  const inFlight = new Set();
  let closing = false;

  const parkOnDlq = (msg, reason) => {
    channel.sendToQueue(dlqOf(queue), msg.content, {
      persistent: true,
      headers: { ...(msg.properties.headers || {}), "x-death-reason": reason },
    });
    metrics.inc("events_dlq_total", { queue });
  };

  const { consumerTag } = await channel.consume(queue, async (msg) => {
    if (!msg) return;

    const work = (async () => {
      let body;
      try {
        body = JSON.parse(msg.content.toString());
      } catch (err) {
        logger.error("Malformed message parked on DLQ", { queue, err });
        parkOnDlq(msg, "malformed-json");
        channel.ack(msg);
        return;
      }

      const log = logger.child({
        queue,
        consumer: consumerName,
        correlationId: body.correlationId,
        eventId: body.eventId,
        eventType: body.eventType,
      });

      const headers = msg.properties.headers || {};
      // 1-based number of *this* delivery. Handlers that must reach a terminal
      // business state rather than be silently parked (payments, compensation)
      // use it to decide when to give up deliberately.
      const attempt = (headers["x-retry-count"] || 0) + 1;

      try {
        await handler(body, { log, channel, attempt, maxRetries: MAX_RETRIES });
        channel.ack(msg);
      } catch (err) {
        if (err instanceof TransientError && attempt <= MAX_RETRIES) {
          const delayMs = Math.min(2 ** attempt * 1000, 30000);
          log.warn("Transient failure, scheduling retry", {
            attempt,
            maxRetries: MAX_RETRIES,
            delayMs,
            err,
          });
          channel.sendToQueue(retryQueueOf(queue), msg.content, {
            persistent: true,
            expiration: String(delayMs),
            headers: { ...headers, "x-retry-count": attempt },
          });
          metrics.inc("consumer_retry_total", { queue });
          channel.ack(msg);
          return;
        }

        const reason =
          err instanceof TransientError ? "retries-exhausted" : `unhandled:${err.name}`;
        log.error("Message parked on DLQ", { attempt, reason, err });
        parkOnDlq(msg, reason);
        channel.ack(msg);
      }
    })();

    inFlight.add(work);
    work.finally(() => inFlight.delete(work));
    await work;
  });

  logger.info("Consumer started", { queue, consumerName, prefetch });

  // Graceful shutdown: stop taking new work, let in-flight handlers finish so we
  // never lose an ack, then close.
  async function stop() {
    if (closing) return;
    closing = true;
    logger.info("Consumer draining", { queue, inFlight: inFlight.size });
    await channel.cancel(consumerTag).catch(() => {});
    await Promise.allSettled([...inFlight]);
    await channel.close().catch(() => {});
    logger.info("Consumer stopped", { queue });
  }

  return { stop };
}

module.exports = { startConsumer, MAX_RETRIES };
