const pool = require("./db");
const { getConnection } = require("./queue");
const logger = require("./logger");
const metrics = require("./metrics");

const BATCH_SIZE = Number(process.env.OUTBOX_BATCH_SIZE || 20);
const POLL_INTERVAL_MS = Number(process.env.OUTBOX_POLL_MS || 1000);
const MAX_PUBLISH_RETRIES = Number(process.env.OUTBOX_MAX_RETRIES || 5);

let channel;
let timer;
let stopping = false;
let ticking = false;

// A confirm channel lets us wait for the broker to acknowledge the publish
// before we mark the row PROCESSED. Without it we would be trusting a fire-and
// -forget write and could lose a message we had already marked as sent.
async function ensureChannel() {
  if (channel) return channel;
  channel = await getConnection().createConfirmChannel();
  channel.on("error", (err) => {
    logger.error("Outbox channel error", { err });
    channel = null;
  });
  channel.on("close", () => {
    channel = null;
  });
  return channel;
}

async function tick() {
  if (stopping || ticking) return;
  ticking = true;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // SKIP LOCKED lets several relay instances drain the same table without
    // fighting over rows or publishing duplicates.
    const res = await client.query(
      `SELECT id, aggregate_type, aggregate_id, event_type, target_queue,
              correlation_id, payload, retry_count
         FROM outbox_events
        WHERE status = 'PENDING'
          AND next_attempt_at <= NOW()
        ORDER BY created_at ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED`,
      [BATCH_SIZE],
    );

    if (res.rowCount === 0) {
      await client.query("COMMIT");
      return;
    }

    const ch = await ensureChannel();

    for (const event of res.rows) {
      const payload =
        typeof event.payload === "string" ? JSON.parse(event.payload) : event.payload;

      // eventId travels with the message so the consumer can dedupe on it.
      const body = Buffer.from(
        JSON.stringify({
          eventId: event.id,
          eventType: event.event_type,
          correlationId: event.correlation_id,
          ...payload,
        }),
      );

      try {
        ch.sendToQueue(event.target_queue, body, {
          persistent: true,
          messageId: event.id,
          correlationId: event.correlation_id || undefined,
          headers: { "x-retry-count": 0 },
        });
        await ch.waitForConfirms();

        await client.query(
          `UPDATE outbox_events
              SET status = 'PROCESSED', processed_at = NOW(), last_error = NULL
            WHERE id = $1`,
          [event.id],
        );
        metrics.inc("outbox_published_total");
        logger.info("Outbox event published", {
          eventId: event.id,
          eventType: event.event_type,
          queue: event.target_queue,
          correlationId: event.correlation_id,
        });
      } catch (err) {
        const attempts = event.retry_count + 1;
        const exhausted = attempts > MAX_PUBLISH_RETRIES;
        const backoffSeconds = Math.min(2 ** attempts, 60);

        await client.query(
          `UPDATE outbox_events
              SET retry_count     = $1,
                  last_error      = $2,
                  status          = $3,
                  next_attempt_at = NOW() + ($4 || ' seconds')::interval
            WHERE id = $5`,
          [attempts, err.message, exhausted ? "FAILED" : "PENDING", backoffSeconds, event.id],
        );

        metrics.inc("outbox_publish_errors_total");
        logger[exhausted ? "error" : "warn"](
          exhausted ? "Outbox event exhausted retries" : "Outbox publish failed, will retry",
          { eventId: event.id, attempts, err },
        );
        channel = null; // force a fresh channel next tick
      }
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    logger.error("Outbox relay tick failed", { err });
  } finally {
    client.release();
    ticking = false;
  }
}

// Self-rescheduling rather than setInterval: an interval fires again while the
// previous async tick is still running, which would double-publish.
function scheduleNext() {
  if (stopping) return;
  timer = setTimeout(async () => {
    await tick();
    scheduleNext();
  }, POLL_INTERVAL_MS);
}

async function startOutboxRelay() {
  stopping = false;
  await ensureChannel();
  logger.info("Outbox relay started", { pollMs: POLL_INTERVAL_MS, batch: BATCH_SIZE });
  scheduleNext();
}

async function stopOutboxRelay() {
  stopping = true;
  if (timer) clearTimeout(timer);
  if (channel) {
    await channel.close().catch(() => {});
    channel = null;
  }
}

module.exports = { startOutboxRelay, stopOutboxRelay };
