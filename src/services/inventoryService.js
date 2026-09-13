// ---------------------------------------------------------------------------
// Inventory service.
//
// Owns two operations and their relationship:
//   RESERVE_INVENTORY  — the forward action
//   RELEASE_INVENTORY  — its compensating transaction
//
// "Compensation" is not a rollback. The reservation really happened and was
// visible to everyone; releasing it is a second, deliberate business operation
// that undoes its effect. It has to be written by hand, per operation, and it
// has to be idempotent — which is what the reservations table is for.
// ---------------------------------------------------------------------------

require("dotenv").config();
process.env.SERVICE_NAME = process.env.SERVICE_NAME || "inventory-service";

const pool = require("../db");
const { QUEUES, connectQueue } = require("../queue");
const { TransientError } = require("../errors");
const { startConsumer } = require("../consumerRunner");
const { startOutboxRelay, stopOutboxRelay } = require("../outboxRelay");
const { emitSagaEvent } = require("../outbox");
const { withInbox } = require("../inbox");
const { startMetricsServer } = require("../metricsServer");
const { installSignalHandlers, onShutdown, waitForDependencies } = require("../lifecycle");
const logger = require("../logger");
const metrics = require("../metrics");

const CONSUMER = "inventory-service";

// Failure injection: forces every release to fail so the "compensation itself
// failed" path can be exercised on demand.
const FAIL_RELEASE = process.env.INVENTORY_FAIL_RELEASE === "true";

const normalizeItems = (items) =>
  (Array.isArray(items) && items.length ? items : [{ sku: "SKU-1", quantity: 1 }]).map((i) => ({
    sku: i.sku || "SKU-1",
    quantity: Number(i.quantity || 1),
  }));

async function reserve(client, { correlationId, orderId, items }) {
  const lines = normalizeItems(items);

  // Claim the reservation first. The PK on correlation_id means a duplicate
  // command can never double-reserve, even if the inbox were bypassed.
  const claim = await client.query(
    `INSERT INTO reservations (correlation_id, order_id, items, status)
     VALUES ($1, $2, $3, 'RESERVED')
     ON CONFLICT (correlation_id) DO NOTHING
     RETURNING correlation_id`,
    [correlationId, orderId, JSON.stringify(lines)],
  );
  if (claim.rowCount === 0) {
    return { ok: true, alreadyReserved: true };
  }

  for (const line of lines) {
    // The WHERE clause is the concurrency control: overselling is impossible
    // because the decrement only applies if the stock is actually there.
    const res = await client.query(
      `UPDATE inventory
          SET available = available - $1, reserved = reserved + $1
        WHERE sku = $2 AND available >= $1
        RETURNING available`,
      [line.quantity, line.sku],
    );

    if (res.rowCount === 0) {
      // Abort the whole reservation; the surrounding transaction rolls back the
      // lines already decremented in this loop.
      return { ok: false, reason: `insufficient stock for ${line.sku}` };
    }
  }

  return { ok: true };
}

async function release(client, { correlationId }) {
  if (FAIL_RELEASE) {
    throw new Error("INVENTORY_FAIL_RELEASE is set: simulated release failure");
  }

  // Only a reservation still marked RESERVED may be released. Releasing twice
  // must not credit stock twice — this guard, not the inbox, is the real
  // protection, because a human replaying a command from the DLQ bypasses the
  // inbox entirely.
  const claim = await client.query(
    `UPDATE reservations
        SET status = 'RELEASED', released_at = NOW()
      WHERE correlation_id = $1 AND status = 'RESERVED'
      RETURNING items`,
    [correlationId],
  );

  if (claim.rowCount === 0) {
    return { ok: true, alreadyReleased: true };
  }

  const lines = claim.rows[0].items;
  for (const line of lines) {
    await client.query(
      `UPDATE inventory
          SET available = available + $1, reserved = GREATEST(reserved - $1, 0)
        WHERE sku = $2`,
      [line.quantity, line.sku],
    );
  }
  return { ok: true };
}

async function handleCommand(msg, { log, attempt, maxRetries }) {
  const { eventId, eventType, correlationId, orderId, items } = msg;

  if (eventType === "RESERVE_INVENTORY") {
    // Inbox pattern: dedupe row and business work commit together, so an
    // at-least-once redelivery is a genuine no-op.
    const outcome = await withInbox(CONSUMER, eventId, async (client) => {
      const result = await reserve(client, { correlationId, orderId, items });

      await emitSagaEvent(client, {
        aggregateType: "INVENTORY",
        aggregateId: orderId,
        eventType: result.ok ? "INVENTORY_RESERVED" : "INVENTORY_FAILED",
        correlationId,
        payload: { orderId, items, reason: result.reason },
      });
      return result;
    });

    if (outcome.duplicate) {
      metrics.inc("events_duplicate_total", { consumer: CONSUMER });
      return;
    }
    log.info(outcome.result.ok ? "Inventory reserved" : "Inventory reservation failed", {
      reason: outcome.result.reason,
    });
    return;
  }

  if (eventType === "RELEASE_INVENTORY") {
    try {
      const outcome = await withInbox(CONSUMER, `${eventId}`, async (client) => {
        const result = await release(client, { correlationId });
        await emitSagaEvent(client, {
          aggregateType: "INVENTORY",
          aggregateId: orderId,
          eventType: "INVENTORY_RELEASED",
          correlationId,
          payload: { orderId, alreadyReleased: result.alreadyReleased || false },
        });
        return result;
      });

      if (outcome.duplicate) {
        metrics.inc("events_duplicate_total", { consumer: CONSUMER });
        return;
      }
      log.info("Inventory released (compensation applied)");
    } catch (err) {
      // Compensation failed. Retry it a bounded number of times, then tell the
      // orchestrator so the workflow lands in COMPENSATION_FAILED and surfaces
      // in /admin/stuck instead of disappearing.
      if (attempt <= maxRetries) {
        log.warn("Inventory release failed, will retry", { attempt, err });
        throw new TransientError(err.message);
      }

      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await emitSagaEvent(client, {
          aggregateType: "INVENTORY",
          aggregateId: orderId,
          eventType: "INVENTORY_RELEASE_FAILED",
          correlationId,
          payload: { orderId, reason: err.message },
        });
        await client.query("COMMIT");
      } finally {
        client.release();
      }
      log.error("Compensation exhausted, escalating to COMPENSATION_FAILED", { err });
    }
    return;
  }

  log.warn("Unknown inventory command ignored", { eventType });
}

async function main() {
  installSignalHandlers();
  await waitForDependencies();
  await connectQueue();

  const consumer = await startConsumer({
    queue: QUEUES.INVENTORY_COMMANDS,
    consumerName: CONSUMER,
    prefetch: Number(process.env.INVENTORY_PREFETCH || 5),
    handler: handleCommand,
  });

  await startOutboxRelay();
  const metricsServer = startMetricsServer();
  onShutdown("metrics", metricsServer.stop);
  onShutdown("outbox-relay", stopOutboxRelay);
  onShutdown("consumer", consumer.stop);
  logger.info("Inventory service ready", { failRelease: FAIL_RELEASE });
}

main().catch((err) => {
  logger.error("Fatal startup error", { err });
  process.exit(1);
});
