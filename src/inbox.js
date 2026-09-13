const pool = require("./db");
const logger = require("./logger");

// Inbox / idempotent-consumer pattern.
//
// RabbitMQ gives at-least-once delivery, so every consumer must tolerate seeing
// the same event twice. We insert the event id into processed_events inside the
// SAME transaction as the business work: either both land or neither does. A
// second delivery hits the primary-key conflict, does no work, and is acked.
//
//   withInbox('payment-worker', eventId, async (client) => { ...business... })
//     -> { duplicate: true }                  event already handled
//     -> { duplicate: false, result: <ret> }  work committed
//
// The callback receives the transaction's client and MUST use it for all writes,
// otherwise the dedupe guarantee is lost.
async function withInbox(consumerName, eventId, fn) {
  if (!eventId) {
    throw new Error(`withInbox(${consumerName}): eventId is required for dedupe`);
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const claim = await client.query(
      `INSERT INTO processed_events (event_id, consumer_name)
       VALUES ($1, $2)
       ON CONFLICT (event_id, consumer_name) DO NOTHING
       RETURNING event_id`,
      [eventId, consumerName],
    );

    if (claim.rowCount === 0) {
      await client.query("COMMIT");
      logger.info("Duplicate event ignored", { consumerName, eventId });
      return { duplicate: true };
    }

    const result = await fn(client);
    await client.query("COMMIT");
    return { duplicate: false, result };
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

// Was this event already handled by this consumer? Used by handlers that must
// do non-transactional work (an HTTP call) between claiming and finishing.
async function alreadyProcessed(consumerName, eventId) {
  const res = await pool.query(
    `SELECT 1 FROM processed_events WHERE event_id = $1 AND consumer_name = $2`,
    [eventId, consumerName],
  );
  return res.rowCount > 0;
}

module.exports = { withInbox, alreadyProcessed };
