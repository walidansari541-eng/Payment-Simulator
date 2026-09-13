const { PAYMENT_STATES, isValidTransition } = require("./stateMachine");

// All payment writes go through here so that no code path can move a payment
// between states without (a) the transition being legal and (b) an audit row
// being written. The payment_events table is the auditable lifecycle Week 7
// asks for.

// Create the payment for a saga, or return the one that already exists.
//
// UNIQUE(correlation_id) is the idempotency anchor: a redelivered
// PROCESS_PAYMENT command cannot create a second payment for the same saga.
async function ensurePayment(client, { correlationId, orderId, amount, currency }) {
  const inserted = await client.query(
    `INSERT INTO payments (correlation_id, order_id, amount, currency, status)
     VALUES ($1, $2, $3, $4, 'CREATED')
     ON CONFLICT (correlation_id) DO NOTHING
     RETURNING id, status, attempts, provider_txn_id`,
    [correlationId, orderId, amount, currency],
  );

  if (inserted.rowCount === 1) {
    const payment = inserted.rows[0];
    await client.query(
      `INSERT INTO payment_events (payment_id, correlation_id, from_status, to_status, reason)
       VALUES ($1, $2, NULL, 'CREATED', 'payment created')`,
      [payment.id, correlationId],
    );
    return { ...payment, created: true };
  }

  // FOR UPDATE: two deliveries racing must not both decide to charge.
  const existing = await client.query(
    `SELECT id, status, attempts, provider_txn_id
       FROM payments WHERE correlation_id = $1 FOR UPDATE`,
    [correlationId],
  );
  return { ...existing.rows[0], created: false };
}

// Move a payment to a new status, refusing illegal transitions outright.
async function transition(client, { paymentId, correlationId, from, to, reason, providerTxnId }) {
  if (!isValidTransition(from, to)) {
    throw new Error(`Illegal payment transition ${from} -> ${to} (payment ${paymentId})`);
  }

  // The reason is always audited in payment_events; it is only promoted onto
  // the payment row itself when it explains a bad outcome.
  const isFailureState = ["FAILED", "UNKNOWN", "CANCELLED"].includes(to);
  const failureReason = isFailureState ? reason || null : null;

  const res = await client.query(
    `UPDATE payments
        SET status          = $1,
            failure_reason  = CASE WHEN $6 THEN $2 ELSE NULL END,
            provider_txn_id = COALESCE($3, provider_txn_id),
            updated_at      = CURRENT_TIMESTAMP
      WHERE id = $4 AND status = $5
      RETURNING status`,
    [to, failureReason, providerTxnId || null, paymentId, from, isFailureState],
  );

  // Lost the race to another worker; it already advanced this payment.
  if (res.rowCount === 0) return { applied: false };

  await client.query(
    `INSERT INTO payment_events (payment_id, correlation_id, from_status, to_status, reason)
     VALUES ($1, $2, $3, $4, $5)`,
    [paymentId, correlationId, from, to, reason || null],
  );
  return { applied: true };
}

async function incrementAttempts(client, paymentId) {
  const res = await client.query(
    `UPDATE payments SET attempts = attempts + 1 WHERE id = $1 RETURNING attempts`,
    [paymentId],
  );
  return res.rows[0].attempts;
}

async function getByCorrelationId(client, correlationId) {
  const res = await client.query(`SELECT * FROM payments WHERE correlation_id = $1`, [
    correlationId,
  ]);
  return res.rows[0] || null;
}

module.exports = {
  PAYMENT_STATES,
  ensurePayment,
  transition,
  incrementAttempts,
  getByCorrelationId,
};
