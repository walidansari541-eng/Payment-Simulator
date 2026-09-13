const crypto = require("crypto");
const express = require("express");
const pool = require("./db");
const {
  handleIdempotency,
  markIdempotencyCompleted,
  markIdempotencyFailed,
} = require("./idempotency");
const { createSagaWorkflow } = require("./orchestratorService");
const { getConnection } = require("./queue");
const metrics = require("./metrics");
const logger = require("./logger");

const router = express.Router();

// ---------------------------------------------------------------------------
// Checkout — starts the saga and returns immediately.
//
// The HTTP request does no downstream work: it writes the workflow and the
// first command in one transaction and hands back a correlation ID. Holding the
// connection open across a slow provider is how a payment API falls over.
// ---------------------------------------------------------------------------
router.post("/checkout", handleIdempotency, async (req, res) => {
  const { order_id: orderId, amount, currency, items } = req.body || {};

  if (!orderId || !amount || !currency) {
    await markIdempotencyFailed(req.idempotencyKey, "validation failed");
    return res.status(400).json({ error: "order_id, amount, and currency are required." });
  }
  if (!Number.isInteger(amount) || amount <= 0) {
    await markIdempotencyFailed(req.idempotencyKey, "invalid amount");
    return res.status(400).json({ error: "amount must be a positive integer (minor units)." });
  }

  // Accept a caller-supplied trace ID so a workflow can be followed from
  // outside the system; otherwise mint one.
  const correlationId =
    req.headers["x-correlation-id"] || `corr_${crypto.randomUUID()}`;

  try {
    const { workflowId } = await createSagaWorkflow({
      orderId,
      amount,
      currency,
      items: items || [],
      correlationId,
    });

    const body = {
      message: "Checkout saga initiated",
      workflowId,
      correlationId,
      status: "RESERVING_INVENTORY",
      poll: `/workflows/${correlationId}`,
    };

    await markIdempotencyCompleted(req.idempotencyKey, 202, body, correlationId);
    res.set("x-correlation-id", correlationId);
    return res.status(202).json(body);
  } catch (err) {
    logger.error("Checkout failed", { err, correlationId });
    // Release the key so the client's retry is not met with a permanent 409.
    await markIdempotencyFailed(req.idempotencyKey, err.message);
    return res.status(500).json({ error: "Internal server error", correlationId });
  }
});

// ---------------------------------------------------------------------------
// Observability endpoints — a workflow you cannot inspect is a workflow you
// cannot operate.
// ---------------------------------------------------------------------------

// Full saga timeline for one correlation ID.
router.get("/workflows/:correlationId", async (req, res) => {
  const { correlationId } = req.params;

  const wf = await pool.query(
    `SELECT * FROM workflow_executions WHERE correlation_id = $1`,
    [correlationId],
  );
  if (wf.rowCount === 0) return res.status(404).json({ error: "workflow not found" });

  const [history, payment, reservation] = await Promise.all([
    pool.query(
      `SELECT step, from_state, to_state, outcome, detail, created_at
         FROM saga_step_history WHERE correlation_id = $1 ORDER BY id ASC`,
      [correlationId],
    ),
    pool.query(`SELECT * FROM payments WHERE correlation_id = $1`, [correlationId]),
    pool.query(`SELECT * FROM reservations WHERE correlation_id = $1`, [correlationId]),
  ]);

  res.json({
    workflow: wf.rows[0],
    payment: payment.rows[0] || null,
    reservation: reservation.rows[0] || null,
    history: history.rows,
  });
});

// Payment plus its auditable status history.
router.get("/payments/:id", async (req, res) => {
  const payment = await pool.query(`SELECT * FROM payments WHERE id = $1`, [req.params.id]);
  if (payment.rowCount === 0) return res.status(404).json({ error: "payment not found" });

  const events = await pool.query(
    `SELECT from_status, to_status, reason, created_at
       FROM payment_events WHERE payment_id = $1 ORDER BY id ASC`,
    [req.params.id],
  );

  res.json({ payment: payment.rows[0], events: events.rows });
});

router.get("/payments", async (req, res) => {
  const { status, order_id: orderId, limit = 50 } = req.query;
  const rows = await pool.query(
    `SELECT * FROM payments
      WHERE ($1::text IS NULL OR status = $1)
        AND ($2::text IS NULL OR order_id = $2)
      ORDER BY created_at DESC LIMIT $3`,
    [status || null, orderId || null, Math.min(Number(limit), 200)],
  );
  res.json({ count: rows.rowCount, payments: rows.rows });
});

// First query of the incident runbook: what is stuck, and where.
router.get("/admin/stuck", async (req, res) => {
  const olderThanSeconds = Number(req.query.older_than_seconds || 30);

  const [workflows, outbox, unknownPayments] = await Promise.all([
    pool.query(
      `SELECT correlation_id, order_id, current_state, failure_reason,
              compensation_attempts, created_at, updated_at
         FROM workflow_executions
        WHERE current_state NOT IN ('COMPLETED','FAILED','COMPENSATED')
          AND updated_at < NOW() - ($1 || ' seconds')::interval
        ORDER BY updated_at ASC`,
      [olderThanSeconds],
    ),
    pool.query(
      `SELECT id, event_type, target_queue, status, retry_count, last_error, created_at
         FROM outbox_events WHERE status <> 'PROCESSED' ORDER BY created_at ASC LIMIT 100`,
    ),
    pool.query(
      `SELECT id, correlation_id, order_id, status, failure_reason, updated_at
         FROM payments WHERE status IN ('UNKNOWN','RECONCILING') ORDER BY updated_at ASC`,
    ),
  ]);

  res.json({
    stuck_workflows: workflows.rows,
    unpublished_outbox: outbox.rows,
    unresolved_payments: unknownPayments.rows,
  });
});

// ---------------------------------------------------------------------------
// Health and metrics
// ---------------------------------------------------------------------------

// Liveness: is the process alive. Must not touch dependencies, or a database
// blip would make Docker kill an otherwise healthy container.
router.get("/health", (_req, res) => res.json({ status: "ok", service: process.env.SERVICE_NAME }));

// Readiness: can this instance actually serve traffic.
router.get("/ready", async (_req, res) => {
  const checks = { postgres: false, rabbitmq: false };
  try {
    await pool.query("SELECT 1");
    checks.postgres = true;
  } catch {
    /* reported below */
  }
  try {
    checks.rabbitmq = Boolean(getConnection());
  } catch {
    /* reported below */
  }

  const ready = Object.values(checks).every(Boolean);
  res.status(ready ? 200 : 503).json({ ready, checks });
});

router.get("/metrics", async (_req, res) => {
  await metrics.collectDbGauges(pool);
  res.set("content-type", "text/plain; version=0.0.4");
  res.send(metrics.render());
});

module.exports = router;
