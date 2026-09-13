const crypto = require("crypto");
const pool = require("./db");
const { QUEUES } = require("./queue");
const { enqueueOutbox } = require("./outbox");
const {
  SAGA_TRANSITIONS,
  WORKFLOW_STATES,
  TERMINAL_WORKFLOW_STATES,
} = require("./stateMachine");
const logger = require("./logger");
const metrics = require("./metrics");

// ---------------------------------------------------------------------------
// Saga orchestrator.
//
// One central component owns the workflow: it holds the state, decides the next
// step, and issues commands. Services never talk to each other — they answer the
// orchestrator on the saga_events bus. That is orchestration rather than
// choreography, chosen here because a payment flow needs one auditable place
// that knows how far it got and what must be undone.
// ---------------------------------------------------------------------------

async function recordStep(client, { workflowId, correlationId, step, from, to, outcome, detail }) {
  await client.query(
    `INSERT INTO saga_step_history
       (workflow_id, correlation_id, step, from_state, to_state, outcome, detail)
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [workflowId || null, correlationId, step, from || null, to || null, outcome, detail || null],
  );
}

// Start a saga. The workflow row and the first command are written in one
// transaction, so a crash immediately after the HTTP response still leaves a
// PENDING outbox row that the relay will publish on restart.
async function createSagaWorkflow({ orderId, amount, currency, items, correlationId }) {
  const client = await pool.connect();
  const corrId = correlationId || `corr_${crypto.randomUUID()}`;

  try {
    await client.query("BEGIN");

    const wf = await client.query(
      `INSERT INTO workflow_executions (correlation_id, order_id, current_state, payload)
       VALUES ($1, $2, 'RESERVING_INVENTORY', $3)
       RETURNING workflow_id`,
      [corrId, orderId, JSON.stringify({ orderId, amount, currency, items })],
    );
    const workflowId = wf.rows[0].workflow_id;

    await enqueueOutbox(client, {
      aggregateType: "INVENTORY",
      aggregateId: workflowId,
      eventType: "RESERVE_INVENTORY",
      targetQueue: QUEUES.INVENTORY_COMMANDS,
      correlationId: corrId,
      payload: { correlationId: corrId, workflowId, orderId, amount, currency, items },
    });

    await recordStep(client, {
      workflowId,
      correlationId: corrId,
      step: "CHECKOUT_REQUESTED",
      from: WORKFLOW_STATES.STARTED,
      to: WORKFLOW_STATES.RESERVING_INVENTORY,
      outcome: "APPLIED",
    });

    await client.query("COMMIT");
    metrics.inc("workflow_started_total");
    logger.info("Saga started", { correlationId: corrId, workflowId, orderId });

    return { workflowId, correlationId: corrId };
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

// Apply one service outcome event to the workflow.
//
// Everything here — the state change, the next command, the audit row — is one
// transaction. Either the saga advanced and the command is guaranteed to be
// published, or nothing happened at all.
async function applySagaEvent(client, event) {
  const { eventType, correlationId } = event;
  const rule = SAGA_TRANSITIONS[eventType];

  if (!rule) {
    await recordStep(client, {
      correlationId,
      step: eventType,
      outcome: "IGNORED",
      detail: "no transition defined for this event type",
    });
    return { applied: false, reason: "unknown-event" };
  }

  // Guarding on current_state in the WHERE clause is what makes a replayed or
  // out-of-order event a no-op: rowCount comes back 0 and we change nothing.
  const updated = await client.query(
    `UPDATE workflow_executions
        SET current_state  = $1,
            failure_reason = COALESCE($2, failure_reason),
            updated_at     = NOW(),
            completed_at   = CASE WHEN $3 THEN NOW() ELSE completed_at END
      WHERE correlation_id = $4
        AND current_state = ANY($5::workflow_status[])
      RETURNING workflow_id, payload, created_at`,
    [
      rule.to,
      event.reason || null,
      TERMINAL_WORKFLOW_STATES.includes(rule.to),
      correlationId,
      rule.from,
    ],
  );

  if (updated.rowCount === 0) {
    const current = await client.query(
      `SELECT workflow_id, current_state FROM workflow_executions WHERE correlation_id = $1`,
      [correlationId],
    );
    const state = current.rows[0]?.current_state || "MISSING";
    await recordStep(client, {
      workflowId: current.rows[0]?.workflow_id,
      correlationId,
      step: eventType,
      from: state,
      outcome: "IGNORED",
      detail: `event not valid in state ${state} (expected one of ${rule.from.join(", ")})`,
    });
    return { applied: false, reason: "state-mismatch", currentState: state };
  }

  const { workflow_id: workflowId, payload, created_at: createdAt } = updated.rows[0];
  const wfPayload = typeof payload === "string" ? JSON.parse(payload) : payload;

  if (rule.command) {
    await enqueueOutbox(client, {
      aggregateType: rule.command.aggregate,
      aggregateId: workflowId,
      eventType: rule.command.type,
      targetQueue: rule.command.queue,
      correlationId,
      payload: {
        correlationId,
        workflowId,
        ...wfPayload,
        // Carry forward whatever the previous step produced (e.g. paymentId).
        ...(event.context || {}),
      },
    });
  }

  await recordStep(client, {
    workflowId,
    correlationId,
    step: eventType,
    from: rule.from.join("|"),
    to: rule.to,
    outcome: "APPLIED",
    detail: event.reason || null,
  });

  recordTerminalMetrics(rule.to, createdAt);

  return { applied: true, workflowId, nextState: rule.to };
}

function recordTerminalMetrics(state, createdAt) {
  const counters = {
    COMPLETED: "workflow_completed_total",
    FAILED: "workflow_failed_total",
    COMPENSATED: "workflow_compensated_total",
    COMPENSATION_FAILED: "workflow_compensation_failed_total",
  };
  if (!counters[state]) return;

  metrics.inc(counters[state]);
  if (createdAt) {
    metrics.observe("workflow_duration_seconds", (Date.now() - new Date(createdAt)) / 1000);
  }
}

async function forceState(client, { correlationId, from, to, reason, step }) {
  const res = await client.query(
    `UPDATE workflow_executions
        SET current_state = $1, failure_reason = $2, updated_at = NOW(),
            completed_at = CASE WHEN $3 THEN NOW() ELSE completed_at END
      WHERE correlation_id = $4 AND current_state = ANY($5::workflow_status[])
      RETURNING workflow_id, created_at`,
    [to, reason, TERMINAL_WORKFLOW_STATES.includes(to), correlationId, from],
  );

  if (res.rowCount === 0) return { applied: false };

  await recordStep(client, {
    workflowId: res.rows[0].workflow_id,
    correlationId,
    step,
    from: from.join("|"),
    to,
    outcome: "APPLIED",
    detail: reason,
  });
  recordTerminalMetrics(to, res.rows[0].created_at);
  return { applied: true, workflowId: res.rows[0].workflow_id };
}

module.exports = { createSagaWorkflow, applySagaEvent, forceState, recordStep };
