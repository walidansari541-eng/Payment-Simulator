// ---------------------------------------------------------------------------
// Recovery jobs. Runs inside the orchestrator.
//
// Three separate concerns, all answering "what if nobody ever tells us how this
// ended?":
//
//   1. reconcileUnknownPayments — resolve payments whose provider outcome is
//      unknown, by ASKING the provider. Never by charging again.
//   2. sweepStuckWorkflows      — workflows that stopped making progress.
//   3. retryFailedCompensation  — compensation that failed, retried with a
//      bound, then parked for a human.
//
// Without these, "unknown" is a permanent leak: a saga sits half-done forever
// and nothing in the system is responsible for noticing.
// ---------------------------------------------------------------------------

const pool = require("../db");
const provider = require("../providerClient");
const paymentRepo = require("../paymentRepo");
const { emitSagaEvent, enqueueOutbox } = require("../outbox");
const { forceState, recordStep } = require("../orchestratorService");
const { QUEUES } = require("../queue");
const { PAYMENT_STATES, WORKFLOW_STATES } = require("../stateMachine");
const logger = require("../logger");
const metrics = require("../metrics");

const INTERVAL_MS = Number(process.env.RECONCILE_INTERVAL_MS || 5000);
const RECONCILE_AFTER_MS = Number(process.env.RECONCILE_AFTER_MS || 5000);
const WORKFLOW_TIMEOUT_MS = Number(process.env.WORKFLOW_TIMEOUT_MS || 60000);
const MAX_COMPENSATION_ATTEMPTS = Number(process.env.MAX_COMPENSATION_ATTEMPTS || 3);

let timer;
let stopping = false;
let running = false;

async function withTx(fn) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const out = await fn(client);
    await client.query("COMMIT");
    return out;
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

// --- 1. unknown provider outcomes ------------------------------------------
async function reconcileUnknownPayments() {
  const candidates = await pool.query(
    `SELECT id, correlation_id, order_id, amount
       FROM payments
      WHERE status = 'UNKNOWN'
        AND updated_at < NOW() - ($1 || ' milliseconds')::interval
      ORDER BY updated_at ASC
      LIMIT 20`,
    [RECONCILE_AFTER_MS],
  );

  for (const payment of candidates.rows) {
    const log = logger.child({
      correlationId: payment.correlation_id,
      paymentId: payment.id,
      job: "reconcile",
    });

    // Claim it so two orchestrator replicas do not both reconcile the same row.
    const claimed = await withTx((client) =>
      paymentRepo.transition(client, {
        paymentId: payment.id,
        correlationId: payment.correlation_id,
        from: PAYMENT_STATES.UNKNOWN,
        to: PAYMENT_STATES.RECONCILING,
        reason: "reconciler querying provider",
      }),
    );
    if (!claimed.applied) continue;

    let truth;
    try {
      truth = await provider.lookup(payment.correlation_id);
    } catch (err) {
      // Provider still unreachable. Put it back and try again next tick.
      log.warn("Reconciliation lookup failed, will retry", { err: err.message });
      await withTx((client) =>
        paymentRepo.transition(client, {
          paymentId: payment.id,
          correlationId: payment.correlation_id,
          from: PAYMENT_STATES.RECONCILING,
          to: PAYMENT_STATES.UNKNOWN,
          reason: `lookup failed: ${err.message}`,
        }),
      );
      continue;
    }

    // found + APPROVED -> the customer WAS charged despite our timeout.
    // not found        -> the provider has no record, so no money moved.
    const approved = truth.found && truth.outcome === "APPROVED";
    const to = approved ? PAYMENT_STATES.SUCCEEDED : PAYMENT_STATES.FAILED;
    const event = approved ? "PAYMENT_SUCCEEDED" : "PAYMENT_FAILED";

    await withTx(async (client) => {
      await paymentRepo.transition(client, {
        paymentId: payment.id,
        correlationId: payment.correlation_id,
        from: PAYMENT_STATES.RECONCILING,
        to,
        reason: approved
          ? "reconciled: provider had charged after all"
          : `reconciled: provider has no record (${truth.found ? truth.outcome : "not found"})`,
        providerTxnId: approved ? truth.txn_id : null,
      });

      await emitSagaEvent(client, {
        aggregateType: "PAYMENT",
        aggregateId: payment.id,
        eventType: event,
        correlationId: payment.correlation_id,
        payload: {
          paymentId: payment.id,
          providerTxnId: approved ? truth.txn_id : null,
          reason: "resolved by reconciliation",
        },
      });
    });

    metrics.inc("payment_reconciled_total", { outcome: approved ? "approved" : "not_charged" });
    log.warn("UNKNOWN payment reconciled", { outcome: to, txnId: truth.txn_id });
  }
}

// --- 2. workflows that stopped moving --------------------------------------
async function sweepStuckWorkflows() {
  const stuck = await pool.query(
    `SELECT correlation_id, current_state, order_id
       FROM workflow_executions
      WHERE current_state NOT IN
            ('COMPLETED','FAILED','COMPENSATED','COMPENSATION_FAILED','AWAITING_RECONCILIATION')
        AND updated_at < NOW() - ($1 || ' milliseconds')::interval
      LIMIT 20`,
    [WORKFLOW_TIMEOUT_MS],
  );

  for (const wf of stuck.rows) {
    const log = logger.child({
      correlationId: wf.correlation_id,
      job: "sweep",
      state: wf.current_state,
    });

    // Where a stalled workflow goes depends on how much of it really happened.
    // This is a business decision, not a generic timeout rule.
    if (wf.current_state === WORKFLOW_STATES.PROCESSING_PAYMENT) {
      // A payment we never heard back about is exactly an unknown outcome.
      await withTx((client) =>
        forceState(client, {
          correlationId: wf.correlation_id,
          from: [WORKFLOW_STATES.PROCESSING_PAYMENT],
          to: WORKFLOW_STATES.AWAITING_RECONCILIATION,
          reason: "payment step timed out with no outcome",
          step: "WORKFLOW_TIMEOUT",
        }),
      );
      await markPaymentUnknown(wf.correlation_id);
      log.error("Payment step timed out, handed to reconciliation");
      continue;
    }

    if (wf.current_state === WORKFLOW_STATES.RESERVING_INVENTORY) {
      // Nothing charged and (as far as we know) nothing reserved.
      await withTx((client) =>
        forceState(client, {
          correlationId: wf.correlation_id,
          from: [WORKFLOW_STATES.RESERVING_INVENTORY],
          to: WORKFLOW_STATES.FAILED,
          reason: "inventory step timed out",
          step: "WORKFLOW_TIMEOUT",
        }),
      );
      log.error("Inventory step timed out, workflow failed");
      continue;
    }

    if (wf.current_state === WORKFLOW_STATES.SENDING_NOTIFICATION) {
      // Money moved and stock is reserved. A missing receipt does not justify
      // unwinding the order.
      await withTx((client) =>
        forceState(client, {
          correlationId: wf.correlation_id,
          from: [WORKFLOW_STATES.SENDING_NOTIFICATION],
          to: WORKFLOW_STATES.COMPLETED,
          reason: "notification step timed out; order completed without receipt",
          step: "WORKFLOW_TIMEOUT",
        }),
      );
      log.warn("Notification timed out, completing workflow anyway");
    }
  }
}

async function markPaymentUnknown(correlationId) {
  await withTx(async (client) => {
    const payment = await paymentRepo.getByCorrelationId(client, correlationId);
    if (!payment || payment.status !== PAYMENT_STATES.PROCESSING) return;
    await paymentRepo.transition(client, {
      paymentId: payment.id,
      correlationId,
      from: PAYMENT_STATES.PROCESSING,
      to: PAYMENT_STATES.UNKNOWN,
      reason: "workflow timeout with no provider outcome",
    });
    metrics.inc("payment_unknown_total");
  });
}

// --- 3. compensation that failed -------------------------------------------
async function retryFailedCompensation() {
  const stuck = await pool.query(
    `SELECT workflow_id, correlation_id, order_id, payload, compensation_attempts
       FROM workflow_executions
      WHERE current_state = 'COMPENSATING_INVENTORY'
        AND updated_at < NOW() - ($1 || ' milliseconds')::interval
      LIMIT 20`,
    [WORKFLOW_TIMEOUT_MS],
  );

  for (const wf of stuck.rows) {
    const log = logger.child({ correlationId: wf.correlation_id, job: "compensation-retry" });
    const payload = typeof wf.payload === "string" ? JSON.parse(wf.payload) : wf.payload;

    if (wf.compensation_attempts >= MAX_COMPENSATION_ATTEMPTS) {
      await withTx((client) =>
        forceState(client, {
          correlationId: wf.correlation_id,
          from: [WORKFLOW_STATES.COMPENSATING_INVENTORY],
          to: WORKFLOW_STATES.COMPENSATION_FAILED,
          reason: `compensation failed after ${wf.compensation_attempts} attempts`,
          step: "COMPENSATION_GIVE_UP",
        }),
      );
      log.error("Compensation abandoned; workflow parked for manual recovery");
      continue;
    }

    await withTx(async (client) => {
      await client.query(
        `UPDATE workflow_executions
            SET compensation_attempts = compensation_attempts + 1, updated_at = NOW()
          WHERE workflow_id = $1`,
        [wf.workflow_id],
      );
      await enqueueOutbox(client, {
        aggregateType: "INVENTORY",
        aggregateId: wf.workflow_id,
        eventType: "RELEASE_INVENTORY",
        targetQueue: QUEUES.INVENTORY_COMMANDS,
        correlationId: wf.correlation_id,
        payload: { correlationId: wf.correlation_id, orderId: wf.order_id, ...payload },
      });
      await recordStep(client, {
        workflowId: wf.workflow_id,
        correlationId: wf.correlation_id,
        step: "COMPENSATION_RETRY",
        from: WORKFLOW_STATES.COMPENSATING_INVENTORY,
        to: WORKFLOW_STATES.COMPENSATING_INVENTORY,
        outcome: "APPLIED",
        detail: `retry ${wf.compensation_attempts + 1}/${MAX_COMPENSATION_ATTEMPTS}`,
      });
    });

    log.warn("Re-issued RELEASE_INVENTORY", { attempt: wf.compensation_attempts + 1 });
  }
}

async function tick() {
  if (stopping || running) return;
  running = true;
  try {
    await reconcileUnknownPayments();
    await sweepStuckWorkflows();
    await retryFailedCompensation();
    await metrics.collectDbGauges(pool);
  } catch (err) {
    logger.error("Reconciler tick failed", { err });
  } finally {
    running = false;
  }
}

function startReconciler() {
  stopping = false;
  logger.info("Reconciler started", {
    intervalMs: INTERVAL_MS,
    reconcileAfterMs: RECONCILE_AFTER_MS,
    workflowTimeoutMs: WORKFLOW_TIMEOUT_MS,
  });
  const loop = async () => {
    await tick();
    if (!stopping) timer = setTimeout(loop, INTERVAL_MS);
  };
  timer = setTimeout(loop, INTERVAL_MS);
}

async function stopReconciler() {
  stopping = true;
  if (timer) clearTimeout(timer);
}

module.exports = { startReconciler, stopReconciler, tick };
