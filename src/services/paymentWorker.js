// ---------------------------------------------------------------------------
// Payment worker — consumes PROCESS_PAYMENT commands, talks to the provider,
// and reports the outcome back to the orchestrator.
//
// Note it does NOT use the inbox table, unlike the inventory and notification
// services. Its work spans a network call that must not sit inside a database
// transaction, and a claimed-then-crashed inbox row would make the saga stall
// forever. Instead correctness comes from two other guarantees:
//
//   * UNIQUE(payments.correlation_id) — one payment row per saga, ever.
//   * the provider's own idempotency key (we send correlationId) — even if we
//     do call /charge twice, the customer is charged once.
//
// That is the honest answer to "which idempotency mechanism belongs where".
// ---------------------------------------------------------------------------

require("dotenv").config();
process.env.SERVICE_NAME = process.env.SERVICE_NAME || "payment-worker";

const pool = require("./../db");
const { QUEUES, connectQueue } = require("../queue");
const { startConsumer } = require("../consumerRunner");
const { startOutboxRelay, stopOutboxRelay } = require("../outboxRelay");
const { emitSagaEvent } = require("../outbox");
const paymentRepo = require("../paymentRepo");
const provider = require("../providerClient");
const { PermanentError, TransientError, UnknownOutcomeError } = require("../errors");
const { PAYMENT_STATES, TERMINAL_PAYMENT_STATES } = require("../stateMachine");
const { startMetricsServer } = require("../metricsServer");
const { installSignalHandlers, onShutdown, waitForDependencies } = require("../lifecycle");
const logger = require("../logger");
const metrics = require("../metrics");

const CONSUMER = "payment-worker";

// Report the result upward. Written through the outbox so the payment state and
// the saga event commit together.
async function finalize(client, { payment, correlationId, to, reason, providerTxnId, event }) {
  await paymentRepo.transition(client, {
    paymentId: payment.id,
    correlationId,
    from: PAYMENT_STATES.PROCESSING,
    to,
    reason,
    providerTxnId,
  });

  await emitSagaEvent(client, {
    aggregateType: "PAYMENT",
    aggregateId: payment.id,
    eventType: event,
    correlationId,
    payload: { paymentId: payment.id, reason, providerTxnId },
  });
}

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

async function handleProcessPayment(msg, { log, attempt, maxRetries }) {
  const { correlationId, orderId, amount, currency } = msg;

  // --- phase 1: claim the payment, in a transaction -----------------------
  const claim = await withTx(async (client) => {
    const payment = await paymentRepo.ensurePayment(client, {
      correlationId,
      orderId,
      amount,
      currency,
    });

    // Already finished on an earlier delivery. Re-announce the outcome instead
    // of charging again — the orchestrator ignores it if it has moved on.
    if (TERMINAL_PAYMENT_STATES.includes(payment.status)) {
      const event =
        payment.status === PAYMENT_STATES.SUCCEEDED ? "PAYMENT_SUCCEEDED" : "PAYMENT_FAILED";
      await emitSagaEvent(client, {
        aggregateType: "PAYMENT",
        aggregateId: payment.id,
        eventType: event,
        correlationId,
        payload: {
          paymentId: payment.id,
          providerTxnId: payment.provider_txn_id,
          reason: "replayed terminal outcome",
        },
      });
      return { payment, skip: true };
    }

    // Already awaiting reconciliation: do not touch it, do not retry.
    if (payment.status === PAYMENT_STATES.UNKNOWN) {
      return { payment, skip: true };
    }

    if (payment.status === PAYMENT_STATES.CREATED) {
      await paymentRepo.transition(client, {
        paymentId: payment.id,
        correlationId,
        from: PAYMENT_STATES.CREATED,
        to: PAYMENT_STATES.PROCESSING,
        reason: "charging provider",
      });
    }
    const attempts = await paymentRepo.incrementAttempts(client, payment.id);
    return { payment, attempts, skip: false };
  });

  if (claim.skip) {
    log.info("Payment already resolved, nothing to charge", {
      paymentId: claim.payment.id,
      status: claim.payment.status,
    });
    return;
  }

  const payment = claim.payment;
  log.info("Charging provider", { paymentId: payment.id, attempt, attempts: claim.attempts });

  // --- phase 2: the remote call, deliberately outside any transaction -----
  try {
    const result = await provider.charge({
      correlationId,
      paymentId: payment.id,
      orderId,
      amount,
      currency,
    });

    await withTx((client) =>
      finalize(client, {
        payment,
        correlationId,
        to: PAYMENT_STATES.SUCCEEDED,
        providerTxnId: result.transactionId,
        event: "PAYMENT_SUCCEEDED",
      }),
    );
    metrics.inc("payment_succeeded_total");
    log.info("Payment succeeded", { paymentId: payment.id, txnId: result.transactionId });
    return;
  } catch (err) {
    // --- phase 3: classify -------------------------------------------------

    if (err instanceof UnknownOutcomeError) {
      // The most dangerous case in the system. We do not retry (that risks a
      // double charge) and we do not compensate (the money may be gone).
      // Park it as UNKNOWN and let the reconciler ask the provider.
      await withTx((client) =>
        finalize(client, {
          payment,
          correlationId,
          to: PAYMENT_STATES.UNKNOWN,
          reason: err.message,
          event: "PAYMENT_UNKNOWN",
        }),
      );
      metrics.inc("payment_unknown_total");
      log.error("Payment outcome UNKNOWN, handing to reconciler", {
        paymentId: payment.id,
        err,
      });
      return;
    }

    if (err instanceof PermanentError) {
      await withTx((client) =>
        finalize(client, {
          payment,
          correlationId,
          to: PAYMENT_STATES.FAILED,
          reason: err.message,
          event: "PAYMENT_FAILED",
        }),
      );
      metrics.inc("payment_failed_total", { kind: "declined" });
      log.warn("Payment permanently declined", { paymentId: payment.id, reason: err.message });
      return;
    }

    if (err instanceof TransientError) {
      if (attempt <= maxRetries) {
        // Roll back to CREATED so the next delivery re-enters cleanly.
        await withTx((client) =>
          client.query(
            `UPDATE payments SET status = 'CREATED', failure_reason = $1,
                    updated_at = CURRENT_TIMESTAMP
              WHERE id = $2 AND status = 'PROCESSING'`,
            [err.message, payment.id],
          ),
        );
        metrics.inc("payment_retry_total");
        throw err; // the runner schedules the backoff
      }

      // Bounded retries: give up deliberately with a terminal business outcome
      // rather than letting the message rot on a DLQ and stalling the saga.
      await withTx((client) =>
        finalize(client, {
          payment,
          correlationId,
          to: PAYMENT_STATES.FAILED,
          reason: `retries exhausted: ${err.message}`,
          event: "PAYMENT_FAILED",
        }),
      );
      metrics.inc("payment_failed_total", { kind: "exhausted" });
      log.error("Payment retries exhausted", { paymentId: payment.id, attempt });
      return;
    }

    throw err;
  }
}

async function main() {
  installSignalHandlers();
  await waitForDependencies();
  await connectQueue();

  const consumer = await startConsumer({
    queue: QUEUES.PAYMENT_COMMANDS,
    consumerName: CONSUMER,
    prefetch: Number(process.env.PAYMENT_PREFETCH || 5),
    handler: (msg, ctx) => {
      if (msg.eventType !== "PROCESS_PAYMENT") {
        ctx.log.warn("Ignoring unexpected command on payment queue");
        return;
      }
      return handleProcessPayment(msg, ctx);
    },
  });

  // This service emits saga events, so it needs its own relay to drain them.
  await startOutboxRelay();
  const metricsServer = startMetricsServer();

  onShutdown("metrics", metricsServer.stop);
  onShutdown("outbox-relay", stopOutboxRelay);
  onShutdown("consumer", consumer.stop);
  logger.info("Payment worker ready");
}

main().catch((err) => {
  logger.error("Fatal startup error", { err });
  process.exit(1);
});
