// ---------------------------------------------------------------------------
// Orchestrator service.
//
// Consumes the single saga_events bus, applies each outcome to the workflow
// state machine, and issues the next command. Also hosts the recovery jobs,
// because deciding that a workflow is stuck is the orchestrator's job too.
// ---------------------------------------------------------------------------

require("dotenv").config();
process.env.SERVICE_NAME = process.env.SERVICE_NAME || "orchestrator";

const { QUEUES, connectQueue } = require("../queue");
const { startConsumer } = require("../consumerRunner");
const { startOutboxRelay, stopOutboxRelay } = require("../outboxRelay");
const { withInbox } = require("../inbox");
const { applySagaEvent } = require("../orchestratorService");
const { startReconciler, stopReconciler } = require("../jobs/reconciler");
const { startMetricsServer } = require("../metricsServer");
const { installSignalHandlers, onShutdown, waitForDependencies } = require("../lifecycle");
const logger = require("../logger");
const metrics = require("../metrics");

const CONSUMER = "orchestrator";

async function handleSagaEvent(msg, { log }) {
  const { eventId, eventType, correlationId } = msg;

  if (!correlationId) {
    log.error("Saga event without a correlationId, cannot route");
    return;
  }

  // Every state change plus the command it produces plus the dedupe row commit
  // as one unit. That single transaction is what makes the saga replay-safe.
  const outcome = await withInbox(CONSUMER, eventId, (client) =>
    applySagaEvent(client, {
      eventType,
      correlationId,
      reason: msg.reason,
      // Carried into the next command so later steps see what earlier ones made.
      context: {
        paymentId: msg.paymentId,
        providerTxnId: msg.providerTxnId,
      },
    }),
  );

  if (outcome.duplicate) {
    metrics.inc("events_duplicate_total", { consumer: CONSUMER });
    return;
  }

  const result = outcome.result;
  if (result.applied) {
    log.info("Saga advanced", { nextState: result.nextState });
  } else {
    // Not an error: an out-of-order or already-applied event landing in a state
    // that no longer accepts it. Recorded in saga_step_history as IGNORED.
    log.warn("Saga event ignored", { reason: result.reason, currentState: result.currentState });
  }
}

async function main() {
  installSignalHandlers();
  await waitForDependencies();
  await connectQueue();

  const consumer = await startConsumer({
    queue: QUEUES.SAGA_EVENTS,
    consumerName: CONSUMER,
    prefetch: Number(process.env.ORCHESTRATOR_PREFETCH || 10),
    handler: handleSagaEvent,
  });

  await startOutboxRelay();
  const metricsServer = startMetricsServer();
  startReconciler();

  onShutdown("reconciler", stopReconciler);
  onShutdown("metrics", metricsServer.stop);
  onShutdown("outbox-relay", stopOutboxRelay);
  onShutdown("consumer", consumer.stop);
  logger.info("Orchestrator ready");
}

main().catch((err) => {
  logger.error("Fatal startup error", { err });
  process.exit(1);
});
