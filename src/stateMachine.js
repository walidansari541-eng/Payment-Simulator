const { QUEUES } = require("./queue");

// ---------------------------------------------------------------------------
// Payment state machine (Week 7)
//
// Status is an enum, not a boolean, because "did it work?" has more than two
// answers: it may not have finished, or we may not know.
// ---------------------------------------------------------------------------
const PAYMENT_STATES = {
  CREATED: "CREATED",
  PROCESSING: "PROCESSING",
  SUCCEEDED: "SUCCEEDED",
  FAILED: "FAILED",
  CANCELLED: "CANCELLED",
  UNKNOWN: "UNKNOWN",
  RECONCILING: "RECONCILING",
};

const PAYMENT_TRANSITIONS = {
  CREATED: [PAYMENT_STATES.PROCESSING, PAYMENT_STATES.CANCELLED],
  // A charge in flight can land anywhere, including "we have no idea".
  PROCESSING: [
    PAYMENT_STATES.SUCCEEDED,
    PAYMENT_STATES.FAILED,
    PAYMENT_STATES.CANCELLED,
    PAYMENT_STATES.UNKNOWN,
  ],
  // UNKNOWN is not terminal: the reconciler picks it up and resolves it.
  UNKNOWN: [PAYMENT_STATES.RECONCILING],
  RECONCILING: [PAYMENT_STATES.SUCCEEDED, PAYMENT_STATES.FAILED, PAYMENT_STATES.UNKNOWN],
  SUCCEEDED: [],
  FAILED: [],
  CANCELLED: [],
};

const TERMINAL_PAYMENT_STATES = ["SUCCEEDED", "FAILED", "CANCELLED"];

function isValidTransition(currentStatus, nextStatus) {
  const allowed = PAYMENT_TRANSITIONS[currentStatus];
  return allowed ? allowed.includes(nextStatus) : false;
}

const IDEMPOTENCY_STATES = {
  PROCESSING: "PROCESSING",
  COMPLETED: "COMPLETED",
  FAILED: "FAILED",
};

// ---------------------------------------------------------------------------
// Saga state machine (Week 8)
//
// One table drives the orchestrator. Every entry says: on this event, if the
// workflow is in one of `from`, move it to `to` and (optionally) emit one
// command. An event arriving in any other state is ignored rather than applied
// — that is how duplicate and out-of-order deliveries are absorbed.
// ---------------------------------------------------------------------------
const WORKFLOW_STATES = {
  STARTED: "STARTED",
  RESERVING_INVENTORY: "RESERVING_INVENTORY",
  PROCESSING_PAYMENT: "PROCESSING_PAYMENT",
  SENDING_NOTIFICATION: "SENDING_NOTIFICATION",
  COMPENSATING_INVENTORY: "COMPENSATING_INVENTORY",
  AWAITING_RECONCILIATION: "AWAITING_RECONCILIATION",
  COMPENSATED: "COMPENSATED",
  COMPENSATION_FAILED: "COMPENSATION_FAILED",
  COMPLETED: "COMPLETED",
  FAILED: "FAILED",
};

const TERMINAL_WORKFLOW_STATES = [
  WORKFLOW_STATES.COMPLETED,
  WORKFLOW_STATES.FAILED,
  WORKFLOW_STATES.COMPENSATED,
  WORKFLOW_STATES.COMPENSATION_FAILED,
];

const SAGA_TRANSITIONS = {
  // --- forward path -------------------------------------------------------
  INVENTORY_RESERVED: {
    from: [WORKFLOW_STATES.RESERVING_INVENTORY],
    to: WORKFLOW_STATES.PROCESSING_PAYMENT,
    command: { type: "PROCESS_PAYMENT", queue: QUEUES.PAYMENT_COMMANDS, aggregate: "PAYMENT" },
  },
  PAYMENT_SUCCEEDED: {
    // Also accepted from AWAITING_RECONCILIATION: the reconciler discovered the
    // provider had in fact charged the card.
    from: [WORKFLOW_STATES.PROCESSING_PAYMENT, WORKFLOW_STATES.AWAITING_RECONCILIATION],
    to: WORKFLOW_STATES.SENDING_NOTIFICATION,
    command: {
      type: "SEND_NOTIFICATION",
      queue: QUEUES.NOTIFICATION_COMMANDS,
      aggregate: "NOTIFICATION",
    },
  },
  NOTIFICATION_SENT: {
    from: [WORKFLOW_STATES.SENDING_NOTIFICATION],
    to: WORKFLOW_STATES.COMPLETED,
    terminal: true,
  },

  // --- failure and compensation ------------------------------------------
  INVENTORY_FAILED: {
    // Nothing has been reserved and nothing charged, so there is nothing to
    // undo. Straight to a terminal failure.
    from: [WORKFLOW_STATES.RESERVING_INVENTORY],
    to: WORKFLOW_STATES.FAILED,
    terminal: true,
  },
  PAYMENT_FAILED: {
    from: [WORKFLOW_STATES.PROCESSING_PAYMENT, WORKFLOW_STATES.AWAITING_RECONCILIATION],
    to: WORKFLOW_STATES.COMPENSATING_INVENTORY,
    command: {
      type: "RELEASE_INVENTORY",
      queue: QUEUES.INVENTORY_COMMANDS,
      aggregate: "INVENTORY",
    },
  },
  PAYMENT_UNKNOWN: {
    // Deliberately emits no command. We do not compensate (the customer may
    // have been charged) and we do not retry (we would charge them twice).
    // The reconciler owns this state.
    from: [WORKFLOW_STATES.PROCESSING_PAYMENT],
    to: WORKFLOW_STATES.AWAITING_RECONCILIATION,
  },
  INVENTORY_RELEASED: {
    from: [WORKFLOW_STATES.COMPENSATING_INVENTORY],
    to: WORKFLOW_STATES.COMPENSATED,
    terminal: true,
  },
  INVENTORY_RELEASE_FAILED: {
    // Compensation itself failed. Terminal only in the automated sense: it is
    // parked for a human via GET /admin/stuck.
    from: [WORKFLOW_STATES.COMPENSATING_INVENTORY],
    to: WORKFLOW_STATES.COMPENSATION_FAILED,
    terminal: true,
  },
  NOTIFICATION_FAILED: {
    // The money moved and stock is reserved; a failed receipt does not justify
    // unwinding the order. Complete the saga and let the notification retry
    // out of band.
    from: [WORKFLOW_STATES.SENDING_NOTIFICATION],
    to: WORKFLOW_STATES.COMPLETED,
    terminal: true,
  },
};

module.exports = {
  PAYMENT_STATES,
  PAYMENT_TRANSITIONS,
  TERMINAL_PAYMENT_STATES,
  IDEMPOTENCY_STATES,
  isValidTransition,
  WORKFLOW_STATES,
  TERMINAL_WORKFLOW_STATES,
  SAGA_TRANSITIONS,
};
