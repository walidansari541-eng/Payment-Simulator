const { QUEUES } = require("./queue");

// Write an outgoing message into the outbox using the caller's transaction.
//
// The whole point: the business state change and the message announcing it
// commit together. If the process dies before RabbitMQ ever hears about it, the
// row is still PENDING and the relay picks it up on restart.
async function enqueueOutbox(client, {
  aggregateType,
  aggregateId,
  eventType,
  targetQueue,
  correlationId,
  payload,
}) {
  const res = await client.query(
    `INSERT INTO outbox_events
       (aggregate_type, aggregate_id, event_type, target_queue, correlation_id, payload)
     VALUES ($1, $2, $3, $4, $5, $6)
     RETURNING id`,
    [
      aggregateType,
      String(aggregateId),
      eventType,
      targetQueue,
      correlationId,
      JSON.stringify(payload),
    ],
  );
  return res.rows[0].id;
}

// Services report their outcome back to the orchestrator on the single saga bus.
function emitSagaEvent(client, { aggregateType, aggregateId, eventType, correlationId, payload }) {
  return enqueueOutbox(client, {
    aggregateType,
    aggregateId,
    eventType,
    targetQueue: QUEUES.SAGA_EVENTS,
    correlationId,
    payload: { correlationId, ...payload },
  });
}

module.exports = { enqueueOutbox, emitSagaEvent };
