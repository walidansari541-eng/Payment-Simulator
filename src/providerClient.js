const { PermanentError, TransientError, UnknownOutcomeError } = require("./errors");
const logger = require("./logger");

const BASE_URL = process.env.PROVIDER_URL || "http://mock-provider:4000";
const TIMEOUT_MS = Number(process.env.PROVIDER_TIMEOUT_MS || 3000);

// Connection-level errors that prove the request never reached the provider, so
// no charge can have happened. Anything else that goes wrong mid-flight is
// genuinely ambiguous.
const NEVER_ARRIVED = new Set(["ECONNREFUSED", "ENOTFOUND", "EAI_AGAIN", "ECONNRESET"]);

// Charge the card.
//
// The classification of the outcome is the important part:
//   2xx           -> approved
//   4xx           -> PermanentError      (a business decision; retrying repeats it)
//   5xx           -> TransientError      (provider fault before any work; safe to retry)
//   timeout       -> UnknownOutcomeError (may or may not have charged — reconcile)
//   conn refused  -> TransientError      (request never arrived)
//
// correlationId doubles as the provider-side idempotency key, so even if we do
// retry, the provider collapses it to one charge.
async function charge({ correlationId, paymentId, orderId, amount, currency }) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);

  let response;
  try {
    response = await fetch(`${BASE_URL}/charge`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "idempotency-key": correlationId,
        "x-correlation-id": correlationId,
      },
      body: JSON.stringify({ payment_id: paymentId, order_id: orderId, amount, currency }),
      signal: controller.signal,
    });
  } catch (err) {
    const code = err.cause?.code;

    if (err.name === "AbortError") {
      throw new UnknownOutcomeError(
        `Provider did not respond within ${TIMEOUT_MS}ms`,
        { correlationId },
      );
    }
    if (NEVER_ARRIVED.has(code)) {
      throw new TransientError(`Provider unreachable (${code})`, { correlationId });
    }
    throw new UnknownOutcomeError(`Provider call failed mid-flight: ${err.message}`, {
      correlationId,
    });
  } finally {
    clearTimeout(timer);
  }

  let body = {};
  try {
    body = await response.json();
  } catch {
    // A 2xx we cannot read is not a confirmed success.
    if (response.ok) {
      throw new UnknownOutcomeError("Provider returned an unreadable success body");
    }
  }

  if (response.ok && body.approved) {
    return { approved: true, transactionId: body.transaction_id, code: body.code };
  }
  if (response.status >= 500) {
    throw new TransientError(`Provider ${response.status}: ${body.error || "server error"}`);
  }
  if (response.status >= 400) {
    throw new PermanentError(
      `Provider declined (${response.status}): ${body.reason || body.error || "declined"}`,
      { code: body.code },
    );
  }

  throw new UnknownOutcomeError(`Unexpected provider response ${response.status}`);
}

// Ask the provider what actually happened for a given idempotency key. Used
// only by the reconciler, and never followed by a re-charge.
async function lookup(correlationId) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);

  try {
    const url = `${BASE_URL}/transactions?idempotency_key=${encodeURIComponent(correlationId)}`;
    const response = await fetch(url, { signal: controller.signal });

    if (response.status === 404) return { found: false };
    if (!response.ok) throw new TransientError(`Provider lookup returned ${response.status}`);

    return await response.json();
  } catch (err) {
    if (err instanceof TransientError) throw err;
    logger.warn("Provider lookup failed", { correlationId, err: err.message });
    throw new TransientError(`Provider lookup failed: ${err.message}`);
  } finally {
    clearTimeout(timer);
  }
}

module.exports = { charge, lookup, TIMEOUT_MS };
