// Failure classification. The whole retry policy hangs off these three types:
// retry a TransientError, give up on a PermanentError, and never retry an
// UnknownOutcomeError — a retry there could charge the customer twice.

class PermanentError extends Error {
  constructor(message, details) {
    super(message);
    this.name = "PermanentError";
    this.details = details;
  }
}

class TransientError extends Error {
  constructor(message, details) {
    super(message);
    this.name = "TransientError";
    this.details = details;
  }
}

// The remote operation may or may not have happened: a timeout, a dropped
// connection mid-flight, an ambiguous 5xx after the request was accepted.
// Resolution is reconciliation (ask the provider what happened), never a retry.
class UnknownOutcomeError extends Error {
  constructor(message, details) {
    super(message);
    this.name = "UnknownOutcomeError";
    this.details = details;
  }
}

module.exports = { PermanentError, TransientError, UnknownOutcomeError };
