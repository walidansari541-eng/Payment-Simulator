const crypto = require("crypto");
const pool = require("./db");
const logger = require("./logger");

// HTTP-layer idempotency (Week 7).
//
// A client that times out cannot tell whether we processed its request, so it
// retries. The key turns "did this happen?" into a question we can answer:
//
//   first time      -> claim the key, run the handler, store the response
//   in flight       -> 409, the original request is still running
//   completed       -> replay the stored response byte for byte
//   different body  -> 422, the key is being reused for a different request
//
// The last case matters: without a body fingerprint the client gets back a
// confidently wrong answer for an operation we never performed.

const hashBody = (body) =>
  crypto.createHash("sha256").update(JSON.stringify(body ?? {})).digest("hex");

async function handleIdempotency(req, res, next) {
  const key = req.headers["idempotency-key"];

  if (!key) {
    return res
      .status(400)
      .json({ error: "Idempotency-Key header is required for payment requests." });
  }

  const requestHash = hashBody(req.body);

  try {
    // Atomic claim. Concurrent duplicates race on the primary key, so exactly
    // one wins and the rest fall into the branch below.
    const claim = await pool.query(
      `INSERT INTO idempotency_keys (key, request_hash, status)
       VALUES ($1, $2, 'PROCESSING')
       ON CONFLICT (key) DO NOTHING
       RETURNING key`,
      [key, requestHash],
    );

    if (claim.rowCount === 0) {
      const existing = await pool.query(`SELECT * FROM idempotency_keys WHERE key = $1`, [key]);
      const record = existing.rows[0];

      if (record.request_hash !== requestHash) {
        return res.status(422).json({
          error: "Idempotency-Key was already used with a different request body.",
        });
      }

      if (record.status === "PROCESSING") {
        return res.status(409).json({
          error: "A request with this idempotency key is currently processing.",
          retry_after_ms: 500,
        });
      }

      if (record.status === "COMPLETED") {
        logger.info("Replaying stored idempotent response", { key });
        return res.status(record.response_code).json(record.response_body);
      }

      // FAILED: the previous attempt errored out and released the key. Let this
      // request take it over rather than wedging the client forever.
      await pool.query(
        `UPDATE idempotency_keys
            SET status = 'PROCESSING', updated_at = CURRENT_TIMESTAMP
          WHERE key = $1`,
        [key],
      );
    }

    req.idempotencyKey = key;
    next();
  } catch (err) {
    logger.error("Idempotency middleware error", { err, key });
    return res.status(500).json({ error: "Internal server error processing idempotency" });
  }
}

async function markIdempotencyCompleted(key, responseCode, responseBody, correlationId) {
  await pool.query(
    `UPDATE idempotency_keys
        SET status = 'COMPLETED', response_code = $1, response_body = $2,
            correlation_id = $3, updated_at = CURRENT_TIMESTAMP
      WHERE key = $4`,
    [responseCode, JSON.stringify(responseBody), correlationId || null, key],
  );
}

// Release the key so the client's retry is not answered with a permanent 409.
async function markIdempotencyFailed(key, reason) {
  await pool.query(
    `UPDATE idempotency_keys
        SET status = 'FAILED', response_body = $1, updated_at = CURRENT_TIMESTAMP
      WHERE key = $2`,
    [JSON.stringify({ error: reason }), key],
  );
}

module.exports = {
  handleIdempotency,
  markIdempotencyCompleted,
  markIdempotencyFailed,
  hashBody,
};
