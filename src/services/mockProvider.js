// ---------------------------------------------------------------------------
// Mock external payment provider.
//
// A separate HTTP service on purpose: only a real network hop gives real
// timeouts, real dropped responses, and a real "we don't know what happened"
// case. An in-process function can only pretend.
//
// Behaviour is controlled two ways:
//   * per-order triggers  — order_id containing FAIL / TIMEOUT / SLOW / LOST / FLAKY
//   * runtime override    — POST /admin/behavior, so failure injection needs no
//                           redeploy or restart
// ---------------------------------------------------------------------------

require("dotenv").config();
process.env.SERVICE_NAME = process.env.SERVICE_NAME || "mock-provider";

const crypto = require("crypto");
const express = require("express");
const pool = require("../db");
const logger = require("../logger");
const { installSignalHandlers, onShutdown, waitForDependencies } = require("../lifecycle");

const PORT = Number(process.env.PROVIDER_PORT || 4000);

const MODES = ["ok", "fail", "slow", "timeout", "lost", "flaky"];

// Runtime-injectable behaviour. `mode: "auto"` means: obey the order_id triggers.
let behavior = {
  mode: "auto",
  latencyMs: Number(process.env.PROVIDER_BASE_LATENCY_MS || 150),
  failureRate: 0,
};

function modeForOrder(orderId = "") {
  if (behavior.mode !== "auto") return behavior.mode;
  const id = String(orderId).toUpperCase();
  if (id.includes("TIMEOUT")) return "timeout";
  if (id.includes("LOST")) return "lost";
  if (id.includes("FLAKY")) return "flaky";
  if (id.includes("SLOW")) return "slow";
  if (id.includes("FAIL") || id.includes("DECLINE")) return "fail";
  return "ok";
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function recordTransaction({ idempotencyKey, amount, outcome, responseSent }) {
  const txnId = `txn_${crypto.randomBytes(6).toString("hex")}`;
  const res = await pool.query(
    `INSERT INTO provider_transactions (idempotency_key, txn_id, amount, outcome, response_sent)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (idempotency_key) DO NOTHING
     RETURNING txn_id, outcome, response_sent`,
    [idempotencyKey, txnId, amount, outcome, responseSent],
  );

  // Conflict means we already charged this key — return the original result.
  if (res.rowCount === 0) {
    const existing = await pool.query(
      `SELECT txn_id, outcome, response_sent FROM provider_transactions WHERE idempotency_key = $1`,
      [idempotencyKey],
    );
    return { ...existing.rows[0], replayed: true };
  }
  return { ...res.rows[0], replayed: false };
}

const app = express();
app.use(express.json());

app.get("/health", (_req, res) => res.json({ status: "ok", behavior }));

app.get("/admin/behavior", (_req, res) => res.json(behavior));

app.post("/admin/behavior", (req, res) => {
  const { mode, latencyMs, failureRate } = req.body || {};
  if (mode && mode !== "auto" && !MODES.includes(mode)) {
    return res.status(400).json({ error: `mode must be "auto" or one of ${MODES.join(", ")}` });
  }
  behavior = {
    mode: mode ?? behavior.mode,
    latencyMs: latencyMs ?? behavior.latencyMs,
    failureRate: failureRate ?? behavior.failureRate,
  };
  logger.warn("Provider behavior changed", behavior);
  res.json(behavior);
});

// Reconciliation endpoint. This is what makes an UNKNOWN outcome resolvable
// without ever re-charging: ask the provider what it did with that key.
app.get("/transactions", async (req, res) => {
  const key = req.query.idempotency_key;
  if (!key) return res.status(400).json({ error: "idempotency_key is required" });

  const found = await pool.query(
    `SELECT idempotency_key, txn_id, amount, outcome, response_sent, created_at
       FROM provider_transactions WHERE idempotency_key = $1`,
    [key],
  );

  if (found.rowCount === 0) {
    // Authoritative "we never saw this charge" — safe to treat as not charged.
    return res.status(404).json({ found: false, idempotency_key: key });
  }
  res.json({ found: true, ...found.rows[0] });
});

app.post("/charge", async (req, res) => {
  const idempotencyKey = req.headers["idempotency-key"];
  const { order_id: orderId, amount, currency, payment_id: paymentId } = req.body || {};

  if (!idempotencyKey) {
    return res.status(400).json({ error: "Idempotency-Key header is required" });
  }
  if (!amount || !orderId) {
    return res.status(400).json({ error: "order_id and amount are required" });
  }

  const mode = modeForOrder(orderId);
  const log = logger.child({ idempotencyKey, orderId, paymentId, mode });

  // An already-known key is replayed verbatim, whatever the current mode says.
  // This mirrors how a real provider protects a client that retries.
  const known = await pool.query(
    `SELECT txn_id, outcome FROM provider_transactions WHERE idempotency_key = $1`,
    [idempotencyKey],
  );
  if (known.rowCount > 0) {
    log.info("Charge replayed from provider ledger", { txnId: known.rows[0].txn_id });
    const row = known.rows[0];
    return row.outcome === "APPROVED"
      ? res.json({ approved: true, transaction_id: row.txn_id, code: "APPROVED", replayed: true })
      : res
          .status(402)
          .json({ approved: false, code: "DECLINED", reason: "Card declined", replayed: true });
  }

  await sleep(behavior.latencyMs);

  if (behavior.failureRate > 0 && Math.random() < behavior.failureRate) {
    log.warn("Charge failed by injected failure rate");
    return res.status(503).json({ error: "provider temporarily unavailable" });
  }

  switch (mode) {
    case "fail": {
      await recordTransaction({ idempotencyKey, amount, outcome: "DECLINED", responseSent: true });
      log.info("Charge declined");
      return res
        .status(402)
        .json({ approved: false, code: "DECLINED", reason: "Insufficient funds" });
    }

    case "flaky": {
      // 5xx before doing any work: genuinely safe to retry.
      log.warn("Charge returned transient 503");
      return res.status(503).json({ error: "gateway busy, retry" });
    }

    case "slow": {
      await sleep(Number(process.env.PROVIDER_SLOW_MS || 1500));
      const txn = await recordTransaction({
        idempotencyKey,
        amount,
        outcome: "APPROVED",
        responseSent: true,
      });
      log.info("Charge approved after delay", { txnId: txn.txn_id });
      return res.json({ approved: true, transaction_id: txn.txn_id, code: "APPROVED" });
    }

    case "timeout": {
      // Nothing recorded, and we never answer. The caller's outcome is unknown,
      // but reconciliation will find no transaction and can safely fail it.
      log.warn("Charge hanging without recording (timeout mode)");
      return; // deliberately no response
    }

    case "lost": {
      // The dangerous one: the money moved, and the caller will never hear it.
      // Only a lookup by idempotency key can discover the truth.
      const txn = await recordTransaction({
        idempotencyKey,
        amount,
        outcome: "APPROVED",
        responseSent: false,
      });
      log.warn("Charge APPROVED but response withheld (lost-response mode)", {
        txnId: txn.txn_id,
      });
      return; // deliberately no response
    }

    default: {
      const txn = await recordTransaction({
        idempotencyKey,
        amount,
        outcome: "APPROVED",
        responseSent: true,
      });
      log.info("Charge approved", { txnId: txn.txn_id, amount, currency });
      return res.json({ approved: true, transaction_id: txn.txn_id, code: "APPROVED" });
    }
  }
});

async function main() {
  installSignalHandlers();
  await waitForDependencies();

  const server = app.listen(PORT, () => logger.info("Mock provider listening", { port: PORT }));
  onShutdown("http", () => new Promise((resolve) => server.close(resolve)));
}

main().catch((err) => {
  logger.error("Fatal startup error", { err });
  process.exit(1);
});
