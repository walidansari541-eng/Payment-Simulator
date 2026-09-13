const pool = require("./db");
const { closeQueue } = require("./queue");
const logger = require("./logger");

// Graceful shutdown, shared by every service.
//
// Docker sends SIGTERM and waits ~10s before SIGKILL. In that window we stop
// accepting new work, let in-flight handlers finish so their acks land, and
// close the broker connection and the DB pool. Skipping this is how you end up
// with unacked messages and half-finished transactions after every deploy.

const closers = [];
let shuttingDown = false;

function onShutdown(name, fn) {
  closers.push({ name, fn });
}

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.info("Shutdown initiated", { signal });

  const timeout = setTimeout(() => {
    logger.error("Shutdown timed out, forcing exit");
    process.exit(1);
  }, Number(process.env.SHUTDOWN_TIMEOUT_MS || 15000)).unref();

  for (const { name, fn } of closers.reverse()) {
    try {
      await fn();
      logger.info("Closed", { component: name });
    } catch (err) {
      logger.error("Error during shutdown", { component: name, err });
    }
  }

  await closeQueue().catch(() => {});
  await pool.end().catch(() => {});

  clearTimeout(timeout);
  logger.info("Shutdown complete");
  process.exit(0);
}

function installSignalHandlers() {
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("unhandledRejection", (err) => logger.error("Unhandled rejection", { err }));
  process.on("uncaughtException", (err) => {
    logger.error("Uncaught exception", { err });
    shutdown("uncaughtException");
  });
}

// Postgres and RabbitMQ may not be accepting connections the instant the
// container starts, even with compose healthchecks. Retry rather than crash-loop.
async function waitForDependencies({ retries = 30, delayMs = 1000 } = {}) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await pool.query("SELECT 1");
      return;
    } catch (err) {
      if (attempt === retries) throw err;
      logger.warn("Database not ready, retrying", { attempt, err: err.message });
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

module.exports = { onShutdown, installSignalHandlers, waitForDependencies };
