// ---------------------------------------------------------------------------
// Payment API. Accepts checkouts, exposes workflow/payment state, and runs an
// outbox relay so the first command of a saga gets published.
// ---------------------------------------------------------------------------

require("dotenv").config();
process.env.SERVICE_NAME = process.env.SERVICE_NAME || "api";

const app = require("../app");
const { connectQueue } = require("../queue");
const { startOutboxRelay, stopOutboxRelay } = require("../outboxRelay");
const { installSignalHandlers, onShutdown, waitForDependencies } = require("../lifecycle");
const logger = require("../logger");

const PORT = Number(process.env.PORT || 3000);

async function main() {
  installSignalHandlers();
  await waitForDependencies();
  await connectQueue();
  await startOutboxRelay();

  const server = app.listen(PORT, () => logger.info("API listening", { port: PORT }));

  // Stop accepting connections first, then drain the relay.
  onShutdown("outbox-relay", stopOutboxRelay);
  onShutdown("http", () => new Promise((resolve) => server.close(resolve)));
}

main().catch((err) => {
  logger.error("Fatal startup error", { err });
  process.exit(1);
});
