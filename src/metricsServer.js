const http = require("http");
const pool = require("./db");
const metrics = require("./metrics");
const logger = require("./logger");

// Counters are per-process, so a worker with no HTTP port would be invisible to
// monitoring — and the interesting counters (saga outcomes, unknown payments)
// live in exactly those workers. Every service therefore exposes a tiny
// /metrics + /health server of its own.
function startMetricsServer(port = Number(process.env.METRICS_PORT || 9100)) {
  const server = http.createServer(async (req, res) => {
    if (req.url === "/health") {
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ status: "ok", service: process.env.SERVICE_NAME }));
    }
    if (req.url === "/metrics") {
      await metrics.collectDbGauges(pool);
      res.writeHead(200, { "content-type": "text/plain; version=0.0.4" });
      return res.end(metrics.render());
    }
    res.writeHead(404).end();
  });

  server.listen(port, () => logger.info("Metrics endpoint listening", { port }));
  return { stop: () => new Promise((resolve) => server.close(resolve)) };
}

module.exports = { startMetricsServer };
