const crypto = require("crypto");
const express = require("express");
const paymentRoutes = require("./route");
const logger = require("./logger");

const app = express();

app.use(express.json());

// Every request gets a correlation ID, echoed back on the response, so a client
// can quote it and we can grep six containers' logs for one workflow.
app.use((req, res, next) => {
  req.correlationId = req.headers["x-correlation-id"] || `corr_${crypto.randomUUID()}`;
  res.set("x-correlation-id", req.correlationId);

  const startedAt = Date.now();
  res.on("finish", () => {
    // Health/metrics polling would drown the log otherwise.
    if (req.path === "/health" || req.path === "/metrics") return;
    logger.info("http request", {
      method: req.method,
      path: req.path,
      status: res.statusCode,
      durationMs: Date.now() - startedAt,
      correlationId: req.correlationId,
    });
  });

  next();
});

app.use(paymentRoutes);

app.use((req, res) => res.status(404).json({ error: `no route for ${req.method} ${req.path}` }));

// Express 5 forwards rejected async handlers here, so a thrown error returns
// JSON rather than an HTML stack trace.
app.use((err, req, res, _next) => {
  logger.error("Unhandled request error", { err, path: req.path, correlationId: req.correlationId });
  res.status(500).json({ error: "Internal server error", correlationId: req.correlationId });
});

module.exports = app;
