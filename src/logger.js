// Structured JSON logging. Every line carries the service name and, wherever we
// have one, the correlation ID — that is what makes a single workflow traceable
// across six containers with `docker compose logs | grep <correlationId>`.

const SERVICE = process.env.SERVICE_NAME || "app";
const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };
const MIN_LEVEL = LEVELS[process.env.LOG_LEVEL || "info"] || LEVELS.info;

function emit(level, message, context = {}) {
  if (LEVELS[level] < MIN_LEVEL) return;

  const line = {
    ts: new Date().toISOString(),
    level,
    service: SERVICE,
    message,
    ...context,
  };

  // Errors do not serialise usefully through JSON.stringify.
  if (line.err instanceof Error) {
    line.err = { name: line.err.name, message: line.err.message, stack: line.err.stack };
  }

  const out = level === "error" || level === "warn" ? process.stderr : process.stdout;
  out.write(JSON.stringify(line) + "\n");
}

const logger = {
  debug: (msg, ctx) => emit("debug", msg, ctx),
  info: (msg, ctx) => emit("info", msg, ctx),
  warn: (msg, ctx) => emit("warn", msg, ctx),
  error: (msg, ctx) => emit("error", msg, ctx),

  // Returns a logger that stamps every line with the given context, so handlers
  // do not have to thread correlationId through each call.
  child(bound) {
    return {
      debug: (msg, ctx) => emit("debug", msg, { ...bound, ...ctx }),
      info: (msg, ctx) => emit("info", msg, { ...bound, ...ctx }),
      warn: (msg, ctx) => emit("warn", msg, { ...bound, ...ctx }),
      error: (msg, ctx) => emit("error", msg, { ...bound, ...ctx }),
      child: (more) => logger.child({ ...bound, ...more }),
    };
  },
};

module.exports = logger;
