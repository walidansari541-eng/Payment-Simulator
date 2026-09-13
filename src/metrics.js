// Minimal in-process Prometheus-text metrics. No dependency, and enough to
// answer the Week 8 question "which metrics would you monitor?" with real
// numbers instead of a list.
//
// Counters live per-process, so each container exposes its own. In a real
// deployment Prometheus scrapes all of them and sums by job.

const counters = new Map();
const gauges = new Map();
const histograms = new Map();

const HELP = {
  workflow_started_total: "Sagas started",
  workflow_completed_total: "Sagas that reached COMPLETED",
  workflow_failed_total: "Sagas that reached FAILED",
  workflow_compensated_total: "Sagas rolled back cleanly",
  workflow_compensation_failed_total: "Sagas whose compensation itself failed",
  payment_succeeded_total: "Payments approved by the provider",
  payment_failed_total: "Payments declined or exhausted",
  payment_unknown_total: "Payments with an undetermined provider outcome",
  payment_retry_total: "Transient payment retries scheduled",
  payment_reconciled_total: "UNKNOWN payments resolved by the reconciler",
  outbox_published_total: "Outbox rows successfully published",
  outbox_publish_errors_total: "Outbox publish failures",
  events_duplicate_total: "Events skipped by the inbox as duplicates",
  events_dlq_total: "Messages parked on a dead-letter queue",
};

const DURATION_BUCKETS = [0.5, 1, 2, 5, 10, 30, 60, 120];

function key(name, labels) {
  if (!labels || Object.keys(labels).length === 0) return name;
  const parts = Object.entries(labels)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}="${String(v).replace(/"/g, "")}"`);
  return `${name}{${parts.join(",")}}`;
}

function inc(name, labels, by = 1) {
  const k = key(name, labels);
  counters.set(k, (counters.get(k) || 0) + by);
}

function setGauge(name, value, labels) {
  gauges.set(key(name, labels), value);
}

function observe(name, seconds) {
  let h = histograms.get(name);
  if (!h) {
    h = { counts: new Array(DURATION_BUCKETS.length).fill(0), sum: 0, count: 0 };
    histograms.set(name, h);
  }
  h.sum += seconds;
  h.count += 1;
  DURATION_BUCKETS.forEach((bucket, i) => {
    if (seconds <= bucket) h.counts[i] += 1;
  });
}

// Gauges that must be read from the database at scrape time rather than
// tracked incrementally (queue depth style metrics).
async function collectDbGauges(pool) {
  const queries = [
    ["outbox_pending", `SELECT COUNT(*)::int AS n FROM outbox_events WHERE status = 'PENDING'`],
    ["outbox_failed", `SELECT COUNT(*)::int AS n FROM outbox_events WHERE status = 'FAILED'`],
    [
      "workflows_in_flight",
      `SELECT COUNT(*)::int AS n FROM workflow_executions
        WHERE current_state NOT IN ('COMPLETED','FAILED','COMPENSATED','COMPENSATION_FAILED')`,
    ],
    ["payments_unknown", `SELECT COUNT(*)::int AS n FROM payments WHERE status = 'UNKNOWN'`],
  ];

  for (const [name, sql] of queries) {
    try {
      const res = await pool.query(sql);
      setGauge(name, res.rows[0].n);
    } catch {
      // A scrape must never take the service down.
    }
  }
}

function render() {
  const lines = [];

  for (const [k, v] of counters) {
    const base = k.split("{")[0];
    if (HELP[base]) lines.push(`# HELP ${base} ${HELP[base]}`);
    lines.push(`# TYPE ${base} counter`);
    lines.push(`${k} ${v}`);
  }

  for (const [k, v] of gauges) {
    const base = k.split("{")[0];
    lines.push(`# TYPE ${base} gauge`);
    lines.push(`${k} ${v}`);
  }

  for (const [name, h] of histograms) {
    lines.push(`# TYPE ${name} histogram`);
    DURATION_BUCKETS.forEach((bucket, i) => {
      lines.push(`${name}_bucket{le="${bucket}"} ${h.counts[i]}`);
    });
    lines.push(`${name}_bucket{le="+Inf"} ${h.count}`);
    lines.push(`${name}_sum ${h.sum}`);
    lines.push(`${name}_count ${h.count}`);
  }

  return lines.join("\n") + "\n";
}

module.exports = { inc, setGauge, observe, collectDbGauges, render };
