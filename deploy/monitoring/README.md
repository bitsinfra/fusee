# Monitoring Fusee

Every engine and server serves Prometheus text at `GET /metrics` once its
HTTP API is on (`fusee server --http ADDR`, `engine.serve()` in an SDK, or
`Config::http` in Rust). Each series is per shard (`shard` label).

- `fusee-alerts.yml`: alert rules. Load them with `rule_files:`. Each alert
  says what to do: stuck runs, paused runs, runs waiting for code, determinism errors,
  slow flushes, busy refusals, stalls.
- `grafana-dashboard.json`: a dashboard over the same series. Import it in
  Grafana (Dashboards → New → Import) and pick the Prometheus data source.

The gauges `fusee_runs_stuck`, `fusee_runs_paused` and `fusee_runs_waiting_for_code` are counted
from the run list when `/metrics` is read, not on the step path.
