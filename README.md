# Private Router Manager

**Manage Ericsson routers across your private network from one place.**

Private Router Manager is a web-based tool for administrating routers in private networks. Load a JSON or CSV router file and perform bulk operations—deploying licenses, firmware, or configurations, fetching router details, monitoring connectivity, or calling REST APIs—across hundreds or thousands of devices in parallel. Built for teams that need to maintain and provision large fleets of routers without relying on cloud management.

<img width="1641" height="854" alt="image" src="https://github.com/user-attachments/assets/9d8210f6-38c8-4a1c-a66c-5d89ade8d7a0" />

## Highlights

- **Router roster** — JSON/CSV editor with row selection, sortable columns, pagination, and lock protection
- **Discover Routers** — Find routers by IP subnet or range
- **Poll Routers** — Fetch Hostname, MAC, Serial, Product Name, NCOS, and API path columns from routers. Manual or auto-poll (default: every 60 minutes)
- **Backup Configurations** (Monitoring tab) — Save config from selected routers to `configs/`
- **Deploy** — Licenses, NCOS firmware, configuration files, or SDK apps to all routers at once
- **Ping monitoring** — Check connectivity with auto-ping and offline event logging
- **Router API** — Make calls to the router API across multiple routers; GET/PUT/POST/DELETE with wildcard paths; copy to routers for auto polling
- **Logs and CSV** — View, download, and manage logs and exported CSV files
- **Scaling** — Configurable worker limits, API pagination, optional async I/O, and Prometheus metrics for large fleets

## Quick Start

```bash
pip install -r requirements.txt
python app.py
```

Open **http://localhost:9000** in your browser.

## Requirements

- Python 3.7+
- Flask, requests  
- For SDK Apps: `sshpass` (macOS/Linux) or PuTTY `pscp.exe` (Windows; place in `bin/win/` or ensure it's in PATH)

For detailed instructions, click the <strong>Help</strong> (?) button in the app or open <code>User Guide.html</code>.

## Starting the App

| Mode | Command | Use case |
|------|---------|----------|
| **Development** | `python app.py` | Local use, debugging |
| **Production** | `gunicorn -c gunicorn_config.py app:app` | Large fleets, shared access |

**Settings (gear icon)** — Configure connection timeout, retries, max workers, worker formula, and async I/O from the UI. Changes are saved to `app_config.json`.

## Production Deployment and Scaling

For thousands of routers, use Gunicorn instead of the Flask dev server:

```bash
pip install -r requirements.txt
gunicorn -c gunicorn_config.py app:app
```

The app processes routers in **batches** (not all at once). The `max_workers` setting caps concurrent connections—e.g. with 5,000 routers and cap 64, at most 64 connections run in parallel; the rest are queued.

### Configuration (`app_config.json`)

- **max_workers** (default: 64) — Cap on concurrent connections/workers for bulk operations
- **max_workers_formula** — `"sqrt"` (scale with √router count), `"cpu"` (CPU-bound), or `"linear"`
- **max_workers_per_cpu** — Multiplier for CPU-based formula (default: 4)
- **use_async_client** — Set `true` to use asyncio + aiohttp for Router API and Discover (faster for large fleets)
- **connection_timeout**, **connection_retries** — Per-router connection settings

All of these can also be set via **Settings** (gear icon).

### API Pagination

List endpoints support `?page=` and `?per_page=` for large result sets. Omit for backward-compatible full responses.

### Monitoring

- **GET /api/monitoring/metrics** — JSON: connection counts, timeouts, errors, memory usage

### Prometheus

**What is Prometheus?** Prometheus is an open-source monitoring system that collects metrics from your apps by periodically scraping HTTP endpoints. It stores time-series data and lets you query it, create dashboards (e.g. via Grafana), and set up alerts.

**Setting up Prometheus with Private Router Manager:**

1. **Install the client library** (optional; without it, `/metrics` returns 501):
   ```bash
   pip install prometheus_client
   ```

2. **Start the app** so it exposes `http://localhost:9000/metrics`.

3. **Install and configure Prometheus** (e.g. on the same host or a monitoring server):
   - Download from [prometheus.io](https://prometheus.io/download/)
   - Edit `prometheus.yml` and add a scrape config:
   ```yaml
   scrape_configs:
     - job_name: 'router-manager'
       static_configs:
         - targets: ['localhost:9000']  # or your app host:port
   ```
   - Run: `./prometheus --config.file=prometheus.yml`

4. **Optional: Grafana** — Add Prometheus as a data source and build dashboards for `router_manager_*` metrics (connections, timeouts, memory, etc.).

### Streaming

- **get-router-info** with `stream: true` returns NDJSON for incremental progress when polling many routers.
