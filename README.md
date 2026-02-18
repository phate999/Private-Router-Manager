# Private Router Manager

**Manage Ericsson routers across your network from one place.**

Private Router Manager is a web-based tool for administrating routers in private networks. Load a JSON or CSV router file and perform bulk operations—deploying licenses, firmware, or configurations, fetching router details, monitoring connectivity, or calling REST APIs—across hundreds of devices in parallel. Built for teams that need to maintain and provision large fleets of routers without relying on cloud management.

## Highlights

- **Router roster** — JSON/CSV editor with row selection, sortable columns, pagination, and lock protection
- **Discover Routers** — Find routers by IP subnet or range
- **Poll Routers** — Fetch Hostname, MAC, Serial, Product Name, NCOS, and API path columns from routers. Manual or auto-poll (default: every 60 minutes)
- **Backup Configurations** (Monitoring tab) — Save config from selected routers to `configs/`
- **Deploy** — Licenses, NCOS firmware, configuration files, or SDK apps to all routers at once
- **Ping monitoring** — Check connectivity with auto-ping and offline event logging
- **Router API** — Make calls to the router API across multiple routers; GET/PUT/POST/DELETE with wildcard paths; copy to routers for auto polling
- **Logs and CSV** — View, download, and manage logs and exported CSV files

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
