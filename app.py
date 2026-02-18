"""
Private Router Manager - Flask backend
Manages Cradlepoint routers: JSON editor, file deployment (Licenses, NCOS, Configuration, SDK Apps)
"""

import csv
import io
import ipaddress
import json
import os
import platform
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from flask import Flask, request, jsonify, render_template, send_file

NCOS_DEVICE_URL = "https://www.cradlepointecm.com/api/v2/firmwares/?limit=500&version="
NCOS_FIRMWARE_BASE = "https://d251cfg5d9gyuq.cloudfront.net"
APP_ROOT = Path(__file__).parent.resolve()
API_KEYS_FILE = APP_ROOT / "api_keys.json"
APP_CONFIG_FILE = APP_ROOT / "app_config.json"
ROUTERS_DIR = APP_ROOT / "routers"

# Migrate from old credentials_config.json to app_config.json if needed
_OLD_CREDENTIALS_FILE = APP_ROOT / "credentials_config.json"

# Disable SSL warnings for isolated networks
requests.packages.urllib3.disable_warnings()

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Router schema: state, ip_address, hostname, mac, serial_number, product_name, ncos_version, username, password, port, created_at
ROUTER_KEYS = ["state", "ip_address", "hostname", "mac", "serial_number", "product_name", "ncos_version", "username", "password", "port", "created_at"]

# In-memory routers store (list of router dicts)
routers_data = []

# Deployment type config: folder, endpoint, file extension
DEPLOYMENT_TYPES = {
    "licenses": {"folder": "licenses", "endpoint": "feature", "ext": ".lic"},
    "ncos": {"folder": "NCOS", "endpoint": "fw_upgrade", "ext": ".bin"},
    "configuration": {"folder": "configs", "endpoint": "config_save", "ext": ".bin"},
    "sdk_apps": {"folder": "sdk_apps", "endpoint": "app_upload", "ext": ".tar.gz"},
}


def ensure_folders():
    """Create routers, logs, csv (legacy), and all deployment folders if they don't exist."""
    ROUTERS_DIR.mkdir(parents=True, exist_ok=True)
    (APP_ROOT / "csv").mkdir(parents=True, exist_ok=True)
    (APP_ROOT / "logs").mkdir(parents=True, exist_ok=True)
    for cfg in DEPLOYMENT_TYPES.values():
        (APP_ROOT / cfg["folder"]).mkdir(parents=True, exist_ok=True)


def _migrate_app_config():
    """Migrate from credentials_config.json to app_config.json (preserve last_file only)."""
    if _OLD_CREDENTIALS_FILE.exists() and not APP_CONFIG_FILE.exists():
        try:
            with open(_OLD_CREDENTIALS_FILE, encoding="utf-8") as f:
                old = json.load(f)
            last = old.get("last_file", "routers.csv")
            cfg = {"last_file": "routers.json" if not last.endswith(".json") else last}
            if not cfg["last_file"].endswith(".json"):
                cfg["last_file"] = "routers.json"
            with open(APP_CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2)
        except (json.JSONDecodeError, IOError):
            pass



def _empty_router():
    """Return a new router dict with default values. state defaults to Online until ping proves otherwise."""
    return {
        "state": "Online",
        "ip_address": "",
        "hostname": "",
        "mac": "",
        "serial_number": "",
        "product_name": "",
        "ncos_version": "",
        "username": "admin",
        "password": "",
        "port": 8080,
        "created_at": "",
    }


def _expand_ip_range(spec):
    """
    Expand IP subnet or range to list of IP addresses.
    Supports: 192.168.1.0/24, 192.168.1.1-10, 192.168.1.1-192.168.1.10, single IPs.
    """
    spec = str(spec).strip()
    if not spec:
        return []
    ips = set()

    # Split by comma for multiple specs
    for part in re.split(r"[\s,]+", spec):
        part = part.strip()
        if not part:
            continue
        if "/" in part:
            try:
                net = ipaddress.ip_network(part, strict=False)
                for ip in net.hosts():
                    ips.add(str(ip))
            except ValueError:
                continue
        elif "-" in part:
            parts = part.split("-", 1)
            try:
                start = ipaddress.ip_address(parts[0].strip())
                end_s = parts[1].strip()
                if "." in end_s:
                    end = ipaddress.ip_address(end_s)
                else:
                    end = ipaddress.ip_address(str(start).rsplit(".", 1)[0] + "." + end_s)
                if start > end:
                    start, end = end, start
                addr = start
                while addr <= end:
                    ips.add(str(addr))
                    addr += 1
            except ValueError:
                continue
        else:
            try:
                ipaddress.ip_address(part)
                ips.add(part)
            except ValueError:
                continue
    return sorted(ips)


ensure_folders()
_migrate_app_config()


def _get_sshpass_path():
    """Return path to sshpass: bundled macOS binary if available, else 'sshpass' for system PATH."""
    if sys.platform != "darwin":
        return "sshpass"
    arch = platform.machine().lower()
    if arch not in ("arm64", "x86_64"):
        return "sshpass"
    bundled = APP_ROOT / "bin" / "macos" / arch / "sshpass"
    if bundled.exists() and os.access(bundled, os.X_OK):
        return str(bundled)
    return "sshpass"


def _get_pscp_path():
    """Return path to pscp: bundled bin/win/pscp.exe if available, else 'pscp.exe' for system PATH."""
    if sys.platform != "win32":
        return "pscp.exe"
    bundled = APP_ROOT / "bin" / "win" / "pscp.exe"
    if bundled.exists():
        return str(bundled)
    return "pscp.exe"


def format_uptime(seconds):
    """Format uptime in seconds: <=24h as HH:MM:SS, >24h rounded to nearest # days"""
    try:
        s = int(float(seconds))
    except (TypeError, ValueError):
        return ""
    if s < 0:
        return ""
    if s <= 86400:  # <= 24 hours
        h, r = divmod(s, 3600)
        m, sec = divmod(r, 60)
        return f"{int(h):02d}:{int(m):02d}:{int(sec):02d}"
    days = round(s / 86400)
    return f"{days} day{'s' if days != 1 else ''}"


def push_sdk_app_via_scp(ip, port, username, password, file_path, result_lines):
    """Push SDK app archive to router via SCP to /app_upload. Appends result to result_lines list. Returns True on success, False on failure."""
    app_archive = str(Path(file_path).resolve())

    def log(msg):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts} {msg}"
        result_lines.append(line)
        print(line)

    try:
        if sys.platform == "win32":
            cmd = [
                _get_pscp_path(),
                "-pw", password,
                "-P", str(port),
                "-batch",
                app_archive,
                f"{username}@{ip}:/app_upload",
            ]
        else:
            sshpass_cmd = _get_sshpass_path()
            cmd = [
                sshpass_cmd, "-p", password,
                "scp", "-O",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "StrictHostKeyChecking=no",
                "-P", str(port),
                app_archive,
                f"{username}@{ip}:/app_upload",
            ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        out = (result.stdout or "").strip()
        err = (result.stderr or "").strip()
        combined = (out + "\n" + err).lower()
        # Failure indicators: router offline or unreachable
        failure_phrases = [
            "connection refused", "no route to host", "network is unreachable",
            "connection timed out", "timed out", "host is unreachable", "connection reset by peer",
            "could not resolve host", "name or service not known",
        ]
        has_failure = any(p in combined for p in failure_phrases)
        # Cradlepoint routers close the connection after upload; SCP reports "lost connection" - that is success
        # But "lost connection" can also appear with connection refused in some cases - trust failure indicators first
        is_success = (
            result.returncode == 0 or
            ("lost connection" in combined and not has_failure)
        )
        if is_success:
            log(f"*** SUCCESS *** Pushed to {ip}:{port}")
            if out:
                log(out)
            if err and "lost connection" in combined:
                log("(Connection closed after transfer - normal for Cradlepoint)")
            return True
        else:
            log(f"*** FAILURE *** {username}@{ip}:/app_upload - {err or out or 'scp failed'}")
            return False
    except subprocess.TimeoutExpired:
        log(f"*** FAILURE *** SCP timed out after 10 seconds for {ip}:{port}")
        return False
    except FileNotFoundError as e:
        log(f"*** FAILURE *** SCP tool not found. Install sshpass (macOS/Linux) or pscp.exe (Windows): {e}")
        return False
    except Exception as e:
        log(f"*** FAILURE *** Exception: {e}")
        return False


def push_to_router(ip, port, username, password, file_path, action, result_lines):
    """Push file to a single router. Appends result to result_lines list."""
    base = f"http://{ip}:{port}" if ":" not in ip else f"http://{ip}"
    product_url = f"{base}/api/status/product_info"
    system_id_url = f"{base}/api/config/system/system_id"
    deploy_url = f"{base}/{action}"
    auth = (username, password)

    def log(msg):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts} {msg}"
        result_lines.append(line)
        print(line)

    try:
        req = requests.get(product_url, auth=auth, verify=False, timeout=10)
        if req.status_code >= 300:
            log(f"*** FAILURE *** Cannot connect to {base}: {req.status_code} {req.text}")
            return
        data = req.json().get("data", {})
        sys_req = requests.get(system_id_url, auth=auth, verify=False, timeout=5)
        system_id = sys_req.json().get("data", "unknown") if sys_req.ok else "unknown"
        log(f"Connected to {system_id} at {base}: {data.get('product_name', 'N/A')}")

        with open(file_path, "rb") as f:
            file_data = {"file": (os.path.basename(file_path), f, "application/octet-stream")}
            resp = requests.post(deploy_url, files=file_data, auth=auth, verify=False, timeout=120)
        if resp.status_code < 300:
            log(f"*** SUCCESS *** Pushed to {system_id}.")
        else:
            log(f"*** FAILURE *** {deploy_url}: {resp.status_code} {resp.text}")
    except Exception as e:
        log(f"*** FAILURE *** Exception: {e}")


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/user-guide")
def user_guide():
    """Serve User Guide HTML (for new-tab fallback)"""
    guide_path = APP_ROOT / "User Guide.html"
    if not guide_path.exists():
        return "", 404
    try:
        return send_file(guide_path, mimetype="text/html")
    except IOError:
        return "", 500


@app.route("/api/user-guide")
def api_user_guide():
    """Return User Guide body content for help modal"""
    guide_path = APP_ROOT / "User Guide.html"
    if not guide_path.exists():
        return jsonify({"error": "User Guide not found"}), 404
    try:
        content = guide_path.read_text(encoding="utf-8")
        match = re.search(r"<body[^>]*>(.*?)</body>", content, re.DOTALL | re.IGNORECASE)
        body_html = match.group(1).strip() if match else content
        return jsonify({"html": body_html})
    except IOError:
        return jsonify({"error": "Failed to read User Guide"}), 500


@app.route("/api/readme")
def readme():
    """Serve README.md as plain text for help modal"""
    readme_path = Path(__file__).parent / "README.md"
    if not readme_path.exists():
        return "", 404
    try:
        return readme_path.read_text(encoding="utf-8"), 200, {"Content-Type": "text/plain; charset=utf-8"}
    except IOError:
        return "", 500


def _load_app_config():
    """Load app config with defaults. Returns dict."""
    defaults = {"last_file": "", "connection_timeout": 2, "connection_retries": 1}
    if not APP_CONFIG_FILE.exists():
        return defaults
    try:
        with open(APP_CONFIG_FILE, encoding="utf-8") as f:
            cfg = json.load(f)
        for k in defaults:
            if k not in cfg:
                cfg[k] = defaults[k]
        cfg["connection_timeout"] = max(1, min(60, int(cfg.get("connection_timeout", 2))))
        cfg["connection_retries"] = max(0, min(10, int(cfg.get("connection_retries", 1))))
        return cfg
    except (json.JSONDecodeError, IOError):
        return defaults


@app.route("/api/config/app", methods=["GET", "POST"])
def app_config():
    """Get or save app config (last_file, connection_timeout, connection_retries)."""
    if request.method == "GET":
        cfg = _load_app_config()
        return jsonify({
            "last_file": cfg.get("last_file", ""),
            "connection_timeout": cfg.get("connection_timeout", 2),
            "connection_retries": cfg.get("connection_retries", 1),
        })
    data = request.get_json() or {}
    cfg = _load_app_config()
    if "last_file" in data:
        cfg["last_file"] = (data.get("last_file") or "").strip()
    if "connection_timeout" in data:
        cfg["connection_timeout"] = max(1, min(60, int(data.get("connection_timeout", 2))))
    if "connection_retries" in data:
        cfg["connection_retries"] = max(0, min(10, int(data.get("connection_retries", 1))))
    with open(APP_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    return jsonify({"ok": True})


def _normalize_router(r):
    """Ensure router dict has all keys with correct types. Preserves extra keys (e.g. deploy results)."""
    out = _empty_router()
    for k in ROUTER_KEYS:
        if k in r:
            v = r[k]
            if k == "port":
                try:
                    out[k] = int(v) if v not in (None, "") else 8080
                except (ValueError, TypeError):
                    out[k] = 8080
            else:
                out[k] = str(v) if v is not None else ""
    for k, v in r.items():
        if k not in ROUTER_KEYS and v not in (None, ""):
            out[k] = v
    return out


def _csv_to_routers(content):
    """Convert legacy CSV content to list of router dicts (for migration)."""
    reader = csv.reader(io.StringIO(content))
    rows = list(reader)
    if not rows:
        return []
    headers = [str(h).strip().lower().replace(" ", "_") for h in rows[0]]
    aliases = {
        "ip_address": ["ip_address", "ipaddress", "ip"],
        "hostname": ["hostname"],
        "mac": ["mac", "mac_address"],
        "serial_number": ["serial", "serial_number"],
        "product_name": ["product", "product_name"],
        "ncos_version": ["ncos", "ncos_version"],
        "username": ["username", "user"],
        "password": ["password", "pass"],
        "port": ["port"],
    }
    col_map = {}
    for k, aliases_list in aliases.items():
        for i, h in enumerate(headers):
            if any(a in h for a in aliases_list):
                col_map[k] = i
                break
    if "ip_address" not in col_map:
        for i, h in enumerate(headers):
            if "ip" in h:
                col_map["ip_address"] = i
                break
    if "ip_address" not in col_map:
        return []
    routers = []
    for row in rows[1:]:
        r = _empty_router()
        for k, idx in col_map.items():
            if idx < len(row):
                v = row[idx]
                if k == "port":
                    try:
                        r[k] = int(v) if v else 8080
                    except (ValueError, TypeError):
                        r[k] = 8080
                else:
                    r[k] = str(v or "").strip()
        if r["ip_address"]:
            routers.append(r)
    return routers


@app.route("/api/routers/list")
def routers_list():
    """List JSON router files in the routers folder."""
    ensure_folders()
    files = []
    if ROUTERS_DIR.exists():
        for f in sorted(ROUTERS_DIR.iterdir()):
            if f.is_file() and f.suffix.lower() == ".json":
                files.append({"name": f.name})
    return jsonify({"files": files})


@app.route("/api/routers/open")
def routers_open():
    """Load a routers JSON file (or migrate from legacy CSV)."""
    filename = request.args.get("filename", "").strip()
    if not filename:
        return jsonify({"error": "Invalid filename"}), 400
    if filename.lower().endswith(".csv"):
        filepath = APP_ROOT / "csv" / filename
        if not filepath.exists():
            filepath = ROUTERS_DIR / filename.replace(".csv", ".json")
    else:
        if not filename.lower().endswith(".json"):
            filename += ".json"
        filepath = ROUTERS_DIR / filename
        if not filepath.exists() and filename == "routers.json":
            old_csv = APP_ROOT / "csv" / "routers.csv"
            if old_csv.exists():
                try:
                    content = old_csv.read_text(encoding="utf-8-sig", errors="replace")
                    migrated = _csv_to_routers(content)
                    if migrated:
                        ROUTERS_DIR.mkdir(parents=True, exist_ok=True)
                        with open(filepath, "w", encoding="utf-8") as f:
                            json.dump({"routers": [_normalize_router(r) for r in migrated]}, f, indent=2)
                except (IOError, json.JSONDecodeError):
                    pass
    if not filepath.exists() or not filepath.is_file():
        return jsonify({"error": "File not found"}), 404
    try:
        content = filepath.read_text(encoding="utf-8-sig", errors="replace")
    except IOError as e:
        return jsonify({"error": str(e)}), 500
    if filepath.suffix.lower() == ".csv":
        routers = _csv_to_routers(content)
    else:
        try:
            data = json.loads(content)
            raw = data.get("routers", data) if isinstance(data, dict) else data
            routers = [_normalize_router(r) for r in raw] if isinstance(raw, list) else []
        except json.JSONDecodeError:
            return jsonify({"error": "Invalid JSON"}), 400
    routers_data.clear()
    routers_data.extend(routers)
    return jsonify({"routers": list(routers_data)})


@app.route("/api/routers/upload", methods=["POST"])
def routers_upload():
    if "file" not in request.files:
        return jsonify({"error": "No file"}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "No filename"}), 400
    fn_lower = f.filename.lower()
    content = f.read().decode("utf-8-sig", errors="replace")
    if fn_lower.endswith(".csv"):
        routers = _csv_to_routers(content)
    elif fn_lower.endswith(".json"):
        try:
            data = json.loads(content)
            raw = data.get("routers", data) if isinstance(data, dict) else data
            routers = [_normalize_router(r) for r in raw] if isinstance(raw, list) else []
        except json.JSONDecodeError:
            return jsonify({"error": "Invalid JSON"}), 400
    else:
        return jsonify({"error": "File must be .json or .csv"}), 400
    if not routers:
        return jsonify({"error": "No valid router data"}), 400
    routers_data.clear()
    routers_data.extend(routers)
    return jsonify({"routers": list(routers_data)})


@app.route("/api/routers/download")
def routers_download():
    """Download routers as JSON from server state."""
    if not routers_data:
        return jsonify({"error": "No router data"}), 400
    payload = {"routers": [_normalize_router(r) for r in routers_data]}
    out = json.dumps(payload, indent=2)
    return send_file(
        io.BytesIO(out.encode("utf-8")),
        mimetype="application/json",
        as_attachment=True,
        download_name="routers.json",
    )


@app.route("/api/routers/exists")
def routers_exists():
    filename = request.args.get("filename", "").strip()
    if not filename or ".." in filename or "/" in filename or "\\" in filename:
        return jsonify({"exists": False})
    if not filename.lower().endswith(".json"):
        filename += ".json"
    filepath = ROUTERS_DIR / filename
    return jsonify({"exists": filepath.is_file()})


@app.route("/api/routers/save", methods=["POST"])
def routers_save():
    data = request.get_json() or {}
    routers = data.get("routers", [])
    filename = (data.get("filename") or "routers.json").strip()
    if not filename.lower().endswith(".json"):
        filename += ".json"
    if not routers:
        return jsonify({"error": "No router data"}), 400
    normalized = [_normalize_router(r) for r in routers]
    routers_data.clear()
    routers_data.extend(normalized)
    filepath = ROUTERS_DIR / filename
    ensure_folders()
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump({"routers": normalized}, f, indent=2)
    return jsonify({"saved": str(filepath)})


@app.route("/api/routers/update", methods=["POST"])
def routers_update():
    """Update in-memory routers from editor."""
    data = request.get_json() or {}
    routers = data.get("routers", [])
    if not routers:
        return jsonify({"error": "No router data"}), 400
    normalized = [_normalize_router(r) for r in routers]
    routers_data.clear()
    routers_data.extend(normalized)
    return jsonify({"ok": True})


@app.route("/api/files/<deploy_type>")
def list_files(deploy_type):
    if deploy_type not in DEPLOYMENT_TYPES:
        return jsonify({"error": "Invalid type"}), 400
    ensure_folders()
    folder = DEPLOYMENT_TYPES[deploy_type]["folder"]
    ext = DEPLOYMENT_TYPES[deploy_type]["ext"]
    want_tar_gz = ext == ".tar.gz"
    files = []
    for f in (APP_ROOT / folder).iterdir():
        if not f.is_file():
            continue
        name_lower = f.name.lower()
        # Only include .tar.gz for sdk_apps; exclude from all other deployment types
        if name_lower.endswith(".tar.gz"):
            if want_tar_gz:
                files.append({"name": f.name, "path": str(f)})
            continue
        if not ext:
            files.append({"name": f.name, "path": str(f)})
        elif f.suffix.lower() == ext.lower():
            files.append({"name": f.name, "path": str(f)})
    return jsonify({"files": files})


@app.route("/api/files/<deploy_type>/delete", methods=["POST"])
def delete_file(deploy_type):
    """Delete a file from the deployment folder. Accepts {"filename": "..."}."""
    if deploy_type not in DEPLOYMENT_TYPES:
        return jsonify({"error": "Invalid type"}), 400
    data = request.get_json() or {}
    filename = (data.get("filename") or "").strip()
    if not filename or "/" in filename or "\\" in filename:
        return jsonify({"error": "Invalid filename"}), 400
    folder = DEPLOYMENT_TYPES[deploy_type]["folder"]
    path = APP_ROOT / folder / filename
    if not path.exists() or not path.is_file():
        return jsonify({"error": "File not found"}), 404
    try:
        path.unlink()
        return jsonify({"ok": True, "name": filename})
    except OSError as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/files/<deploy_type>/upload", methods=["POST"])
def upload_file(deploy_type):
    if deploy_type not in DEPLOYMENT_TYPES:
        return jsonify({"error": "Invalid type"}), 400
    if "file" not in request.files:
        return jsonify({"error": "No file"}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "No filename"}), 400
    ensure_folders()
    folder = DEPLOYMENT_TYPES[deploy_type]["folder"]
    path = APP_ROOT / folder / f.filename
    f.save(path)
    return jsonify({"name": f.filename, "path": str(path)})


def _get_ncos_headers():
    """Get NCOS ECM API headers from env vars first, then config file. Returns (headers_dict, source)."""
    env_keys = (
        os.environ.get("X_CP_API_ID"),
        os.environ.get("X_CP_API_KEY"),
        os.environ.get("X_ECM_API_ID"),
        os.environ.get("X_ECM_API_KEY"),
    )
    if all(env_keys):
        return {
            "X-CP-API-ID": env_keys[0],
            "X-CP-API-KEY": env_keys[1],
            "X-ECM-API-ID": env_keys[2],
            "X-ECM-API-KEY": env_keys[3],
        }, "env"
    if not API_KEYS_FILE.exists():
        return None, None
    try:
        with open(API_KEYS_FILE, encoding="utf-8") as f:
            cfg = json.load(f)
        h = {
            "X-CP-API-ID": cfg.get("X-CP-API-ID", ""),
            "X-CP-API-KEY": cfg.get("X-CP-API-KEY", ""),
            "X-ECM-API-ID": cfg.get("X-ECM-API-ID", ""),
            "X-ECM-API-KEY": cfg.get("X-ECM-API-KEY", ""),
        }
        if not all(h.values()):
            return None, None
        return h, "file"
    except (json.JSONDecodeError, IOError):
        return None, None


@app.route("/api/ncos/config", methods=["GET", "POST"])
def ncos_config():
    """Get or save NCOS API keys"""
    if request.method == "GET":
        h, source = _get_ncos_headers()
        if not h:
            return jsonify({"configured": False})
        resp = {"configured": True, "source": source or "file"}
        if source == "env":
            masked = "\u2022" * 8
            resp.update({
                "X-CP-API-ID": masked,
                "X-CP-API-KEY": masked,
                "X-ECM-API-ID": masked,
                "X-ECM-API-KEY": masked,
            })
        else:
            resp.update({
                "X-CP-API-ID": h.get("X-CP-API-ID", ""),
                "X-CP-API-KEY": h.get("X-CP-API-KEY", ""),
                "X-ECM-API-ID": h.get("X-ECM-API-ID", ""),
                "X-ECM-API-KEY": h.get("X-ECM-API-KEY", ""),
            })
        return jsonify(resp)
    data = request.get_json() or {}
    cfg = {
        "X-CP-API-ID": data.get("X-CP-API-ID", ""),
        "X-CP-API-KEY": data.get("X-CP-API-KEY", ""),
        "X-ECM-API-ID": data.get("X-ECM-API-ID", ""),
        "X-ECM-API-KEY": data.get("X-ECM-API-KEY", ""),
    }
    with open(API_KEYS_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    return jsonify({"ok": True})


@app.route("/api/ncos/firmwares")
def ncos_firmwares():
    """List available NCOS images for a version and model"""
    version = request.args.get("version", "").strip()
    model = request.args.get("model", "").strip()
    if not version or not model:
        return jsonify({"error": "version and model required"}), 400
    headers, _ = _get_ncos_headers()
    if not headers or not all(headers.values()):
        return jsonify({"error": "NCOS API keys not configured"}), 400
    try:
        url = NCOS_DEVICE_URL + version
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json().get("data", [])
        model_upper = model.upper()
        matches = [i for i in data if model_upper in (i.get("url") or "").upper()]
        return jsonify({"firmwares": [{"url": m["url"], "id": i} for i, m in enumerate(matches)]})
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ncos/download", methods=["POST"])
def ncos_download():
    """Download NCOS image from Cradlepoint ECM and save to NCOS folder"""
    data = request.get_json() or {}
    version = data.get("version", "").strip()
    model = data.get("model", "").strip()
    url_path = data.get("url", "").strip()
    if not version or not model or not url_path:
        return jsonify({"error": "version, model, and url required"}), 400
    headers, _ = _get_ncos_headers()
    if not headers or not all(headers.values()):
        return jsonify({"error": "NCOS API keys not configured"}), 400
    try:
        firmware_url = NCOS_FIRMWARE_BASE + url_path
        r = requests.get(firmware_url, headers=headers, timeout=300)
        r.raise_for_status()
        filename = f"{model}-{version}.bin"
        ensure_folders()
        out_path = APP_ROOT / "NCOS" / filename
        with open(out_path, "wb") as f:
            f.write(r.content)
        return jsonify({"ok": True, "name": filename, "path": str(out_path)})
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500


def _ping_target(ip, count=5):
    """Ping an IP and return dict with status, tx, rx, loss_pct, min_ms, avg_ms, max_ms."""
    result = {
        "ip": ip,
        "status": "Offline",
        "tx": count,
        "rx": 0,
        "loss_pct": 100.0,
        "min_ms": "",
        "avg_ms": "",
        "max_ms": "",
    }
    try:
        cmd = ["ping", "-c", str(count), "-W", "3", str(ip)]
        if sys.platform == "win32":
            cmd = ["ping", "-n", str(count), "-w", "3000", str(ip)]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=count + 5)
        out = (proc.stdout or "") + (proc.stderr or "")
        # Packets transmitted, received
        tx_rx = re.search(r"(\d+)\s+packets?\s+transmitted,\s*(\d+)\s+packets?\s+received", out, re.I)
        if tx_rx:
            result["tx"] = int(tx_rx.group(1))
            result["rx"] = int(tx_rx.group(2))
        # Loss %
        loss_m = re.search(r"([\d.]+)%\s+packet\s+loss", out, re.I)
        if loss_m:
            result["loss_pct"] = float(loss_m.group(1))
        # Round-trip min/avg/max (macOS/Linux)
        rtt_m = re.search(r"round-trip min/avg/max[^=]*=\s*([\d.]+)/([\d.]+)/([\d.]+)", out)
        if rtt_m:
            result["min_ms"] = rtt_m.group(1)
            result["avg_ms"] = rtt_m.group(2)
            result["max_ms"] = rtt_m.group(3)
        else:
            # Windows format
            rtt_w = re.search(r"Minimum\s*=\s*(\d+)ms.*Maximum\s*=\s*(\d+)ms.*Average\s*=\s*(\d+)ms", out, re.S)
            if rtt_w:
                result["min_ms"] = rtt_w.group(1)
                result["avg_ms"] = rtt_w.group(3)
                result["max_ms"] = rtt_w.group(2)
        if result["rx"] > 0:
            result["status"] = "Online"
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
        pass
    return result


@app.route("/api/monitoring/ping", methods=["POST"])
def api_ping():
    """Ping all targets in parallel. Returns list of results. Updates state in routers_data."""
    data = request.get_json() or {}
    targets = data.get("targets", [])
    if not targets:
        return jsonify({"error": "No targets provided"}), 400
    count = int(data.get("count", 5))
    max_workers = min(32, max(4, len(targets)))
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_ping_target, t, count): t for t in targets}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception:
                results.append({
                    "ip": futures[future], "status": "Offline", "tx": count, "rx": 0,
                    "loss_pct": 100.0, "min_ms": "", "avg_ms": "", "max_ms": "",
                })
    # Preserve targets order
    by_ip = {r["ip"]: r for r in results}
    ordered = [by_ip.get(t, {"ip": t, "status": "Offline", "tx": count, "rx": 0, "loss_pct": 100.0, "min_ms": "", "avg_ms": "", "max_ms": ""}) for t in targets]
    # Update state in routers_data for each matching router
    for r in routers_data:
        ip = str(r.get("ip_address", "")).strip()
        if ":" in ip:
            ip = ip.split(":")[0]
        if ip and ip in by_ip:
            r["state"] = by_ip[ip]["status"]
    # Persist updated state to file so it survives refresh
    if routers_data:
        filename = "routers.json"
        if APP_CONFIG_FILE.exists():
            try:
                with open(APP_CONFIG_FILE, encoding="utf-8") as f:
                    cfg = json.load(f)
                if cfg.get("last_file"):
                    filename = cfg["last_file"]
            except (json.JSONDecodeError, IOError):
                pass
        if not filename.lower().endswith(".json"):
            filename += ".json"
        filepath = ROUTERS_DIR / filename
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump({"routers": [_normalize_router(r) for r in routers_data]}, f, indent=2)
        except IOError:
            pass
    return jsonify({"results": ordered})


@app.route("/api/monitoring/ping/offline-log", methods=["POST"])
def api_ping_offline_log():
    """Log offline/online events for ping monitoring. Creates or updates log file in logs/ folder."""
    data = request.get_json() or {}
    event = data.get("event")  # "offline" or "online"
    ip = (data.get("ip") or "").strip()
    hostname = (data.get("hostname") or "-").strip().replace("|", "-")[:32]
    filename = (data.get("filename") or "Offline Events.log").strip()
    if not event or event not in ("offline", "online") or not ip:
        return jsonify({"error": "Invalid request"}), 400
    if ".." in filename or "/" in filename or "\\" in filename:
        return jsonify({"error": "Invalid filename"}), 400
    if not filename.lower().endswith(".log"):
        filename += ".log"
    logs_dir = APP_ROOT / "logs"
    ensure_folders()
    filepath = logs_dir / filename
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        if event == "offline":
            header = (
                "# Offline Time      | Online Time         | IP Address      | Hostname\n"
                "# ----------------- | ------------------- | --------------- | -----------------\n"
            )
            if not filepath.exists():
                filepath.write_text(header, encoding="utf-8")
            line = f"{now:19} | {'OFFLINE':19} | {ip:15} | {hostname:20}\n"
            with open(filepath, "a", encoding="utf-8") as f:
                f.write(line)
        else:  # online
            if not filepath.exists():
                return jsonify({"ok": True})
            content = filepath.read_text(encoding="utf-8")
            lines = content.split("\n")
            found_idx = -1
            for i in range(len(lines) - 1, -1, -1):
                line = lines[i]
                if line.startswith("#") or not line.strip():
                    continue
                parts = [p.strip() for p in line.split("|")]
                if len(parts) >= 3 and parts[1] == "OFFLINE" and parts[2] == ip:
                    found_idx = i
                    break
            if found_idx >= 0:
                parts = [p.strip() for p in lines[found_idx].split("|")]
                if len(parts) >= 3:
                    p0, _, p2 = parts[0], parts[1], parts[2]
                    p3 = parts[3] if len(parts) > 3 else "-"
                    lines[found_idx] = f"{p0:19} | {now:19} | {p2:15} | {p3:20}"
                    filepath.write_text("\n".join(lines), encoding="utf-8")
        return jsonify({"ok": True})
    except IOError as e:
        return jsonify({"error": str(e)}), 500


def _format_connection_error(e):
    """Return a short, user-friendly message for connection/request errors."""
    err_str = str(e).lower()
    if hasattr(requests.exceptions, "ConnectTimeout") and isinstance(e, requests.exceptions.ConnectTimeout):
        return "Connection timed out"
    if hasattr(requests.exceptions, "ReadTimeout") and isinstance(e, requests.exceptions.ReadTimeout):
        return "Read timed out"
    if hasattr(requests.exceptions, "ConnectionError") and isinstance(e, requests.exceptions.ConnectionError):
        if "connection refused" in err_str or "connectionrefused" in err_str:
            return "Connection refused"
        if "timed out" in err_str or "timeout" in err_str or "max retries" in err_str:
            return "Connection timed out"
        if "name or service not known" in err_str or "nodename nor servname" in err_str:
            return "Host not found"
        if "no route to host" in err_str or "network is unreachable" in err_str:
            return "Host unreachable"
    # Fallback: truncate and clean common patterns
    s = str(e)
    if "max retries exceeded" in s.lower():
        return "Connection timed out"
    if "connection refused" in s.lower():
        return "Connection refused"
    if len(s) > 80:
        return s[:80] + "..."
    return s


def _keyword_matches(key_lower, kw):
    """Check if key matches keyword pattern. *x=ends with x, x*=starts with x, *x*=contains x, x=exact match (no wildcard)."""
    if not kw:
        return False
    start_wild = kw.startswith("*")
    end_wild = kw.endswith("*")
    inner = kw.strip("*").lower()
    if not inner:
        return True  # bare * matches all
    if start_wild and end_wild:
        return inner in key_lower
    if start_wild:
        return key_lower.endswith(inner)
    if end_wild:
        return key_lower.startswith(inner)
    return key_lower == inner  # no wildcard = exact match


def _segment_matches_key(seg, key_str):
    """Check if path segment (e.g. * or mdm*) matches a dict key or list index."""
    key_lower = key_str.lower() if isinstance(key_str, str) else str(key_str).lower()
    if seg == "*":
        return True
    if "*" in seg:
        return _keyword_matches(key_lower, seg)
    return key_str == seg or (isinstance(key_str, str) and key_str.lower() == seg.lower())


def _get_item_identifier(item):
    """For list items (e.g. devices), get id/uid/name for pattern matching."""
    if isinstance(item, dict):
        for k in ("id", "uid", "name", "_id", "device_id"):
            if k in item and item[k] is not None:
                return str(item[k])
    return None


def _expand_path_wildcard(obj, base_path, segments):
    """Post-call: expand * or pattern (e.g. mdm*) in path by traversing JSON. Returns [(full_path, value), ...].
    e.g. status/devices/*/signal -> each child's signal; status/devices/mdm*/diagnostics -> keys matching mdm*."""
    if not segments:
        return [(base_path, obj)]

    seg, rest = segments[0], segments[1:]
    is_wild = seg == "*" or "*" in seg
    if is_wild:
        results = []
        if isinstance(obj, list):
            for i, v in enumerate(obj):
                # Match by index (for *) or by item id/uid/name (for mdm* etc)
                key_to_match = _get_item_identifier(v) if "*" in seg and seg != "*" else None
                match_str = key_to_match if key_to_match is not None else str(i)
                if _segment_matches_key(seg, match_str):
                    sub_path = f"{base_path}/{key_to_match if key_to_match is not None else i}"
                    results.extend(_expand_path_wildcard(v, sub_path, rest))
        elif isinstance(obj, dict):
            for k, v in obj.items():
                if _segment_matches_key(seg, str(k)):
                    sub_path = f"{base_path}/{k}"
                    results.extend(_expand_path_wildcard(v, sub_path, rest))
        return results
    else:
        if isinstance(obj, dict) and seg in obj:
            sub_path = f"{base_path}/{seg}"
            return _expand_path_wildcard(obj[seg], sub_path, rest)
        if isinstance(obj, dict):
            # Try case-insensitive key match
            for k, v in obj.items():
                if str(k).lower() == seg.lower():
                    sub_path = f"{base_path}/{k}"
                    return _expand_path_wildcard(v, sub_path, rest)
        if isinstance(obj, list) and seg.isdigit():
            i = int(seg)
            if 0 <= i < len(obj):
                sub_path = f"{base_path}/{seg}"
                return _expand_path_wildcard(obj[i], sub_path, rest)
        return []


def _call_router_api(ip, port, username, password, method, path, payload=None):
    """Call router API at /api/{path}. Returns (success, data_or_error).
    Cradlepoint API expects application/x-www-form-urlencoded with body: data=<urlencoded-json>"""
    base = f"http://{ip}:{port}" if ":" not in str(ip) else f"http://{ip}"
    url = f"{base}/api/{path.lstrip('/')}"
    auth = (username, password)
    try:
        if method == "GET":
            r = requests.get(url, auth=auth, verify=False, timeout=15)
        elif method in ("PUT", "POST"):
            # Cradlepoint expects form-urlencoded with data=json (see test.har)
            payload_data = payload if payload is not None else {}
            data_str = json.dumps(payload_data)
            body = {"data": data_str}
            headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
            if method == "PUT":
                r = requests.put(url, data=body, auth=auth, verify=False, timeout=15, headers=headers)
            else:
                r = requests.post(url, data=body, auth=auth, verify=False, timeout=15, headers=headers)
        elif method == "DELETE":
            r = requests.delete(url, auth=auth, verify=False, timeout=15)
        else:
            return False, "Invalid method"
        if r.status_code >= 300:
            return False, f"{r.status_code}: {r.text[:200] if r.text else 'No response'}"
        try:
            data = r.json()
        except Exception:
            return True, r.text or "(empty)"
        # Check for error in response body (router may return 200 with error payload)
        if isinstance(data, dict):
            if data.get("success") is False:
                return False, data.get("error") or data.get("message") or str(data)[:200]
            if "error" in data and data["error"]:
                return False, str(data["error"])[:200]
        # For GET: extract "data" key for cleaner results
        if method == "GET" and isinstance(data, dict) and "data" in data:
            return True, data["data"]
        return True, data
    except Exception as e:
        return False, _format_connection_error(e)


@app.route("/api/monitoring/remote-api", methods=["POST"])
def api_remote_api():
    """Call router API endpoints. Returns results per router."""
    data = request.get_json() or {}
    method = (data.get("method") or "GET").upper()
    if method not in ("GET", "PUT", "POST", "DELETE"):
        return jsonify({"error": "Invalid method"}), 400

    if not routers_data:
        return jsonify({"error": "No router data. Load a routers file first."}), 400

    if method == "GET":
        paths_raw = data.get("paths", "")
        if not paths_raw:
            return jsonify({"error": "Enter at least one path for GET"}), 400
        paths = [p.strip() for p in re.split(r"[\s,\n]+", paths_raw) if p.strip()]
        if not paths:
            return jsonify({"error": "Enter at least one path for GET"}), 400
    else:
        path = (data.get("path") or "").strip()
        if not path:
            return jsonify({"error": "Enter path"}), 400
        paths = [path]
        payload = None
        try:
            payload_str = (data.get("payload") or "").strip()
            if payload_str:
                payload = json.loads(payload_str)
        except json.JSONDecodeError:
            return jsonify({"error": "Invalid JSON payload"}), 400

    raw_indices = data.get("indices", [])
    if not raw_indices:
        return jsonify({"error": "Select one or more routers."}), 400
    indices = []
    for x in raw_indices:
        try:
            i = int(x)
            if 0 <= i < len(routers_data):
                indices.append(i)
        except (TypeError, ValueError):
            pass
    if not indices:
        return jsonify({"error": "No valid router selection."}), 400

    targets = []
    for i in indices:
        r = routers_data[i]
        ip = str(r.get("ip_address", "")).strip()
        if not ip:
            continue
        if ":" in ip:
            ip = ip.split(":")[0]
        username = str(r.get("username", "")).strip() or "admin"
        password = str(r.get("password", "")).strip()
        if not password:
            continue
        port = int(r.get("port", 8080)) if r.get("port") else 8080
        hostname = str(r.get("hostname", "")).strip()
        targets.append({"ip": ip, "port": port, "username": username, "password": password, "hostname": hostname})

    if not targets:
        return jsonify({"error": "No valid targets with credentials"}), 400

    def fetch_one(idx, t):
        if method == "GET":
            row_data = {"ip": t["ip"], "hostname": t["hostname"] or "-"}
            for p in paths:
                has_wildcard = "*" in p
                api_path = p  # default: use path as-is
                if has_wildcard:
                    path_segments = p.split("/")
                    star_idx = next((i for i, s in enumerate(path_segments) if "*" in s), None)
                    if star_idx is not None and star_idx > 0:
                        base_api_path = "/".join(path_segments[:star_idx])
                        remainder = path_segments[star_idx:]  # ["*", "signal"] or ["mdm*", "diagnostics"]
                        api_path = base_api_path
                    else:
                        if star_idx == 0:
                            row_data[p] = "Failed: path must have segment before *"
                            continue
                        has_wildcard = False

                success, data_or_err = _call_router_api(t["ip"], t["port"], t["username"], t["password"], "GET", api_path)
                if success and isinstance(data_or_err, (dict, list)) and has_wildcard:
                    expanded = _expand_path_wildcard(data_or_err, api_path, remainder)
                    for full_path, val in expanded:
                        row_data[full_path] = val if val is None or isinstance(val, (str, int, float, bool)) else json.dumps(val)
                elif success:
                    if isinstance(data_or_err, (dict, list)):
                        row_data[p] = json.dumps(data_or_err) if data_or_err else ""
                    else:
                        row_data[p] = str(data_or_err) if data_or_err is not None else ""
                else:
                    row_data[p] = f"Failed: {data_or_err}"
            return idx, row_data
        else:
            success, data_or_err = _call_router_api(t["ip"], t["port"], t["username"], t["password"], method, paths[0], payload)
            return idx, {
                "ip": t["ip"],
                "hostname": t["hostname"] or "-",
                "result": "Success" if success else f"Failed: {data_or_err}"
            }

    max_workers = min(16, max(4, len(targets)))
    results_by_idx = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_one, i, t): i for i, t in enumerate(targets)}
        for future in as_completed(futures):
            idx, row_data = future.result()
            results_by_idx[idx] = row_data
    results = [results_by_idx[i] for i in range(len(targets))]

    path_cols = paths if method == "GET" else []
    non_wildcard_paths = [p for p in path_cols if "*" not in p]
    base_cols = ["ip", "hostname"] + non_wildcard_paths
    exclude_from_extra = set(non_wildcard_paths)
    if method == "GET" and results:
        extra_cols = sorted(set(k for r in results for k in r if k not in ("ip", "hostname") and k not in exclude_from_extra))
        columns = base_cols + extra_cols
    else:
        columns = base_cols if method == "GET" else ["ip", "hostname", "result"]

    return jsonify({
        "method": method,
        "paths": paths,
        "results": results,
        "columns": columns
    })


@app.route("/api/monitoring/remote-api/save", methods=["POST"])
def api_remote_api_save():
    """Save remote API results to a CSV file in the logs folder."""
    data = request.get_json() or {}
    headers = data.get("headers", [])
    rows = data.get("rows", [])
    filename = (data.get("filename") or "remote_api_results.csv").strip()
    if not filename.lower().endswith(".csv"):
        filename += ".csv"
    if not headers:
        return jsonify({"error": "No headers"}), 400
    logs_dir = APP_ROOT / "logs"
    ensure_folders()
    filepath = logs_dir / filename
    try:
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(headers)
            w.writerows(rows)
        return jsonify({"ok": True, "saved": filename})
    except IOError as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/monitoring/save-config", methods=["POST"])
def api_save_config():
    """GET config from selected routers and save to configs folder."""
    data = request.get_json() or {}
    indices = data.get("indices", [])

    if not routers_data or not indices:
        return jsonify({"error": "No router data or selection"}), 400

    configs_dir = APP_ROOT / "configs"
    ensure_folders()
    today = datetime.now()
    date_str = f"{today.month}-{today.day}-{today.year}"

    def _sanitize(s):
        return re.sub(r"[^\w\-.]", "_", str(s or "").strip())[:50] or "unknown"

    def save_one(idx):
        if idx >= len(routers_data):
            return idx, "-", "-", None, "Invalid index"
        r = routers_data[idx]
        ip = str(r.get("ip_address", "")).strip()
        if not ip:
            return idx, "-", str(r.get("hostname", "")).strip() or "-", None, "No IP"
        if ":" in ip:
            ip = ip.split(":")[0]
        port = int(r.get("port", 8080)) if r.get("port") else 8080
        username = str(r.get("username", "")).strip() or "admin"
        password = str(r.get("password", "")).strip()
        if not password:
            hostname = str(r.get("hostname", "")).strip() or "-"
            return idx, ip, hostname, None, "Missing credentials"
        hostname = str(r.get("hostname", "")).strip()
        hostname_safe = _sanitize(hostname) if hostname else _sanitize(ip)
        base = f"http://{ip}:{port}" if ":" not in str(ip) else f"http://{ip}"
        auth = (username, password)
        try:
            r_info = requests.get(f"{base}/api/status/product_info", auth=auth, verify=False, timeout=10)
            product_name = "unknown"
            if r_info.status_code < 300:
                info = r_info.json().get("data", {})
                product_name = _sanitize(str(info.get("product_name") or "unknown").split("-")[0])
            r_cfg = requests.get(f"{base}/config_save", auth=auth, verify=False, timeout=60, stream=True)
            if r_cfg.status_code >= 300:
                return idx, ip, hostname or "-", None, f"HTTP {r_cfg.status_code}"
            content = r_cfg.content
            if not content:
                return idx, ip, hostname or "-", None, "Empty response"
            base_name = f"{hostname_safe}_config_{product_name}_{date_str}.bin"
            filepath = configs_dir / base_name
            n = 0
            while filepath.exists():
                n += 1
                stem = base_name[:-4] if base_name.endswith(".bin") else base_name
                filepath = configs_dir / f"{stem} ({n}).bin"
            filepath.write_bytes(content)
            return idx, ip, hostname or "-", filepath.name, None
        except Exception as e:
            return idx, ip, hostname or "-", None, str(e)

    max_workers = min(16, max(4, len(indices)))
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(save_one, i): i for i in indices}
        for future in as_completed(futures):
            idx, ip, hostname, filename, err = future.result()
            results.append({
                "idx": idx,
                "ip": ip,
                "hostname": hostname,
                "success": filename is not None,
                "filename": filename,
                "error": err,
            })

    results.sort(key=lambda r: indices.index(r["idx"]) if r["idx"] in indices else 999)

    return jsonify({"results": results, "ok": True})


@app.route("/api/logs/exists")
def logs_exists():
    """Check if a file exists in the logs folder"""
    filename = request.args.get("filename", "").strip()
    if not filename or ".." in filename or "/" in filename or "\\" in filename:
        return jsonify({"exists": False})
    lower = filename.lower()
    if not (lower.endswith(".log") or lower.endswith(".csv")):
        return jsonify({"exists": False})
    logs_dir = (APP_ROOT / "logs").resolve()
    filepath = (logs_dir / filename).resolve()
    try:
        filepath.relative_to(logs_dir)
    except ValueError:
        return jsonify({"exists": False})
    return jsonify({"exists": filepath.is_file()})


@app.route("/api/logs/list")
def logs_list():
    """List log and CSV files in the logs folder"""
    ensure_folders()
    logs_dir = APP_ROOT / "logs"
    files = []
    if logs_dir.exists():
        for f in sorted(logs_dir.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
            if f.is_file() and f.suffix.lower() in (".log", ".csv"):
                files.append({"name": f.name})
    return jsonify({"files": files})


def _safe_log_path(filename):
    """Validate filename and return path. Returns None if invalid (path traversal, not in logs, or not .log/.csv)."""
    if not filename or ".." in filename or "/" in filename or "\\" in filename:
        return None
    lower = filename.lower()
    if not (lower.endswith(".log") or lower.endswith(".csv")):
        return None
    logs_dir = (APP_ROOT / "logs").resolve()
    path = (logs_dir / filename).resolve()
    if not path.is_file():
        return None
    try:
        path.relative_to(logs_dir)
    except ValueError:
        return None
    return path


@app.route("/api/logs/read")
def logs_read():
    """Read log file content as plain text"""
    filename = request.args.get("filename", "").strip()
    path = _safe_log_path(filename)
    if not path:
        return jsonify({"error": "File not found or invalid"}), 404
    try:
        content = path.read_text(encoding="utf-8", errors="replace")
        return jsonify({"content": content})
    except IOError as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/logs/download/<path:filename>")
def logs_download(filename):
    """Download a log or CSV file"""
    path = _safe_log_path(filename)
    if not path:
        return jsonify({"error": "File not found or invalid"}), 404
    mimetype = "text/csv" if path.suffix.lower() == ".csv" else "text/plain"
    try:
        return send_file(path, mimetype=mimetype, as_attachment=True, download_name=path.name)
    except IOError as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/logs/delete", methods=["POST"])
def logs_delete():
    """Delete a log file"""
    data = request.get_json() or {}
    filename = (data.get("filename") or "").strip()
    path = _safe_log_path(filename)
    if not path:
        return jsonify({"error": "File not found or invalid"}), 404
    try:
        path.unlink()
        return jsonify({"ok": True})
    except IOError as e:
        return jsonify({"error": str(e)}), 500


def _valid_log_filename(name):
    """Check filename is safe for logs folder: no path traversal, ends with .log or .csv."""
    if not name or ".." in name or "/" in name or "\\" in name:
        return False
    lower = name.strip().lower()
    return lower.endswith(".log") or lower.endswith(".csv")


@app.route("/api/logs/rename", methods=["POST"])
def logs_rename():
    """Rename a file in the logs folder."""
    data = request.get_json() or {}
    old_name = (data.get("filename") or data.get("old_filename") or "").strip()
    new_name = (data.get("new_filename") or "").strip()
    path = _safe_log_path(old_name)
    if not path:
        return jsonify({"error": "File not found or invalid"}), 404
    if not _valid_log_filename(new_name):
        return jsonify({"error": "Invalid filename. Must end with .log or .csv"}), 400
    logs_dir = (APP_ROOT / "logs").resolve()
    new_path = (logs_dir / new_name).resolve()
    try:
        new_path.relative_to(logs_dir)
    except ValueError:
        return jsonify({"error": "Invalid filename"}), 400
    if new_path.exists():
        return jsonify({"error": "A file with that name already exists"}), 400
    try:
        path.rename(new_path)
        return jsonify({"ok": True, "filename": new_name})
    except IOError as e:
        return jsonify({"error": str(e)}), 500


def _fetch_router_info(ip, port, username, password, timeout=10, retries=0):
    """Fetch Hostname, MAC, Serial, Product Name, NCOS from router API. Returns dict.
    Handles both wrapped {"data": {...}} and direct response formats.
    retries: number of retries on failure (0 = 1 attempt, 1 = 2 attempts, etc.)
    """
    result = {"hostname": "", "mac_address": "", "serial_number": "", "product_name": "", "ncos": ""}
    base = f"http://{ip}:{port}" if ":" not in str(ip) else f"http://{ip}"
    auth = (username, password)

    def _unwrap(j):
        """Extract data from API response - handles both wrapped and direct formats."""
        if not isinstance(j, dict):
            return {}
        data = j.get("data")
        if data is not None and isinstance(data, dict):
            return data
        return j

    def do_fetch():
        try:
            r1 = requests.get(f"{base}/api/status/product_info", auth=auth, verify=False, timeout=timeout)
            if r1.status_code >= 300:
                return None
            j = r1.json()
            info = _unwrap(j)
            return info if isinstance(info, dict) else {}
        except Exception:
            return None

    info = None
    for attempt in range(retries + 1):
        info = do_fetch()
        if info is not None:
            break

    if info is None:
        return result

    try:
        product_name = str(info.get("product_name") or "").strip()
        mac_raw = str(info.get("mac0") or info.get("mac") or "").strip().lower().replace(":", "").replace("-", "")
        manufacturing = info.get("manufacturing") or {}
        if isinstance(manufacturing, dict):
            serial_num = str(manufacturing.get("serial_num") or manufacturing.get("serial_number") or "").strip()
        else:
            serial_num = ""
        serial_num = serial_num or str(info.get("serial_num") or info.get("serial_number") or "").strip()
        result["product_name"] = product_name
        result["mac_address"] = mac_raw.upper() if mac_raw else ""
        result["serial_number"] = serial_num

        system_id = ""
        try:
            r2 = requests.get(f"{base}/api/config/system/system_id", auth=auth, verify=False, timeout=timeout)
            if r2.status_code < 300:
                d = r2.json()
                sid = d.get("data", "") if isinstance(d, dict) else ""
                system_id = str(sid or "").strip()
        except Exception:
            pass

        fw_info = {}
        try:
            r3 = requests.get(f"{base}/api/status/fw_info", auth=auth, verify=False, timeout=timeout)
            if r3.status_code < 300:
                fw_info = _unwrap(r3.json() or {})
                fw_info = fw_info if isinstance(fw_info, dict) else {}
        except Exception:
            pass
        maj = fw_info.get("major_version", "")
        minv = fw_info.get("minor_version", "")
        patch = fw_info.get("patch_version", "")
        tag = fw_info.get("fw_release_tag", "")
        bdate = fw_info.get("build_date", "")
        result["ncos"] = f"{maj}.{minv}.{patch} {tag} {bdate}".strip()

        if system_id:
            result["hostname"] = system_id
        else:
            prefix = product_name.split("-")[0].strip() if product_name else ""
            mac_suffix = (mac_raw[-3:] if len(mac_raw) >= 3 else mac_raw).upper()
            result["hostname"] = f"{prefix}-{mac_suffix}" if prefix or mac_suffix else ""
        return result
    except Exception:
        return result


@app.route("/api/discover-routers", methods=["POST"])
def discover_routers():
    """Discover routers by IP subnet/range. Uses connection_timeout/retries from app config."""
    data = request.get_json() or {}
    ip_spec = (data.get("ip_range") or data.get("ip_subnet") or "").strip()
    username = (data.get("username") or "admin").strip() or "admin"
    password = (data.get("password") or "").strip()
    port = int(data.get("port", 8080)) if data.get("port") else 8080
    cfg = _load_app_config()
    timeout = cfg.get("connection_timeout", 2)
    retries = cfg.get("connection_retries", 1)

    if not ip_spec:
        return jsonify({"error": "Enter IP subnet or range (e.g. 192.168.1.0/24 or 192.168.1.1-10)"}), 400
    if not password:
        return jsonify({"error": "Password required"}), 400

    ips = _expand_ip_range(ip_spec)
    if not ips:
        return jsonify({"error": "No valid IP addresses from range"}), 400
    if len(ips) > 512:
        return jsonify({"error": "Too many IPs (max 512). Use a smaller range."}), 400

    def fetch_one(ip):
        info = _fetch_router_info(ip, port, username, password, timeout=timeout, retries=retries)
        return ip, info

    max_workers = min(16, max(4, len(ips)))
    discovered = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_one, ip): ip for ip in ips}
        for future in as_completed(futures):
            try:
                ip, info = future.result()
                r = _empty_router()
                r["ip_address"] = ip
                r["state"] = "Online"  # discovered = successfully connected
                r["created_at"] = datetime.now().isoformat()
                r["username"] = username
                r["password"] = password
                r["port"] = port
                r["hostname"] = info.get("hostname", "")
                r["mac"] = info.get("mac_address", "")
                r["serial_number"] = info.get("serial_number", "")
                r["product_name"] = info.get("product_name", "")
                r["ncos_version"] = info.get("ncos", "")
                discovered.append((ip, r, any([info.get("hostname"), info.get("mac_address"), info.get("product_name")])))
            except Exception:
                ip = futures[future]
                discovered.append((ip, None, False))

    # Write log
    log_filename = f"discover_routers_{datetime.now().strftime('%Y-%m-%d_%H.%M.%S')}.log"
    logs_dir = APP_ROOT / "logs"
    ensure_folders()
    log_path = logs_dir / log_filename
    log_lines = ["Discover Routers", f"Started: {datetime.now().isoformat()}", ""]
    for ip, r, ok in discovered:
        if ok and r:
            log_lines.append(f"--- {ip} (OK) ---")
            for k in ["hostname", "mac", "serial_number", "product_name", "ncos_version"]:
                v = r.get(k, "")
                if v:
                    log_lines.append(f"  {k}: {v}")
        else:
            log_lines.append(f"--- {ip} (No response) ---")
        log_lines.append("")
    try:
        log_path.write_text("\n".join(log_lines), encoding="utf-8")
    except IOError:
        pass

    new_routers = [_normalize_router(r) for ip, r, ok in discovered if ok and r]
    if not new_routers:
        return jsonify({"error": "No response from any routers. Check IPs, credentials, and network.", "log_file": log_filename}), 400

    # Add or update routers: merge new credentials/info for existing IPs, append for new IPs
    ip_to_idx = {str(x.get("ip_address", "")).strip().split(":")[0]: i for i, x in enumerate(routers_data)}
    for r in new_routers:
        ip = str(r.get("ip_address", "")).strip().split(":")[0]
        if not ip:
            continue
        if ip in ip_to_idx:
            # Update existing router with new credentials and discovered info
            routers_data[ip_to_idx[ip]] = r
        else:
            routers_data.append(r)
            ip_to_idx[ip] = len(routers_data) - 1

    filename = "routers.json"
    if APP_CONFIG_FILE.exists():
        try:
            with open(APP_CONFIG_FILE, encoding="utf-8") as f:
                cfg = json.load(f)
            if cfg.get("last_file"):
                filename = cfg["last_file"]
        except (json.JSONDecodeError, IOError):
            pass
    if not filename.lower().endswith(".json"):
        filename += ".json"
    filepath = ROUTERS_DIR / filename
    ensure_folders()
    # Ensure all routers have username, password, port, created_at before persisting
    routers_to_save = [_normalize_router(r) for r in routers_data]
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump({"routers": routers_to_save}, f, indent=2)

    try:
        cfg = {}
        if APP_CONFIG_FILE.exists():
            with open(APP_CONFIG_FILE, encoding="utf-8") as f:
                cfg = json.load(f)
        cfg["last_file"] = filename
        with open(APP_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
    except (json.JSONDecodeError, IOError):
        pass

    return jsonify({"routers": list(routers_data), "last_file": filename, "log_file": log_filename})


def _collect_api_path_columns(routers):
    """Collect column names that look like API paths (from Remote API Copy to Routers)."""
    paths = set()
    for r in routers:
        for k in r:
            if k not in ROUTER_KEYS and "/" in k and len(k) > 2:
                paths.add(k)
    return list(paths)


@app.route("/api/get-router-info", methods=["POST"])
def get_router_info():
    """Fetch hostname, mac, serial, product_name, ncos_version for selected routers. Uses per-router credentials.
    Also fetches any API path columns that were added via Remote API Copy to Routers."""
    data = request.get_json() or {}
    routers = data.get("routers") or []
    if not routers:
        return jsonify({"error": "No routers provided"}), 400
    cfg = _load_app_config()
    timeout = cfg.get("connection_timeout", 2)
    retries = cfg.get("connection_retries", 1)
    api_paths = _collect_api_path_columns(routers)

    def fetch_one(r):
        ip = str(r.get("ip_address") or "").strip().split(":")[0]
        if not ip:
            return None, r, False
        port = int(r.get("port") or 8080)
        username = str(r.get("username") or "admin").strip() or "admin"
        password = str(r.get("password") or "").strip()
        info = _fetch_router_info(ip, port, username, password, timeout=timeout, retries=retries)
        r["hostname"] = info.get("hostname", "")
        r["mac"] = info.get("mac_address", "")
        r["serial_number"] = info.get("serial_number", "")
        r["product_name"] = info.get("product_name", "")
        r["ncos_version"] = info.get("ncos", "")
        r["state"] = "Online" if any([info.get("hostname"), info.get("mac_address"), info.get("product_name")]) else r.get("state", "")
        for path in api_paths:
            success, data_or_err = _call_router_api(ip, port, username, password, "GET", path)
            if success:
                r[path] = json.dumps(data_or_err) if isinstance(data_or_err, (dict, list)) else (str(data_or_err) if data_or_err is not None else "")
            else:
                r[path] = f"Failed: {data_or_err}"
        return ip, r, any([info.get("hostname"), info.get("mac_address"), info.get("product_name")])

    max_workers = min(16, max(4, len(routers)))
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_one, dict(r)): r for r in routers}
        for future in as_completed(futures):
            try:
                ip, updated, ok = future.result()
                if ip:
                    results.append((ip, updated, ok))
            except Exception:
                orig = futures[future]
                ip = str(orig.get("ip_address") or "").strip().split(":")[0]
                results.append((ip, dict(orig), False))

    # Update routers_data by matching ip_address. Merge poll results and preserve custom columns from client.
    by_ip = {ip: (r, ok) for ip, r, ok in results if ip}
    for rd in routers_data:
        ip = str(rd.get("ip_address", "")).strip().split(":")[0]
        if ip and ip in by_ip:
            upd, _ = by_ip[ip]
            for k, v in upd.items():
                rd[k] = v

    # Persist to file
    if routers_data:
        filename = "routers.json"
        if APP_CONFIG_FILE.exists():
            try:
                with open(APP_CONFIG_FILE, encoding="utf-8") as f:
                    c = json.load(f)
                if c.get("last_file"):
                    filename = c["last_file"]
            except (json.JSONDecodeError, IOError):
                pass
        if not filename.lower().endswith(".json"):
            filename += ".json"
        filepath = ROUTERS_DIR / filename
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump({"routers": [_normalize_router(r) for r in routers_data]}, f, indent=2)
        except IOError:
            pass

    return jsonify({"routers": list(routers_data)})


@app.route("/api/deploy", methods=["POST"])
def deploy():
    data = request.get_json() or {}
    file_path = data.get("file_path")
    deploy_type = data.get("deploy_type")
    ssh_port = int(data.get("ssh_port", 22))

    if not file_path or deploy_type not in DEPLOYMENT_TYPES:
        return jsonify({"error": "Invalid deploy request"}), 400
    path = Path(file_path) if Path(file_path).is_absolute() else APP_ROOT / file_path
    if not path.exists() or not path.is_file():
        return jsonify({"error": "File not found"}), 400
    action = DEPLOYMENT_TYPES[deploy_type]["endpoint"]
    use_scp = deploy_type == "sdk_apps"

    if not routers_data:
        return jsonify({"error": "No router data. Load a routers file first."}), 400

    raw_indices = data.get("indices", [])
    if not raw_indices:
        return jsonify({"error": "Select one or more routers."}), 400
    indices = []
    for x in raw_indices:
        try:
            i = int(x)
            if 0 <= i < len(routers_data):
                indices.append(i)
        except (TypeError, ValueError):
            pass
    if not indices:
        return jsonify({"error": "No valid router selection."}), 400

    deploy_type_labels = {
        "licenses": "License",
        "ncos": "NCOS",
        "configuration": "Configuration",
        "sdk_apps": "SDK App Deployment",
    }
    timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    col_header = f"{timestamp_str} - {deploy_type_labels.get(deploy_type, deploy_type)} - {path.name}"

    if use_scp:
        def deploy_sdk_one(idx, r):
            ip = str(r.get("ip_address", "")).strip()
            if not ip:
                return idx, None, ["--- Skipped (no IP) ---"]
            if ":" in ip:
                ip = ip.split(":")[0]
            # SDK deploy uses SCP; router's "port" is HTTP admin, use ssh_port for SCP
            port = ssh_port
            username = str(r.get("username", "")).strip() or "admin"
            password = str(r.get("password", "")).strip()
            if not password:
                return idx, None, ["--- Skipped (missing credentials) ---"]
            result_lines = []
            header_line = f"--- {username}@{ip}:{port} ---"
            success = push_sdk_app_via_scp(ip, port, username, password, str(path), result_lines)
            return idx, success, [header_line] + result_lines

        max_workers = min(16, max(4, len(indices)))
        results_by_idx = {}
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(deploy_sdk_one, i, routers_data[i]): i for i in indices}
            for future in as_completed(futures):
                idx, success, lines = future.result()
                results_by_idx[idx] = (success, lines)

        log_lines = [f"SDK App Deployment: {path.name}", f"Started: {datetime.now().isoformat()}", ""]
        failure_count = 0
        for i in indices:
            success, lines = results_by_idx.get(i, (False, ["--- Skipped ---"]))
            if success is False:
                failure_count += 1
            log_lines.extend(lines)
            log_lines.append("")

        log_filename = f"sdk_deploy_{datetime.now().strftime('%Y-%m-%d_%H.%M.%S')}.log"
        logs_dir = APP_ROOT / "logs"
        ensure_folders()
        (logs_dir / log_filename).write_text("\n".join(log_lines), encoding="utf-8")
        return jsonify({"ok": True, "log_file": log_filename, "failure_count": failure_count, "routers": list(routers_data)})
    else:
        def deploy_one(idx, r):
            ip = str(r.get("ip_address", "")).strip()
            if not ip:
                return idx, None
            if ":" in ip:
                ip = ip.split(":")[0]
            port = int(r.get("port", 8080)) if r.get("port") else 8080
            username = str(r.get("username", "")).strip() or "admin"
            password = str(r.get("password", "")).strip()
            if not password:
                return idx, "\n".join(["Skipped: missing credentials"])
            result_lines = []
            push_to_router(ip, port, username, password, str(path), action, result_lines)
            return idx, "\n".join(result_lines) if result_lines else ""

        max_workers = min(16, max(4, len(indices)))
        results_by_idx = {}
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(deploy_one, i, routers_data[i]): i for i in indices}
            for future in as_completed(futures):
                idx, result = future.result()
                results_by_idx[idx] = result

        for i in indices:
            r = routers_data[i]
            res = results_by_idx.get(i, "")
            r[col_header] = res

        filename = "routers.json"
        if APP_CONFIG_FILE.exists():
            try:
                with open(APP_CONFIG_FILE, encoding="utf-8") as f:
                    cfg = json.load(f)
                if cfg.get("last_file"):
                    filename = cfg["last_file"]
            except (json.JSONDecodeError, IOError):
                pass
        if not filename.lower().endswith(".json"):
            filename += ".json"
        filepath = ROUTERS_DIR / filename
        ensure_folders()
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump({"routers": [_normalize_router(r) for r in routers_data]}, f, indent=2)

        return jsonify({"ok": True, "routers": list(routers_data)})


if __name__ == "__main__":
    print("Private Router Manager: http://localhost:9000")
    app.run(host="127.0.0.1", port=9000, debug=True)
