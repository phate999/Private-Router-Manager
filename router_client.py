"""Async router HTTP client using aiohttp for concurrent router API calls."""

import asyncio
import json

try:
    import aiohttp
except ImportError:
    aiohttp = None


def _format_async_error(e):
    """Return a short, user-friendly message for connection/request errors."""
    err_str = str(e).lower()
    if "timeout" in err_str or "timed out" in err_str:
        return "Connection timed out"
    if "connection refused" in err_str or "connectionrefused" in err_str:
        return "Connection refused"
    if "name or service not known" in err_str or "nodename nor servname" in err_str:
        return "Host not found"
    if "no route to host" in err_str or "network is unreachable" in err_str:
        return "Host unreachable"
    s = str(e)
    if len(s) > 80:
        return s[:80] + "..."
    return s


async def _call_router_api_async(session, ip, port, username, password, method, path, payload=None, timeout=15, metrics=None):
    """Call router API at /api/{path}. Returns (success, data_or_error).
    Cradlepoint API expects application/x-www-form-urlencoded with body: data=<urlencoded-json>"""
    base = f"http://{ip}:{port}" if ":" not in str(ip) else f"http://{ip}"
    url = f"{base}/api/{path.lstrip('/')}"
    auth = aiohttp.BasicAuth(username, password)
    if metrics:
        metrics.connection_started()
    try:
        status = 0
        text = ""
        if method == "GET":
            async with session.get(url, auth=auth, ssl=False, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                status = resp.status
                text = await resp.text()
        elif method in ("PUT", "POST"):
            payload_data = payload if payload is not None else {}
            data_str = json.dumps(payload_data)
            body = {"data": data_str}
            headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
            if method == "PUT":
                async with session.put(url, data=body, auth=auth, ssl=False, timeout=aiohttp.ClientTimeout(total=timeout), headers=headers) as resp:
                    status = resp.status
                    text = await resp.text()
            else:
                async with session.post(url, data=body, auth=auth, ssl=False, timeout=aiohttp.ClientTimeout(total=timeout), headers=headers) as resp:
                    status = resp.status
                    text = await resp.text()
        elif method == "DELETE":
            async with session.delete(url, auth=auth, ssl=False, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                status = resp.status
                text = await resp.text()
        else:
            if metrics:
                metrics.connection_finished()
                metrics.request_completed(success=False)
            return False, "Invalid method"

        if status >= 300:
            if metrics:
                metrics.connection_finished()
                metrics.request_completed(success=False)
            return False, f"{resp.status}: {text[:200] if text else 'No response'}"

        try:
            data = json.loads(text) if text else {}
        except json.JSONDecodeError:
            if metrics:
                metrics.connection_finished()
                metrics.request_completed(success=True)
            return True, text or "(empty)"

        if isinstance(data, dict):
            if data.get("success") is False:
                if metrics:
                    metrics.connection_finished()
                    metrics.request_completed(success=False)
                return False, data.get("error") or data.get("message") or str(data)[:200]
            if data.get("error"):
                if metrics:
                    metrics.connection_finished()
                    metrics.request_completed(success=False)
                return False, str(data["error"])[:200]

        if method == "GET" and isinstance(data, dict) and "data" in data:
            if metrics:
                metrics.connection_finished()
                metrics.request_completed(success=True)
            return True, data["data"]
        if metrics:
            metrics.connection_finished()
            metrics.request_completed(success=True)
        return True, data
    except asyncio.TimeoutError:
        if metrics:
            metrics.connection_finished()
            metrics.request_completed(success=False, timed_out=True)
        return False, "Connection timed out"
    except Exception as e:
        if metrics:
            metrics.connection_finished()
            is_timeout = "timeout" in str(e).lower() or "timed out" in str(e).lower()
            metrics.request_completed(success=False, timed_out=is_timeout)
        return False, _format_async_error(e)


async def _fetch_router_info_async(session, ip, port, username, password, timeout=10):
    """Fetch hostname, MAC, serial, product_name, NCOS, description, asset_id from router API. Returns dict."""
    result = {"hostname": "", "mac_address": "", "serial_number": "", "product_name": "", "ncos": "", "description": "", "asset_id": ""}
    base = f"http://{ip}:{port}" if ":" not in str(ip) else f"http://{ip}"
    auth = aiohttp.BasicAuth(username, password)
    client_timeout = aiohttp.ClientTimeout(total=timeout)

    def _unwrap(j):
        if not isinstance(j, dict):
            return {}
        data = j.get("data")
        if data is not None and isinstance(data, dict):
            return data
        return j

    try:
        async with session.get(f"{base}/api/status/product_info", auth=auth, ssl=False, timeout=client_timeout) as r1:
            if r1.status >= 300:
                return result
            j = await r1.json()
            info = _unwrap(j)
            if not isinstance(info, dict):
                return result
    except Exception:
        return result

    try:
        product_name = str(info.get("product_name") or "").strip()
        mac_raw = str(info.get("mac0") or info.get("mac") or "").strip().lower().replace(":", "").replace("-", "")
        manufacturing = info.get("manufacturing") or {}
        serial_num = str(manufacturing.get("serial_num") or manufacturing.get("serial_number") or "").strip() if isinstance(manufacturing, dict) else ""
        serial_num = serial_num or str(info.get("serial_num") or info.get("serial_number") or "").strip()
        result["product_name"] = product_name
        result["mac_address"] = mac_raw.upper() if mac_raw else ""
        result["serial_number"] = serial_num

        system_id = ""
        description = ""
        asset_id = ""
        try:
            async with session.get(f"{base}/api/config/system", auth=auth, ssl=False, timeout=client_timeout) as r2:
                if r2.status < 300:
                    cfg = _unwrap(await r2.json() or {})
                    if isinstance(cfg, dict):
                        system_id = str(cfg.get("system_id") or "").strip()
                        description = str(cfg.get("desc") or cfg.get("description") or "").strip()
                        asset_id = str(cfg.get("asset_id") or "").strip()
            if not system_id:
                async with session.get(f"{base}/api/config/system/system_id", auth=auth, ssl=False, timeout=client_timeout) as r2b:
                    if r2b.status < 300:
                        d = await r2b.json()
                        sid = d.get("data", "") if isinstance(d, dict) else ""
                        system_id = str(sid or "").strip()
        except Exception:
            pass

        fw_info = {}
        try:
            async with session.get(f"{base}/api/status/fw_info", auth=auth, ssl=False, timeout=client_timeout) as r3:
                if r3.status < 300:
                    fw_info = _unwrap(await r3.json() or {})
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
        result["description"] = description
        result["asset_id"] = asset_id
        return result
    except Exception:
        return result


async def call_routers_api_concurrent(targets, method, path, payload=None, max_concurrent=50, timeout=15, metrics=None):
    """
    Call router API on multiple targets concurrently. targets: list of (ip, port, username, password).
    Returns list of (ip, hostname, success, data_or_error) in same order as targets.
    """
    if not aiohttp:
        raise RuntimeError("aiohttp is required for async router client. Install with: pip install aiohttp")

    sem = asyncio.Semaphore(max_concurrent)
    results_by_ip = {}

    async def do_one(t):
        ip, port, username, password = t
        hostname = ""
        async with sem:
            async with aiohttp.ClientSession() as session:
                success, data = await _call_router_api_async(session, ip, port, username, password, method, path, payload, timeout, metrics)
        return ip, hostname, success, data

    tasks = [do_one(t) for t in targets]
    completed = await asyncio.gather(*tasks, return_exceptions=True)
    ordered = []
    for i, t in enumerate(targets):
        ip = t[0]
        if isinstance(completed[i], Exception):
            ordered.append((ip, "", False, _format_async_error(completed[i])))
        else:
            ordered.append(completed[i])
    return ordered


async def fetch_routers_info_concurrent(ip_cred_list, timeout=10, max_concurrent=50):
    """
    Fetch router info from multiple IPs concurrently.
    ip_cred_list: list of (ip, port, username, password).
    Returns list of (ip, info_dict) in same order.
    """
    if not aiohttp:
        raise RuntimeError("aiohttp is required for async router client. Install with: pip install aiohttp")

    sem = asyncio.Semaphore(max_concurrent)

    async def do_one(t):
        ip, port, username, password = t
        async with sem:
            async with aiohttp.ClientSession() as session:
                info = await _fetch_router_info_async(session, ip, port, username, password, timeout)
        return ip, info

    tasks = [do_one(t) for t in ip_cred_list]
    return await asyncio.gather(*tasks, return_exceptions=True)


def run_async(coro):
    """Run async coroutine from sync context."""
    return asyncio.run(coro)
