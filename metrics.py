"""In-process metrics for connection counts, timeouts, and memory growth."""

import threading

# Thread-safe counters
_lock = threading.Lock()
_connections_active = 0
_requests_total = 0
_timeouts_total = 0
_errors_total = 0


def connection_started():
    """Increment active connection count."""
    global _connections_active
    with _lock:
        _connections_active += 1


def connection_finished():
    """Decrement active connection count."""
    global _connections_active
    with _lock:
        _connections_active = max(0, _connections_active - 1)


def request_completed(success=True, timed_out=False):
    """Record request completion. success=False or timed_out=True increments error/timeout counters."""
    global _requests_total, _timeouts_total, _errors_total
    with _lock:
        _requests_total += 1
        if timed_out:
            _timeouts_total += 1
        if not success:
            _errors_total += 1


def get_metrics(routers_count=0):
    """Return current metrics as a dict."""
    with _lock:
        return {
            "connections_active": _connections_active,
            "requests_total": _requests_total,
            "timeouts_total": _timeouts_total,
            "errors_total": _errors_total,
            "memory_rss_mb": _get_memory_rss_mb(),
            "routers_count": routers_count,
        }


def _get_memory_rss_mb():
    """Return process RSS in MB if psutil available, else None."""
    try:
        import psutil
        process = psutil.Process()
        return round(process.memory_info().rss / (1024 * 1024), 2)
    except ImportError:
        return None
