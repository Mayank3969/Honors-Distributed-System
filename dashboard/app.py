"""
MODULE 5: DASHBOARD SERVER
Author: Person 5
Description: Flask web server that displays real-time analytics from the pipeline.
             Shows log levels, error rates, worker activity, and top IPs.
             Reads from Storage Layer (DynamoDB or local files).
"""

import json
import os
import sys
import time
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'storage'))

from flask import Flask, render_template, jsonify
from storage_layer import StorageQuery

app = Flask(__name__)
query = StorageQuery()

# Cache to avoid hammering DB on every request
_cache = {"data": None, "last_updated": 0}
CACHE_TTL = 10  # seconds


def get_cached_stats() -> dict:
    now = time.time()
    if _cache["data"] is None or (now - _cache["last_updated"]) > CACHE_TTL:
        _cache["data"] = query.get_summary_stats()
        _cache["last_updated"] = now
    return _cache["data"]


# ─── ROUTES ────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Main dashboard page"""
    stats = get_cached_stats()
    return render_template("dashboard.html", stats=stats)


@app.route("/api/stats")
def api_stats():
    """JSON API for dashboard auto-refresh"""
    return jsonify(get_cached_stats())


@app.route("/api/results")
def api_results():
    """All batch results"""
    results = query.get_all_results()
    return jsonify({"results": results, "count": len(results)})


@app.route("/api/workers")
def api_workers():
    """Worker activity summary"""
    results = query.get_all_results()
    worker_map = {}
    for r in results:
        wid = r.get("worker_id", "unknown")
        if wid not in worker_map:
            worker_map[wid] = {"batches": 0, "total_logs": 0, "errors": 0}
        worker_map[wid]["batches"]    += 1
        worker_map[wid]["total_logs"] += r.get("total_logs", 0)
        lc = r.get("level_counts", {})
        if isinstance(lc, str):
            lc = json.loads(lc)
        worker_map[wid]["errors"] += lc.get("ERROR", 0)
    return jsonify(worker_map)


@app.route("/health")
def health():
    return jsonify({"status": "ok", "timestamp": time.time()})


# ─── MAIN ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print(f"\n[DASHBOARD] Starting on http://0.0.0.0:{port}")
    print(f"[DASHBOARD] Storage mode: {query.mode}")
    app.run(host="0.0.0.0", port=port, debug=True)
