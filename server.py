"""
server.py - Flask Web Server and REST API for Persistent Priority Queue Dashboard

Run: python server.py
Open: http://localhost:5000
"""

import logging
import math
import os
import time
from typing import Any, Dict, Optional
from flask import Flask, render_template, request, jsonify
from module import PersistentPriorityQueue, validate_finite_priority, _UNCHANGED

logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration via environment variables with sensible defaults
BACKEND = os.environ.get("PQ_BACKEND", "json")
STORAGE_FILE = os.environ.get(
    "PQ_STORAGE_FILE",
    "priority_queue.json" if BACKEND == "json" else "priority_queue.db",
)
POSTGRES_DSN = os.environ.get("PQ_POSTGRES_DSN", None)
DECAY_RATE = float(os.environ.get("PQ_DECAY_RATE", "0.01"))
PORT = int(os.environ.get("PQ_PORT", "5000"))

# Initialize persistent queue instance
pq = PersistentPriorityQueue(
    backend=BACKEND,
    storage_file=STORAGE_FILE,
    postgres_dsn=POSTGRES_DSN,
    decay_rate=DECAY_RATE,
)

# In-memory tracking for metrics (session-level)
_extracted_count = 0


def _build_stats() -> Dict[str, Any]:
    """Helper to compute comprehensive stats."""
    items = pq.get_all_items()
    oldest_age = max((item["age_seconds"] for item in items), default=0.0) if items else 0.0
    avg_priority = (
        round(sum(item["priority"] for item in items) / len(items), 2)
        if items
        else None
    )
    avg_effective = (
        round(sum(item["effective_priority"] for item in items) / len(items), 2)
        if items
        else None
    )
    return {
        "size": pq.size(),
        "is_empty": pq.is_empty(),
        "decay_rate": pq.decay_rate,
        "extracted_count": _extracted_count,
        "oldest_age_seconds": oldest_age,
        "avg_base_priority": avg_priority,
        "avg_effective_priority": avg_effective,
        "backend": BACKEND,
        "storage_file": STORAGE_FILE,
    }


# ---------------------------------------------------------------------------
# HTML Dashboard Route
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """Render the dashboard HTML interface."""
    return render_template("index.html")


# ---------------------------------------------------------------------------
# Health and Observability Endpoints
# ---------------------------------------------------------------------------

@app.route("/api/health", methods=["GET"])
def health():
    """Simple system health and status check verifying storage accessibility."""
    try:
        current_size = pq.size()
        return jsonify({
            "status": "healthy",
            "queue_size": current_size,
            "backend": BACKEND,
            "timestamp": time.time(),
        }), 200
    except Exception as e:
        logger.exception("Health check failed: %s", e)
        return jsonify({
            "status": "unhealthy",
            "error": "Storage backend unavailable",
            "backend": BACKEND,
            "timestamp": time.time(),
        }), 500


@app.route("/api/stats", methods=["GET"])
def get_stats():
    """Return comprehensive queue performance and operational metrics."""
    try:
        return jsonify(_build_stats()), 200
    except Exception as e:
        logger.exception("Stats retrieval failed: %s", e)
        return jsonify({"success": False, "error": "Internal server error"}), 500


# ---------------------------------------------------------------------------
# Queue Core API Endpoints
# ---------------------------------------------------------------------------

@app.route("/api/queue", methods=["GET"])
def get_queue():
    """Return all items sorted by effective priority with queue statistics."""
    try:
        items = pq.get_all_items()
        stats = _build_stats()
        return jsonify({"items": items, "stats": stats}), 200
    except Exception as e:
        logger.exception("Queue retrieval failed: %s", e)
        return jsonify({"success": False, "error": "Internal server error"}), 500


@app.route("/api/insert", methods=["POST"])
def insert_item():
    """Insert a new item into the priority queue."""
    try:
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"success": False, "error": "Invalid or missing JSON body"}), 400

        item_id = data.get("item_id")
        priority = data.get("priority")
        payload = data.get("payload")

        if item_id is None or str(item_id).strip() == "":
            return jsonify({"success": False, "error": "Missing required field: item_id"}), 400

        if priority is None:
            return jsonify({"success": False, "error": "Missing required field: priority"}), 400

        try:
            priority_val = float(priority)
            if not math.isfinite(priority_val):
                return jsonify({"success": False, "error": "Priority must be a finite number"}), 400
        except (ValueError, TypeError):
            return jsonify({"success": False, "error": "Priority must be a valid number"}), 400

        pq.insert(item_id=str(item_id).strip(), priority=priority_val, payload=payload)
        return jsonify({
            "success": True,
            "message": f"Item '{item_id}' inserted successfully",
            "item": {
                "item_id": str(item_id).strip(),
                "priority": priority_val,
                "payload": payload,
            },
        }), 201
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.exception("Unexpected error inserting item: %s", e)
        return jsonify({"success": False, "error": "Internal server error"}), 500


@app.route("/api/extract_min", methods=["POST"])
@app.route("/api/extract-min", methods=["POST"])
def extract_min():
    """Extract and remove the item with lowest effective priority."""
    global _extracted_count
    try:
        result = pq.extract_min()
        if result is None:
            return jsonify({"success": False, "error": "Queue is empty"}), 404
        _extracted_count += 1
        item_id, priority, payload = result
        return jsonify({
            "success": True,
            "item": {
                "item_id": item_id,
                "priority": priority,
                "payload": payload,
            },
        }), 200
    except Exception as e:
        logger.exception("Unexpected error in extract_min: %s", e)
        return jsonify({"success": False, "error": "Internal server error"}), 500


@app.route("/api/extract_max", methods=["POST"])
@app.route("/api/extract-max", methods=["POST"])
def extract_max():
    """Extract and remove the item with highest effective priority."""
    global _extracted_count
    try:
        result = pq.extract_max()
        if result is None:
            return jsonify({"success": False, "error": "Queue is empty"}), 404
        _extracted_count += 1
        item_id, priority, payload = result
        return jsonify({
            "success": True,
            "item": {
                "item_id": item_id,
                "priority": priority,
                "payload": payload,
            },
        }), 200
    except Exception as e:
        logger.exception("Unexpected error in extract_max: %s", e)
        return jsonify({"success": False, "error": "Internal server error"}), 500


@app.route("/api/peek", methods=["GET"])
def peek_item():
    """Peek at the min or max item without removing it."""
    try:
        mode = request.args.get("mode", "min")
        if mode not in ("min", "max"):
            return jsonify({"success": False, "error": "mode parameter must be 'min' or 'max'"}), 400

        result = pq.peek(mode=mode)
        if result is None:
            return jsonify({"success": False, "error": "Queue is empty"}), 404
        item_id, priority, payload = result
        return jsonify({
            "success": True,
            "item": {
                "item_id": item_id,
                "priority": priority,
                "payload": payload,
            },
        }), 200
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.exception("Unexpected error in peek: %s", e)
        return jsonify({"success": False, "error": "Internal server error"}), 500


@app.route("/api/update", methods=["POST"])
@app.route("/api/update/<item_id>", methods=["PUT", "POST"])
@app.route("/api/items/<item_id>", methods=["PUT"])
def update_item(item_id: Optional[str] = None):
    """Update priority and/or payload of an existing item."""
    try:
        data = request.get_json(silent=True) or {}
        if not isinstance(data, dict):
            return jsonify({"success": False, "error": "Invalid JSON body"}), 400

        target_id = item_id if item_id is not None else data.get("item_id")
        if target_id is None or str(target_id).strip() == "":
            return jsonify({"success": False, "error": "Missing required field: item_id"}), 400

        priority = data.get("priority")
        payload = data["payload"] if "payload" in data else _UNCHANGED

        if priority is None and payload is _UNCHANGED:
            return jsonify({"success": False, "error": "Must provide 'priority' or 'payload' to update"}), 400

        priority_val = None
        if priority is not None:
            try:
                priority_val = float(priority)
                if not math.isfinite(priority_val):
                    return jsonify({"success": False, "error": "Priority must be a finite number"}), 400
            except (ValueError, TypeError):
                return jsonify({"success": False, "error": "Priority must be a valid number"}), 400

        updated = pq.update(item_id=str(target_id).strip(), priority=priority_val, payload=payload)
        if updated:
            return jsonify({"success": True, "message": f"Item '{target_id}' updated successfully"}), 200
        else:
            return jsonify({"success": False, "error": f"Item '{target_id}' not found"}), 404
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.exception("Unexpected error in update: %s", e)
        return jsonify({"success": False, "error": "Internal server error"}), 500


@app.route("/api/delete", methods=["POST"])
@app.route("/api/delete/<item_id>", methods=["DELETE", "POST"])
@app.route("/api/items/<item_id>", methods=["DELETE"])
def delete_item(item_id: Optional[str] = None):
    """Delete an item from the queue by ID."""
    try:
        target_id = item_id
        if target_id is None:
            data = request.get_json(silent=True) or {}
            if isinstance(data, dict):
                target_id = data.get("item_id")

        if target_id is None or str(target_id).strip() == "":
            return jsonify({"success": False, "error": "Missing required field: item_id"}), 400

        deleted = pq.delete(str(target_id).strip())
        if deleted:
            return jsonify({"success": True, "message": f"Item '{target_id}' deleted successfully"}), 200
        else:
            return jsonify({"success": False, "error": f"Item '{target_id}' not found"}), 404
    except Exception as e:
        logger.exception("Unexpected error in delete: %s", e)
        return jsonify({"success": False, "error": "Internal server error"}), 500


@app.route("/api/clear", methods=["POST"])
def clear_queue():
    """Clear all items from the priority queue."""
    try:
        pq.clear()
        return jsonify({"success": True, "message": "Queue cleared"}), 200
    except Exception as e:
        logger.exception("Unexpected error in clear: %s", e)
        return jsonify({"success": False, "error": "Internal server error"}), 500


if __name__ == "__main__":
    print(f"\n  [*] Persistent Priority Queue Server")
    print(f"  -----------------------------------")
    print(f"  Dashboard:  http://localhost:{PORT}")
    print(f"  Backend:    {BACKEND}")
    print(f"  Storage:    {STORAGE_FILE}")
    print(f"  Decay Rate: {DECAY_RATE}/sec")
    print(f"  -----------------------------------\n")
    app.run(host="0.0.0.0", port=PORT, debug=False)
