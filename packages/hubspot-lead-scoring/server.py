"""Webhook server — receives HubSpot workflow webhooks and triggers scoring."""

import os
import sys
import hashlib
import hmac
import time
import threading

from flask import Flask, request, jsonify
from flask_cors import CORS

from scoring.pipeline import score_lead
from scoring.database import get_score, get_all_scores
from scoring.hubspot_client import search_unscored_leads

app = Flask(__name__)
CORS(app)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")


# ─── Webhook endpoint ────────────────────────────────────────────────────────

@app.route("/webhook", methods=["POST"])
def handle_webhook():
    """
    Receive a webhook from a HubSpot workflow.

    Expected payload: {"hs_lead_object_id": "...", "hs_contact_object_id": "...", "hs_company_object_id": "..."}
    - hs_lead_object_id is required
    - hs_contact_object_id is informational only — pipeline resolves contact via association API
    - hs_company_object_id is optional — passed through to pipeline for company data lookup
    """
    # Optional signature verification
    if WEBHOOK_SECRET:
        signature = request.headers.get("X-HubSpot-Signature", "")
        body = request.get_data(as_text=True)
        expected = hashlib.sha256((WEBHOOK_SECRET + body).encode()).hexdigest()
        if not hmac.compare_digest(signature, expected):
            return jsonify({"error": "Invalid signature"}), 401

    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "No JSON payload"}), 400

    # Accept hs_lead_object_id (HubSpot workflow), or lead_id/objectId as fallbacks
    lead_id = str(
        data.get("hs_lead_object_id")
        or data.get("lead_id")
        or data.get("objectId")
        or ""
    ).strip()
    if not lead_id:
        return jsonify({"error": "hs_lead_object_id is required"}), 400

    # Optional company ID (used for company product usage scoring)
    company_id = str(data.get("hs_company_object_id") or "").strip() or None

    # Score in background so the webhook returns fast
    thread = threading.Thread(target=_score_in_background, args=(lead_id, company_id), daemon=True)
    thread.start()

    return jsonify({"status": "ok", "lead_id": lead_id, "company_id": company_id}), 200


def _score_in_background(lead_id, company_id=None):
    """Run scoring in a background thread so the webhook returns fast."""
    try:
        result = score_lead(lead_id, company_id=company_id)
        if result:
            print(f"[server] {result.get('tier_display', result['score'])} — lead {lead_id}", file=sys.stderr)
    except Exception as e:
        print(f"[server] Error scoring lead {lead_id}: {e}", file=sys.stderr)


# ─── Backlog batch processing ─────────────────────────────────────────────────

_backlog_status = {
    "running": False,
    "batch_size": 0,
    "total_unscored": 0,
    "scored": 0,
    "errors": 0,
    "current_lead": None,
    "results": [],
}
_backlog_lock = threading.Lock()


@app.route("/backlog", methods=["POST"])
def start_backlog():
    """
    Score the next batch of unscored inbound US leads (oldest first).

    Optional JSON body: {"batch_size": 10, "delay_seconds": 5}
    """
    with _backlog_lock:
        if _backlog_status["running"]:
            return jsonify({"error": "Batch already in progress", "status": _backlog_status}), 409

    data = request.get_json(silent=True) or {}
    batch_size = data.get("batch_size", 10)
    delay_seconds = data.get("delay_seconds", 5)

    # Fetch the batch of lead IDs before starting background work
    try:
        lead_ids, total_unscored = search_unscored_leads(batch_size=batch_size)
    except Exception as e:
        return jsonify({"error": f"HubSpot search failed: {e}"}), 500

    if not lead_ids:
        return jsonify({"status": "done", "message": "No unscored leads found matching criteria"}), 200

    # Initialize status
    with _backlog_lock:
        _backlog_status.update({
            "running": True,
            "batch_size": len(lead_ids),
            "total_unscored": total_unscored,
            "scored": 0,
            "errors": 0,
            "current_lead": None,
            "results": [],
        })

    thread = threading.Thread(
        target=_process_batch,
        args=(lead_ids, delay_seconds),
        daemon=True,
    )
    thread.start()

    return jsonify({
        "status": "started",
        "batch_size": len(lead_ids),
        "total_unscored": total_unscored,
        "delay_seconds": delay_seconds,
        "leads": lead_ids,
    }), 200


@app.route("/backlog/status", methods=["GET"])
def backlog_status():
    """Check progress of the current/last batch."""
    return jsonify(_backlog_status)


def _process_batch(lead_ids, delay_seconds):
    """Score a list of leads sequentially with a delay between each."""
    for i, lead_id in enumerate(lead_ids):
        with _backlog_lock:
            _backlog_status["current_lead"] = lead_id

        print(f"[backlog] Scoring lead {lead_id} ({i + 1}/{len(lead_ids)})...", file=sys.stderr)

        try:
            result = score_lead(lead_id)
            if result:
                with _backlog_lock:
                    _backlog_status["scored"] += 1
                    _backlog_status["results"].append({
                        "lead_id": lead_id,
                        "tier_display": result.get("tier_display", ""),
                        "score": result.get("score", 0),
                    })
                print(f"[backlog] {result.get('tier_display')} — lead {lead_id}", file=sys.stderr)
            else:
                with _backlog_lock:
                    _backlog_status["errors"] += 1
                print(f"[backlog] No result for lead {lead_id} (dedup or error)", file=sys.stderr)
        except Exception as e:
            with _backlog_lock:
                _backlog_status["errors"] += 1
            print(f"[backlog] Error scoring lead {lead_id}: {e}", file=sys.stderr)

        # Delay between leads (skip after the last one)
        if i < len(lead_ids) - 1:
            time.sleep(delay_seconds)

    with _backlog_lock:
        _backlog_status["running"] = False
        _backlog_status["current_lead"] = None

    print(
        f"[backlog] Batch complete: {_backlog_status['scored']} scored, "
        f"{_backlog_status['errors']} errors",
        file=sys.stderr,
    )


# ─── API endpoints (for inspection) ──────────────────────────────────────────

@app.route("/scores", methods=["GET"])
def list_scores():
    """List all scored records."""
    limit = request.args.get("limit", 50, type=int)
    records = get_all_scores(limit=limit)
    return jsonify(records)


@app.route("/scores/<hubspot_record_id>", methods=["GET"])
def get_single_score(hubspot_record_id):
    """Get a single scored record by HubSpot ID."""
    record = get_score(hubspot_record_id)
    if record is None:
        return jsonify({"error": "Not found"}), 404
    return jsonify(record)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Railway sets PORT; WEBHOOK_PORT is for local dev; fallback to 3000
    port = int(os.getenv("PORT", os.getenv("WEBHOOK_PORT", 3000)))
    print(f"Lead scoring webhook server starting on port {port}...", file=sys.stderr)
    app.run(host="0.0.0.0", port=port)
