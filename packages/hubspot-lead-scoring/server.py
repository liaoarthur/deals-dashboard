"""Webhook server — receives HubSpot workflow webhooks and triggers scoring."""

import os
import sys
import hashlib
import hmac
import threading

from flask import Flask, request, jsonify
from flask_cors import CORS

from scoring.pipeline import score_lead
from scoring.database import get_score, get_all_scores

app = Flask(__name__)
CORS(app)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")


# ─── Webhook endpoint ────────────────────────────────────────────────────────

@app.route("/webhook", methods=["POST"])
def handle_webhook():
    """
    Receive a webhook from a HubSpot workflow.

    Expected payload: {"hs_lead_object_id": "...", "hs_contact_object_id": "..."}
    - hs_lead_object_id is required
    - hs_contact_object_id is informational only — pipeline resolves contact via association API
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

    # Score in background so the webhook returns fast
    thread = threading.Thread(target=_score_in_background, args=(lead_id,), daemon=True)
    thread.start()

    return jsonify({"status": "ok", "lead_id": lead_id}), 200


def _score_in_background(lead_id):
    """Run scoring in a background thread so the webhook returns fast."""
    try:
        result = score_lead(lead_id)
        if result:
            print(f"[server] {result.get('tier_display', result['score'])} — lead {lead_id}", file=sys.stderr)
    except Exception as e:
        print(f"[server] Error scoring lead {lead_id}: {e}", file=sys.stderr)


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
