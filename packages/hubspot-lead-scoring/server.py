"""Webhook server — receives HubSpot webhook events and triggers scoring."""

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
    Receive HubSpot webhook events.
    Expects an array of event objects from HubSpot.
    Supported: lead.creation, lead.propertyChange
    """
    # Optional signature verification
    if WEBHOOK_SECRET:
        signature = request.headers.get("X-HubSpot-Signature", "")
        body = request.get_data(as_text=True)
        expected = hashlib.sha256((WEBHOOK_SECRET + body).encode()).hexdigest()
        if not hmac.compare_digest(signature, expected):
            return jsonify({"error": "Invalid signature"}), 401

    events = request.get_json(force=True, silent=True)
    if not events:
        return jsonify({"error": "No events in payload"}), 400

    # HubSpot sends an array of events
    if not isinstance(events, list):
        events = [events]

    # Filter for lead events we care about
    lead_ids = set()
    for event in events:
        sub_type = event.get("subscriptionType", "")
        if sub_type in ("lead.creation", "lead.propertyChange"):
            lid = event.get("objectId")
            if lid:
                lead_ids.add(str(lid))

    if not lead_ids:
        return jsonify({"status": "ok", "message": "No scoreable events"}), 200

    # Score each lead in a background thread (don't block the webhook response)
    for lid in lead_ids:
        thread = threading.Thread(target=_score_in_background, args=(lid,), daemon=True)
        thread.start()

    return jsonify({
        "status": "ok",
        "leads_queued": list(lead_ids),
    }), 200


def _score_in_background(lead_id):
    """Run scoring in a background thread so the webhook returns fast."""
    try:
        result = score_lead(lead_id)
        if result:
            print(f"[server] Scored lead {lead_id}: {result['score']}/100", file=sys.stderr)
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
    port = int(os.getenv("WEBHOOK_PORT", 3000))
    print(f"Lead scoring webhook server starting on port {port}...", file=sys.stderr)
    app.run(host="0.0.0.0", port=port, debug=True)
