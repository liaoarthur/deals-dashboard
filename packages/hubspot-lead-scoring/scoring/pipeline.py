"""Scoring pipeline — orchestrates the full scoring flow with dedup and error handling."""

import sys
import time
import threading
from datetime import datetime, timezone

from .config import get_weights, get_dedup_window
from .hubspot_client import fetch_lead_context
from .router import classify_lead_type, get_modules_for_lead_type, extract_message_text
from .score_opportunity_size import score as score_opportunity_size
from .score_message import score as score_message
from .score_person_role import score as score_person_role
from .database import upsert_score


# ─── Deduplication ────────────────────────────────────────────────────────────
_in_flight = {}  # lead_id → timestamp
_in_flight_lock = threading.Lock()


def _check_dedup(lead_id):
    """Return True if this lead should be scored (not a duplicate)."""
    window = get_dedup_window()
    now = time.time()

    with _in_flight_lock:
        last = _in_flight.get(lead_id)
        if last and (now - last) < window:
            return False
        _in_flight[lead_id] = now
        return True


def _clear_dedup(lead_id):
    """Remove a lead from the in-flight set after scoring completes."""
    with _in_flight_lock:
        _in_flight.pop(lead_id, None)


# ─── Pipeline ─────────────────────────────────────────────────────────────────

def score_lead(lead_id):
    """
    Full scoring pipeline for a single lead.
    Fetches Lead → Contact → Company, then runs scoring modules.
    Returns the scored record dict, or None if dedup-skipped.
    """
    lead_id = str(lead_id)

    # Dedup check
    if not _check_dedup(lead_id):
        print(f"[pipeline] Skipping lead {lead_id} — duplicate within dedup window", file=sys.stderr)
        return None

    try:
        return _run_pipeline(lead_id)
    finally:
        _clear_dedup(lead_id)


def _run_pipeline(lead_id):
    """Core pipeline logic, separated from dedup for testability."""
    # 1. Fetch full context: Lead → Contact → Company
    print(f"[pipeline] Fetching context for lead {lead_id}...", file=sys.stderr)
    context = fetch_lead_context(lead_id)

    # 2. Classify lead type
    lead_type = classify_lead_type(context)
    print(f"[pipeline] Lead type: {lead_type}", file=sys.stderr)

    # 3. Determine which modules to run
    modules_to_run = get_modules_for_lead_type(lead_type, context)
    print(f"[pipeline] Modules: {modules_to_run}", file=sys.stderr)

    # 4. Run each module, collecting scores and handling errors
    sub_scores = {}
    modules_succeeded = []
    errors = []

    for module_name in modules_to_run:
        try:
            module_score = _run_module(module_name, context)
            if module_score is not None:
                sub_scores[module_name] = module_score
                modules_succeeded.append(module_name)
            else:
                print(f"[pipeline] Module '{module_name}' returned None, excluding", file=sys.stderr)
        except Exception as e:
            print(f"[pipeline] Module '{module_name}' failed: {e}", file=sys.stderr)
            errors.append({"module": module_name, "error": str(e)})

    # 5. Compute weighted composite score
    weights = get_weights(lead_type)
    composite_score, weights_used = _compute_composite(sub_scores, weights, modules_succeeded)

    # 6. Build the scored record
    record = {
        "hubspot_record_id": lead_id,
        "lead_type": lead_type,
        "score": composite_score,
        "sub_scores": sub_scores,
        "modules_run": modules_succeeded,
        "weights_used": weights_used,
        "scored_at": datetime.now(timezone.utc).isoformat(),
        "raw_inputs": {
            "lead_properties": context.get("lead_properties", {}),
            "contact_id": context.get("contact_id"),
            "merged_properties": context.get("properties", {}),
            "form_count": len(context.get("form_submissions", [])),
            "has_company": bool(context.get("company")),
            "errors": errors,
        },
    }

    # 7. Store locally
    upsert_score(record)

    print(f"[pipeline] Scored lead {lead_id}: {composite_score}/100 ({lead_type})", file=sys.stderr)
    return record


def _run_module(module_name, context):
    """Dispatch to the appropriate scoring module."""
    if module_name == "opportunity_size":
        return score_opportunity_size(context)

    elif module_name == "message_analysis":
        message_text = extract_message_text(context)
        return score_message(message_text)

    elif module_name == "person_role":
        return score_person_role(context)

    else:
        raise ValueError(f"Unknown scoring module: {module_name}")


def _compute_composite(sub_scores, configured_weights, modules_run):
    """
    Compute weighted average, redistributing weights if some modules didn't run.
    Returns (score, weights_used_dict).
    """
    if not sub_scores:
        return 0, {}

    # Filter to only weights for modules that succeeded
    active_weights = {m: configured_weights.get(m, 0) for m in modules_run if m in sub_scores}

    if not active_weights:
        return 0, {}

    # Normalize weights to sum to 1.0
    total_weight = sum(active_weights.values())
    if total_weight == 0:
        return 0, {}

    normalized = {m: w / total_weight for m, w in active_weights.items()}

    composite = sum(sub_scores[m] * normalized[m] for m in normalized)
    composite = max(0, min(100, round(composite)))

    return composite, normalized
