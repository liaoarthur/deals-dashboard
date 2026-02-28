"""Scoring pipeline — orchestrates the full scoring flow with dedup and error handling."""

import sys
import time
import threading
from datetime import datetime, timezone

from .config import get_weights, get_dedup_window, get_weight_adjustments, get_score_floors
from .hubspot_client import fetch_lead_context, write_lead_score
from .router import classify_lead_type, get_modules_for_lead_type, extract_message_text
from .score_opportunity_size import score as score_opportunity_size
from .score_message import score as score_message
from .score_person_role import score as score_person_role
from .score_specialty_company import score as score_specialty_company
from .inbound import score_inbound
from .database import upsert_score
from .tier import classify_tier, format_tier_display, build_rationale


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

def score_lead(lead_id, company_id=None):
    """
    Full scoring pipeline for a single lead.
    Fetches Lead → Contact → Company, then runs scoring modules.
    Returns the scored record dict, or None if dedup-skipped.

    company_id: optional HubSpot company ID (from webhook payload).
    """
    lead_id = str(lead_id)

    # Dedup check
    if not _check_dedup(lead_id):
        print(f"[pipeline] Skipping lead {lead_id} — duplicate within dedup window", file=sys.stderr)
        return None

    try:
        return _run_pipeline(lead_id, company_id=company_id)
    finally:
        _clear_dedup(lead_id)


def _run_pipeline(lead_id, company_id=None):
    """Core pipeline logic, separated from dedup for testability."""
    # 1. Fetch full context: Lead → Contact → Company
    print(f"[pipeline] Fetching context for lead {lead_id}...", file=sys.stderr)
    context = fetch_lead_context(lead_id, company_id=company_id)

    # 2. Classify lead type
    lead_type = classify_lead_type(context)
    print(f"[pipeline] Lead type: {lead_type}", file=sys.stderr)

    # 2b. Inbound leads use the new 5-criteria scoring system
    if lead_type == "inbound":
        return _run_inbound_pipeline(lead_id, lead_type, context)

    # 3. Determine which modules to run (non-inbound path)
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

    # 5. Compute weighted composite score (with contextual adjustments)
    weights = get_weights(lead_type)
    adjusted_weights = _adjust_weights(weights, context, sub_scores)
    composite_score, weights_used = _compute_composite(sub_scores, adjusted_weights, modules_succeeded)

    # 5b. Apply score floors — strong person_role or message_analysis guarantees B-Monitor
    composite_score = _apply_score_floors(composite_score, sub_scores)

    # 6. Classify tier
    tier_label = classify_tier(composite_score)
    tier_display = format_tier_display(tier_label, composite_score)

    # 7. Build the scored record
    record = {
        "hubspot_record_id": lead_id,
        "lead_type": lead_type,
        "score": composite_score,
        "tier": tier_label,
        "tier_display": tier_display,
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
            "person_lookup": context.get("_enrichment", {}).get("person_lookup"),
            "errors": errors,
        },
    }

    # 8. Generate rationale from the complete record
    record["rationale"] = build_rationale(record)

    # 9. Store locally
    upsert_score(record)

    # 10. Write score back to HubSpot Lead record
    try:
        write_lead_score(lead_id, tier_display, record["rationale"])
    except Exception as e:
        print(f"[pipeline] HubSpot writeback failed for lead {lead_id}: {e}", file=sys.stderr)

    print(f"[pipeline] {tier_display} — lead {lead_id} ({lead_type})", file=sys.stderr)
    return record


def _run_inbound_pipeline(lead_id, lead_type, context):
    """
    Inbound-specific scoring pipeline using the 5-criteria system.
    Called instead of the generic module pipeline for inbound leads.
    """
    print(f"[pipeline] Running inbound 5-criteria scoring for lead {lead_id}", file=sys.stderr)

    # Run inbound scoring
    inbound_result = score_inbound(context)

    composite_score = inbound_result["score"]
    sub_scores = inbound_result["sub_scores"]
    weights_used = inbound_result["weights_used"]
    modules_run = inbound_result["modules_run"]

    # Classify tier
    tier_label = classify_tier(composite_score)
    tier_display = format_tier_display(tier_label, composite_score)

    # Build the scored record (same structure as generic pipeline)
    record = {
        "hubspot_record_id": lead_id,
        "lead_type": lead_type,
        "score": composite_score,
        "tier": tier_label,
        "tier_display": tier_display,
        "sub_scores": sub_scores,
        "modules_run": modules_run,
        "weights_used": weights_used,
        "scored_at": datetime.now(timezone.utc).isoformat(),
        "raw_inputs": {
            "lead_properties": context.get("lead_properties", {}),
            "contact_id": context.get("contact_id"),
            "merged_properties": context.get("properties", {}),
            "form_count": len(context.get("form_submissions", [])),
            "has_company": bool(context.get("company")),
            "person_lookup": context.get("_enrichment", {}).get("person_lookup"),
            "errors": [],
        },
    }

    # Generate rationale
    record["rationale"] = build_rationale(record)

    # Store locally
    upsert_score(record)

    # Write score back to HubSpot Lead record
    try:
        write_lead_score(lead_id, tier_display, record["rationale"])
    except Exception as e:
        print(f"[pipeline] HubSpot writeback failed for lead {lead_id}: {e}", file=sys.stderr)

    print(f"[pipeline] {tier_display} — lead {lead_id} ({lead_type}, inbound)", file=sys.stderr)
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

    elif module_name == "specialty_company":
        return score_specialty_company(context)

    else:
        raise ValueError(f"Unknown scoring module: {module_name}")


def _adjust_weights(base_weights, context, sub_scores):
    """
    Contextual weight adjustments based on lead signals.

    1. Physician at a large health system who isn't a decision-maker
       → reduce opportunity_size weight
       (org size inflates opportunity score but this person can't drive a deal)

    2. High/low intent message → boost/reduce message_analysis weight

    Returns a new weights dict (does not mutate the original).
    """
    adj = get_weight_adjustments()
    weights = dict(base_weights)

    person_lookup = context.get("_enrichment", {}).get("person_lookup")

    is_non_notable_at_large_system = False

    if person_lookup:
        seniority = person_lookup.get("seniority", "unknown")
        is_clinical = person_lookup.get("is_clinical", False)
        is_decision_maker = person_lookup.get("is_decision_maker", False)

        # Check: physician (clinical + non-decision-maker) at a large system
        if is_clinical and not is_decision_maker and seniority in ("individual", "senior", "unknown"):
            is_non_notable_at_large_system = True

    adjustments_applied = []

    # Reduction: Non-notable physician at large system → opportunity_size matters less
    if is_non_notable_at_large_system and "opportunity_size" in weights:
        reduction = adj.get("non_notable_physician_opp_size_reduction", 0.10)
        weights["opportunity_size"] = max(0.05, weights["opportunity_size"] - reduction)
        adjustments_applied.append(f"opp_size -{reduction} (non-notable physician at large system)")

    # Boost/reduce message_analysis weight based on intent level
    msg_score = sub_scores.get("message_analysis")
    if msg_score is not None and "message_analysis" in weights:
        high_threshold = adj.get("high_intent_message_threshold", 70)
        low_threshold = adj.get("low_intent_message_threshold", 40)
        if msg_score >= high_threshold:
            boost = adj.get("high_intent_message_boost", 0.10)
            weights["message_analysis"] += boost
            adjustments_applied.append(f"message +{boost} (high intent, score={msg_score})")
        elif msg_score < low_threshold:
            reduction = adj.get("low_intent_message_reduction", 0.10)
            weights["message_analysis"] = max(0.05, weights["message_analysis"] - reduction)
            adjustments_applied.append(f"message -{reduction} (low intent, score={msg_score})")

    if adjustments_applied:
        print(f"[pipeline] Weight adjustments: {adjustments_applied}", file=sys.stderr)

    return weights


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


def _apply_score_floors(composite_score, sub_scores):
    """
    If a key signal is strong enough, guarantee a minimum composite score.
    A high person_role or message_analysis score means at least B-Monitor.
    """
    floors = get_score_floors()
    if not floors:
        return composite_score

    floor_score = floors.get("floor_score", 50)
    triggered_by = []

    person_threshold = floors.get("person_role_threshold", 70)
    person_score = sub_scores.get("person_role")
    if person_score is not None and person_score >= person_threshold:
        triggered_by.append(f"person_role={person_score}")

    msg_threshold = floors.get("message_analysis_threshold", 70)
    msg_score = sub_scores.get("message_analysis")
    if msg_score is not None and msg_score >= msg_threshold:
        triggered_by.append(f"message_analysis={msg_score}")

    if triggered_by and composite_score < floor_score:
        print(
            f"[pipeline] Score floor applied: {composite_score} → {floor_score} "
            f"(triggered by {', '.join(triggered_by)})",
            file=sys.stderr,
        )
        return floor_score

    return composite_score
