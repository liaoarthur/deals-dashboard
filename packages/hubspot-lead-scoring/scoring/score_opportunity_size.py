"""Opportunity Size scoring module.

Scores based on available sizing signals:
1. Team size from Lead object (most relevant sizing signal)
2. Company employee count (fallback)
3. Annual revenue (supplementary)
4. Fallback minimum score if no data available

For large teams (500+), the score is gated on person notability — a
mid-level clinician at a 2,000-person hospital doesn't signal a real
buying opportunity the way a director at a 200-person group does.
"""

import sys
from .config import get_opportunity_size_config


def score(context):
    """Compute opportunity size sub-score (0-100)."""
    config = get_opportunity_size_config()
    company = context.get("company", {})
    props = context.get("properties", {})
    person_lookup = context.get("_enrichment", {}).get("person_lookup")

    signals = []

    # ─── Sizing signals ───────────────────────────────────────────────────
    team_size_score = _score_team_size(props, config, person_lookup)
    employee_score = _score_employees(company, props, config)
    revenue_score = _score_revenue(company, props, config)

    # Use team_size_score if available, otherwise fall back to employee_score
    size_score = team_size_score if team_size_score > 0 else employee_score

    if size_score > 0 and revenue_score > 0:
        size_weight = 0.5 if team_size_score > 0 else 0.4
        rev_weight = 1.0 - size_weight
        score_value = int(size_score * size_weight + revenue_score * rev_weight)
    elif size_score > 0:
        score_value = size_score
    elif revenue_score > 0:
        score_value = revenue_score
    else:
        score_value = 15  # Minimum fallback

    if team_size_score:
        signals.append(f"team_size_score={team_size_score}")
    if employee_score:
        signals.append(f"employee_score={employee_score}")
    if revenue_score:
        signals.append(f"revenue_score={revenue_score}")

    score_value = max(0, min(100, score_value))
    print(f"[opportunity_size] score={score_value} signals={signals}", file=sys.stderr)
    return score_value


TEAM_SIZE_ENUM = {
    "JUST_ME": 1,
    "TWO_TO_FIVE": 3,
    "SIX_TO_TWENTY": 13,
    "TWENTY_ONE_TO_FIFTY": 35,
    "FIFTY_ONE_PLUS": 75,
}


def _score_team_size(props, config, person_lookup):
    """Score based on team_size from Lead object.
    Handles both HubSpot enum strings (JUST_ME, TWO_TO_FIVE, etc.) and numeric values.
    For large teams (500+), gates on person notability."""
    raw = props.get("team_size")
    if not raw:
        return 0

    # Try enum mapping first, then numeric parse
    team_size = TEAM_SIZE_ENUM.get(str(raw).strip().upper()) or _parse_number(raw)
    if not team_size:
        return 0

    threshold = config.get("large_team_threshold", 500)

    if team_size >= threshold:
        if _person_is_notable(person_lookup):
            score_val = config.get("large_team_notable_score", 90)
            print(f"[opportunity_size] large team ({int(team_size)}) + notable person → {score_val}", file=sys.stderr)
        else:
            score_val = config.get("large_team_default_score", 60)
            print(f"[opportunity_size] large team ({int(team_size)}) + non-notable person → {score_val}", file=sys.stderr)
        return score_val

    for tier in config.get("team_size_tiers", []):
        if team_size <= tier["max"]:
            return tier["score"]

    return 90


def _person_is_notable(person_lookup):
    """Check if the person is in a leadership/decision-making role."""
    if not person_lookup:
        return False
    if person_lookup.get("is_decision_maker"):
        return True
    seniority = person_lookup.get("seniority", "unknown")
    if seniority in ("founder", "c_suite", "clinical_leader", "vp", "director"):
        return True
    return False


def _score_employees(company, contact_props, config):
    """Score based on employee count."""
    count = _parse_number(
        company.get("numberofemployees")
        or contact_props.get("numemployees")
    )
    if not count:
        return 0

    for tier in config.get("employee_tiers", []):
        if count <= tier["max"]:
            return tier["score"]

    return 95  # Above all tiers


def _score_revenue(company, contact_props, config):
    """Score based on annual revenue."""
    revenue = _parse_number(
        company.get("annualrevenue")
        or contact_props.get("annualrevenue")
    )
    if not revenue:
        return 0

    for tier in config.get("revenue_tiers", []):
        if revenue <= tier["max"]:
            return tier["score"]

    return 95


def _parse_number(value):
    """Safely parse a numeric value from various string formats."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").replace("$", "").replace("+", "").strip()
        if not cleaned:
            return None
        return float(cleaned)
    except (ValueError, TypeError):
        return None
