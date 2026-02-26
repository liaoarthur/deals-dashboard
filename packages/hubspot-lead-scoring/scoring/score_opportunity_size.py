"""Opportunity Size scoring module.

Priority order:
1. Form submission exists → high base score + any budget/size fields
2. Enriched company data (employee count, revenue)
3. Fallback minimum score
"""

import sys
from .config import get_opportunity_size_config


def score(context):
    """Compute opportunity size sub-score (0-100)."""
    config = get_opportunity_size_config()
    form_submissions = context.get("form_submissions", [])
    company = context.get("company", {})
    props = context.get("properties", {})

    score_value = 0
    signals = []

    # ─── Form submission signal (top tier) ────────────────────────────────
    if form_submissions:
        base = config.get("form_submission_base_score", 70)
        score_value = base
        signals.append(f"form_submission_base={base}")

        # Check for budget/size fields in form data
        budget_boost = _extract_budget_signal(form_submissions)
        if budget_boost > 0:
            score_value = min(100, score_value + budget_boost)
            signals.append(f"budget_boost={budget_boost}")

    # ─── Company data fallback ────────────────────────────────────────────
    employee_score = _score_employees(company, props, config)
    revenue_score = _score_revenue(company, props, config)

    if not form_submissions:
        # No form: company data is the primary signal
        if employee_score > 0 and revenue_score > 0:
            score_value = int(employee_score * 0.4 + revenue_score * 0.6)
        elif employee_score > 0:
            score_value = employee_score
        elif revenue_score > 0:
            score_value = revenue_score
        else:
            score_value = 15  # Minimum fallback
    else:
        # Has form: company data can boost above base
        company_signal = max(employee_score, revenue_score)
        if company_signal > score_value:
            score_value = int(score_value * 0.6 + company_signal * 0.4)

    if employee_score:
        signals.append(f"employee_score={employee_score}")
    if revenue_score:
        signals.append(f"revenue_score={revenue_score}")

    score_value = max(0, min(100, score_value))
    print(f"[opportunity_size] score={score_value} signals={signals}", file=sys.stderr)
    return score_value


def _extract_budget_signal(form_submissions):
    """Look for budget/size indicators in form fields."""
    budget_keywords = ["budget", "spend", "size", "seats", "users", "employees", "revenue"]

    for submission in form_submissions:
        fields = submission.get("fields", {})
        for key, value in fields.items():
            if any(kw in key.lower() for kw in budget_keywords):
                # Try to parse a numeric value
                num = _parse_number(value)
                if num and num > 0:
                    if num > 100000:
                        return 25
                    elif num > 10000:
                        return 15
                    elif num > 1000:
                        return 10
                    else:
                        return 5
    return 0


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
