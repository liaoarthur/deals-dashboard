"""Inbound lead scoring — 5-criteria system for inbound leads.

Criteria:
  1. Size         — org size enum → weighted score
  2. Role         — leadership detection → boost only (+0 to +10)
  3. Message      — buying intent via Claude analysis → weighted score
  4. Contact Usage — product session count + recency → boost only (+0 to +8)
  5. Company Usage — company session count + user count → weighted when data exists

Architecture:
  Base Score = weighted_average(size, message, [company_usage])
  Final Score = min(100, Base + role_boost + contact_usage_boost)
"""

import re
import sys
from datetime import datetime, timezone, timedelta

from .config import get_inbound_scoring_config
from .router import extract_message_text
from .score_message import score as score_message_analysis
from .claude_client import lookup_person


def score_inbound(context):
    """
    Run all 5 inbound scoring criteria and compute the composite score.

    Returns dict with:
      - score (0-100)
      - sub_scores: {size, message_analysis, company_usage, role_boost, contact_usage_boost}
      - weights_used: the weight dict that was applied
      - modules_run: list of criteria that ran
    """
    config = get_inbound_scoring_config()
    props = context.get("properties", {})
    company = context.get("company", {})

    sub_scores = {}
    modules_run = []

    # ── Criterion 1: Size ────────────────────────────────────────────────
    size_score = _score_size(props, config)
    sub_scores["size"] = size_score
    modules_run.append("size")
    print(f"[inbound] size={size_score}", file=sys.stderr)

    # ── Criterion 2: Role (boost only) ───────────────────────────────────
    role_boost = _score_role(context, config)
    sub_scores["role_boost"] = role_boost
    modules_run.append("role_boost")
    print(f"[inbound] role_boost=+{role_boost}", file=sys.stderr)

    # ── Criterion 3: Message analysis ────────────────────────────────────
    message_text = extract_message_text(context)
    message_score = None
    if message_text and len(message_text.strip()) >= 10:
        try:
            message_score = score_message_analysis(message_text)
        except Exception as e:
            print(f"[inbound] Message analysis failed: {e}", file=sys.stderr)
            message_score = None

    has_message = message_score is not None
    if has_message:
        sub_scores["message_analysis"] = message_score
        modules_run.append("message_analysis")
        print(f"[inbound] message_analysis={message_score}", file=sys.stderr)
    else:
        print("[inbound] No analyzable message, excluding from weights", file=sys.stderr)

    # ── Criterion 4: Contact product usage (boost only) ──────────────────
    contact_boost = _score_contact_product_usage(props, config)
    sub_scores["contact_usage_boost"] = contact_boost
    modules_run.append("contact_usage_boost")
    print(f"[inbound] contact_usage_boost=+{contact_boost}", file=sys.stderr)

    # ── Criterion 5: Company product usage ───────────────────────────────
    company_usage_score, has_company_data = _score_company_product_usage(company, config)
    if has_company_data:
        sub_scores["company_usage"] = company_usage_score
        modules_run.append("company_usage")
        print(f"[inbound] company_usage={company_usage_score}", file=sys.stderr)
    else:
        print("[inbound] No company usage data, excluding from weights", file=sys.stderr)

    # ── Composite calculation ────────────────────────────────────────────
    base_score, weights_used = _compute_inbound_composite(
        size_score, message_score, company_usage_score,
        has_message, has_company_data, config,
    )

    # Add boosts (capped at 100)
    final_score = min(100, base_score + role_boost + contact_boost)

    print(
        f"[inbound] base={base_score} + role_boost={role_boost} + contact_boost={contact_boost} "
        f"= {final_score}",
        file=sys.stderr,
    )

    return {
        "score": final_score,
        "sub_scores": sub_scores,
        "weights_used": weights_used,
        "modules_run": modules_run,
    }


# ─── Criterion 1: Size ──────────────────────────────────────────────────────

def _score_size(props, config):
    """
    Score based on organization_size enum.
    Fallback chain: organization_size → organisation_size__product_ → company_employee_size_range__c_
    All on the contact object.
    """
    enum_scores = config.get("size_enum_scores", {})
    no_data = config.get("size_no_data_score", 20)

    # Primary: organization_size
    raw = (props.get("organization_size") or "").strip().upper()
    if raw and raw in enum_scores:
        return enum_scores[raw]

    # Fallback 1: organisation_size__product_
    fallback1 = (props.get("organisation_size__product_") or "").strip().upper()
    if fallback1 and fallback1 in enum_scores:
        return enum_scores[fallback1]

    # Fallback 2: company_employee_size_range__c_
    fallback2 = (props.get("company_employee_size_range__c_") or "").strip().upper()
    if fallback2 and fallback2 in enum_scores:
        return enum_scores[fallback2]

    # No data
    return no_data


# ─── Criterion 2: Role (boost only) ─────────────────────────────────────────

def _score_role(context, config):
    """
    Determine role boost: +leadership_boost if leadership, +0 otherwise.
    If both role fields are empty, perform a web search via lookup_person().
    """
    props = context.get("properties", {})
    leadership_boost = config.get("role_boost_leadership", 10)
    keywords = config.get("leadership_keywords", [])

    # Primary: organisation_type__product_ (self-reported role in product)
    role_text = (props.get("organisation_type__product_") or "").strip()

    # Fallback: lc_job_title
    if not role_text:
        role_text = (props.get("lc_job_title") or "").strip()

    if role_text:
        # Check if any leadership keyword matches
        if _is_leadership(role_text, keywords):
            print(f"[inbound] Role '{role_text}' matched leadership keyword", file=sys.stderr)
            return leadership_boost
        else:
            print(f"[inbound] Role '{role_text}' present but not leadership — neutral", file=sys.stderr)
            return 0

    # Both fields empty — do a web search
    print("[inbound] No role data, attempting person web search...", file=sys.stderr)
    return _role_from_web_search(context, config)


def _is_leadership(text, keywords):
    """Check if text contains any leadership keyword (case-insensitive)."""
    text_lower = text.lower()
    for kw in keywords:
        # Use word boundary matching for short keywords to avoid false positives
        if len(kw) <= 3:
            if re.search(r'\b' + re.escape(kw) + r'\b', text_lower):
                return True
        else:
            if kw.lower() in text_lower:
                return True
    return False


def _role_from_web_search(context, config):
    """
    Fall back to Claude web search for person role.
    Stores result in context["_enrichment"] for the rationale builder.
    """
    props = context.get("properties", {})
    leadership_boost = config.get("role_boost_leadership", 10)

    name = (
        props.get("hs_lead_name")
        or props.get("hs_primary_associated_object_name")
        or f"{props.get('firstname', '')} {props.get('lastname', '')}".strip()
    )
    company = (props.get("hs_associated_company_name") or props.get("company") or "").strip()
    email = (props.get("email") or "").strip()

    if not name or name == " ":
        print("[inbound] No name available for web search", file=sys.stderr)
        return 0

    try:
        person_lookup = lookup_person(name, company, email)
    except Exception as e:
        print(f"[inbound] Person web search failed: {e}", file=sys.stderr)
        person_lookup = None

    # Store enrichment for rationale builder (same pattern as existing score_person_role.py)
    if "_enrichment" not in context:
        context["_enrichment"] = {}
    context["_enrichment"]["person_lookup"] = person_lookup

    if not person_lookup:
        print("[inbound] Person web search returned no results", file=sys.stderr)
        return 0

    # Check if the web search found a leadership role
    seniority = person_lookup.get("seniority", "unknown")
    is_decision_maker = person_lookup.get("is_decision_maker", False)
    title = person_lookup.get("title", "")

    leadership_seniorities = {"founder", "c_suite", "clinical_leader", "vp", "director"}
    if seniority in leadership_seniorities or is_decision_maker:
        print(f"[inbound] Web search found leadership: {title} ({seniority})", file=sys.stderr)
        return leadership_boost

    # Check title against leadership keywords as a final fallback
    if title and _is_leadership(title, config.get("leadership_keywords", [])):
        print(f"[inbound] Web search title '{title}' matched leadership keyword", file=sys.stderr)
        return leadership_boost

    print(f"[inbound] Web search found non-leadership: {title} ({seniority})", file=sys.stderr)
    return 0


# ─── Criterion 4: Contact Product Usage (boost only) ────────────────────────

def _score_contact_product_usage(props, config):
    """
    Boost based on contact's product usage: session count + recency.
    Returns +0 to +8.
    """
    cu_config = config.get("contact_usage", {})
    high_sessions = cu_config.get("high_sessions", 5)
    mid_sessions = cu_config.get("mid_sessions", 2)
    recency_days = cu_config.get("recency_days", 30)
    boost_high = cu_config.get("boost_high", 8)
    boost_mid = cu_config.get("boost_mid", 6)
    boost_low = cu_config.get("boost_low", 3)

    # Parse session count
    session_count = _parse_number(props.get("db_session_count"))
    if session_count is None or session_count <= 0:
        return 0

    # Parse last active date
    is_recent = _is_recent(props.get("db_last_active_date"), recency_days)

    # Tiered boost
    if session_count >= high_sessions and is_recent:
        return boost_high
    elif session_count >= mid_sessions and is_recent:
        return boost_mid
    else:
        # Any sessions at all
        return boost_low


# ─── Criterion 5: Company Product Usage ─────────────────────────────────────

def _score_company_product_usage(company, config):
    """
    Score based on company-level product usage.
    Returns (score_0_100, has_data_bool).
    When has_data=False, this criterion is excluded from the weighted average.
    """
    cu_config = config.get("company_usage", {})

    # Parse raw values
    session_count = _parse_number(company.get("db_company_session_count"))
    user_count = _parse_number(company.get("number_of_heidi_users"))

    # If no data at all, exclude from scoring
    if (session_count is None or session_count <= 0) and (user_count is None or user_count <= 0):
        return 0, False

    # User count scoring
    user_weight = cu_config.get("user_weight", 0.6)
    user_score = _tier_score(
        user_count or 0,
        cu_config.get("user_tiers", []),
        cu_config.get("user_default_score", 0),
    )

    # Session count scoring
    session_weight = cu_config.get("session_weight", 0.4)
    session_score = _tier_score(
        session_count or 0,
        cu_config.get("session_tiers", []),
        cu_config.get("session_default_score", 0),
    )

    # Blended score
    blended = round(user_score * user_weight + session_score * session_weight)
    return max(0, min(100, blended)), True


# ─── Composite Calculation ───────────────────────────────────────────────────

def _compute_inbound_composite(size_score, message_score, company_usage_score,
                                has_message, has_company_data, config):
    """
    Compute the weighted base score (before boosts).

    Weight selection:
    - has_message + has_company_data → weights_with_company
    - has_message + no company data  → weights_without_company
    - no message + has_company_data  → redistribute message weight to size/company
    - no message + no company data   → weights_size_only
    """
    if has_message and has_company_data:
        weights = config.get("weights_with_company", {"size": 0.45, "message": 0.30, "company_usage": 0.25})
        base = (
            size_score * weights["size"]
            + message_score * weights["message"]
            + company_usage_score * weights["company_usage"]
        )
        return round(base), weights

    elif has_message and not has_company_data:
        weights = config.get("weights_without_company", {"size": 0.55, "message": 0.45})
        base = size_score * weights["size"] + message_score * weights["message"]
        return round(base), weights

    elif not has_message and has_company_data:
        # Redistribute message weight proportionally to size + company_usage
        wc = config.get("weights_with_company", {"size": 0.45, "message": 0.30, "company_usage": 0.25})
        size_w = wc["size"]
        company_w = wc["company_usage"]
        total = size_w + company_w
        weights = {
            "size": size_w / total,
            "company_usage": company_w / total,
        }
        base = size_score * weights["size"] + company_usage_score * weights["company_usage"]
        return round(base), weights

    else:
        # No message, no company data — size only
        weights = config.get("weights_size_only", {"size": 1.0})
        return round(size_score * weights["size"]), weights


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _parse_number(value):
    """Safely parse a numeric value from HubSpot (may be string, None, or empty)."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _is_recent(date_value, days):
    """Check if a date string is within the last N days."""
    if not date_value:
        return False
    try:
        # HubSpot dates can be ISO format or epoch ms
        if isinstance(date_value, (int, float)):
            dt = datetime.fromtimestamp(date_value / 1000, tz=timezone.utc)
        else:
            # Try ISO format
            date_str = str(date_value).strip()
            if 'T' in date_str:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            else:
                dt = datetime.strptime(date_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        return dt >= cutoff

    except (ValueError, TypeError, OSError):
        return False


def _tier_score(value, tiers, default):
    """Match a numeric value against descending tiers [{min, score}]."""
    for tier in tiers:
        if value >= tier["min"]:
            return tier["score"]
    return default
