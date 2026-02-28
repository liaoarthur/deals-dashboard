"""Inbound lead scoring — 5-criteria system for inbound leads.

Positive marking: leads start at 0 and only gain points from criteria
that demonstrate real intent. Non-qualifying sizes, generic messages,
excluded roles, and low product usage all score 0.

Size is the sole foundation. All other criteria are additive boosts.

Criteria:
  1. Size         — org size enum × field confidence → base score (0 for non-qualifying)
  2. Role         — leadership detection with exclusions → boost (+0 or +25)
  3. Message      — buying intent via Claude analysis → boost (+0, +8, +18, or +25)
  4. Contact Usage — product session count + recency → boost (+0, +5, or +10)
  5. Company Usage — company session/user count → boost (+0, +5, +10, or +15)

Architecture:
  Final = min(100, size_score + role_boost + message_boost + company_boost + contact_boost)
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
    Run all 5 inbound scoring criteria and compute the final score.

    Size is the sole base. All other criteria are additive boosts.
    Final = min(100, size + role + message + company + contact)

    Returns dict with:
      - score (0-100)
      - sub_scores: {size, role_boost, message_boost, company_boost, contact_usage_boost, ...}
      - modules_run: list of criteria that ran
    """
    config = get_inbound_scoring_config()
    props = context.get("properties", {})
    company = context.get("company", {})

    sub_scores = {}
    modules_run = []

    # ── Criterion 1: Size (base score) ─────────────────────────────────
    size_score = _score_size(props, config)
    sub_scores["size"] = size_score
    modules_run.append("size")
    print(f"[inbound] size={size_score}", file=sys.stderr)

    # ── Criterion 2: Role (boost) ──────────────────────────────────────
    role_boost = _score_role(context, config)
    sub_scores["role_boost"] = role_boost
    modules_run.append("role_boost")
    print(f"[inbound] role_boost=+{role_boost}", file=sys.stderr)

    # ── Criterion 3: Message analysis (boost) ──────────────────────────
    message_text = extract_message_text(context)
    message_score = None
    if message_text and len(message_text.strip()) >= 10:
        try:
            message_score = score_message_analysis(message_text)
        except Exception as e:
            print(f"[inbound] Message analysis failed: {e}", file=sys.stderr)
            message_score = None

    message_boost = _message_intent_boost(message_score, config)
    sub_scores["message_boost"] = message_boost
    modules_run.append("message_boost")
    if message_score is not None:
        sub_scores["message_analysis_raw"] = message_score
        print(f"[inbound] message: intent={message_score} → boost=+{message_boost}", file=sys.stderr)
    else:
        print(f"[inbound] message: no analyzable message → boost=+0", file=sys.stderr)

    # ── Criterion 4: Contact product usage (boost) ─────────────────────
    contact_boost = _score_contact_product_usage(props, config)
    sub_scores["contact_usage_boost"] = contact_boost
    modules_run.append("contact_usage_boost")
    print(f"[inbound] contact_usage_boost=+{contact_boost}", file=sys.stderr)

    # ── Criterion 5: Company product usage (boost) ─────────────────────
    company_blended, has_company_data = _score_company_product_usage(company, config)
    company_boost = _company_usage_boost(company_blended, has_company_data, config)
    sub_scores["company_boost"] = company_boost
    modules_run.append("company_boost")
    if has_company_data:
        sub_scores["company_usage_raw"] = company_blended
        print(f"[inbound] company: blended={company_blended} → boost=+{company_boost}", file=sys.stderr)
    else:
        print(f"[inbound] company: no data → boost=+0", file=sys.stderr)

    # ── Final score (all additive, capped at 100) ──────────────────────
    final_score = min(100, size_score + role_boost + message_boost + company_boost + contact_boost)

    print(
        f"[inbound] size={size_score} + role=+{role_boost} + msg=+{message_boost} "
        f"+ co=+{company_boost} + contact=+{contact_boost} = {final_score}",
        file=sys.stderr,
    )

    return {
        "score": final_score,
        "sub_scores": sub_scores,
        "modules_run": modules_run,
    }


# ─── Criterion 1: Size ──────────────────────────────────────────────────────

def _score_size(props, config):
    """
    Score based on organization_size enum with confidence multiplier.

    Fallback chain (each field has a confidence multiplier applied to the base score):
      organization_size           → 100% of base score (primary)
      organisation_size__product_ → 80% of base score  (fallback1)
      company_employee_size_range__c_ → 50% of base score (fallback2)

    Positive marking: JUST_ME, TWO_TO_FIVE, UNKNOWN, and no data all return 0.
    Only SIX_TO_TWENTY(80), TWENTY_ONE_TO_FIFTY(85), FIFTY_ONE_PLUS(65),
    FIVEHUNDREDPLUS(50) get positive scores.
    """
    enum_scores = config.get("size_enum_scores", {})
    no_data = config.get("size_no_data_score", 0)
    confidence = config.get("field_confidence", {"primary": 1.0, "fallback1": 0.8, "fallback2": 0.5})

    # Try each field in order with decreasing confidence
    fields = [
        ("organization_size", confidence.get("primary", 1.0)),
        ("organisation_size__product_", confidence.get("fallback1", 0.8)),
        ("company_employee_size_range__c_", confidence.get("fallback2", 0.5)),
    ]

    for field_name, conf_multiplier in fields:
        raw = (props.get(field_name) or "").strip().upper()
        if raw and raw in enum_scores:
            base_score = enum_scores[raw]
            # No need to multiply if base score is 0 (non-qualifying size)
            if base_score == 0:
                return 0
            final = round(base_score * conf_multiplier)
            print(f"[inbound] size: {field_name}={raw} → {base_score} × {conf_multiplier} = {final}", file=sys.stderr)
            return final

    # No data at all
    return no_data


# ─── Criterion 2: Role (boost only) ─────────────────────────────────────────

def _score_role(context, config):
    """
    Determine role boost: +leadership_boost if leadership, +0 otherwise.
    Exclusion keywords are checked FIRST — e.g., "VP of Marketing" → excluded → +0.
    If both role fields are empty, perform a web search via lookup_person().
    """
    props = context.get("properties", {})
    leadership_boost = config.get("role_boost_leadership", 30)
    keywords = config.get("leadership_keywords", [])
    exclusion_keywords = config.get("role_exclusion_keywords", [])

    # Primary: organisation_type__product_ (self-reported role in product)
    role_text = (props.get("organisation_type__product_") or "").strip()

    # Fallback: lc_job_title
    if not role_text:
        role_text = (props.get("lc_job_title") or "").strip()

    if role_text:
        # Check exclusions FIRST — excluded roles get +0 even if they match leadership
        if _is_excluded_role(role_text, exclusion_keywords):
            print(f"[inbound] Role '{role_text}' matched exclusion keyword — excluded", file=sys.stderr)
            return 0

        # Check if any leadership keyword matches
        if _is_leadership(role_text, keywords):
            print(f"[inbound] Role '{role_text}' matched leadership keyword → +{leadership_boost}", file=sys.stderr)
            return leadership_boost
        else:
            print(f"[inbound] Role '{role_text}' present but not leadership — neutral", file=sys.stderr)
            return 0

    # Both fields empty — do a web search
    print("[inbound] No role data, attempting person web search...", file=sys.stderr)
    return _role_from_web_search(context, config)


def _is_excluded_role(text, exclusion_keywords):
    """Check if text matches any exclusion keyword (case-insensitive).

    Exclusion keywords are checked BEFORE leadership keywords.
    E.g., "VP of Marketing" matches "marketing" → excluded → +0.
    """
    text_lower = text.lower()
    for kw in exclusion_keywords:
        # Use word boundary matching for short keywords (≤3 chars) to avoid false positives
        if len(kw) <= 3:
            if re.search(r'\b' + re.escape(kw.lower()) + r'\b', text_lower):
                return True
        else:
            if kw.lower() in text_lower:
                return True
    return False


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
    Checks exclusion keywords against web search title before awarding boost.
    """
    props = context.get("properties", {})
    leadership_boost = config.get("role_boost_leadership", 30)
    exclusion_keywords = config.get("role_exclusion_keywords", [])

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

    # Check exclusions against web search title BEFORE awarding boost
    if title and _is_excluded_role(title, exclusion_keywords):
        print(f"[inbound] Web search title '{title}' matched exclusion keyword — excluded", file=sys.stderr)
        return 0

    leadership_seniorities = {"founder", "c_suite", "clinical_leader", "vp", "director"}
    if seniority in leadership_seniorities or is_decision_maker:
        print(f"[inbound] Web search found leadership: {title} ({seniority}) → +{leadership_boost}", file=sys.stderr)
        return leadership_boost

    # Check title against leadership keywords as a final fallback
    if title and _is_leadership(title, config.get("leadership_keywords", [])):
        print(f"[inbound] Web search title '{title}' matched leadership keyword → +{leadership_boost}", file=sys.stderr)
        return leadership_boost

    print(f"[inbound] Web search found non-leadership: {title} ({seniority})", file=sys.stderr)
    return 0


# ─── Criterion 4: Contact Product Usage (boost only) ────────────────────────

def _score_contact_product_usage(props, config):
    """
    Boost based on contact's product usage: session count + recency.
    Positive marking: only meaningful usage (>5 sessions + recent) earns a boost.

    Returns +0, +5, or +10.
    """
    cu_config = config.get("contact_usage", {})
    high_sessions = cu_config.get("high_sessions", 10)
    mid_sessions = cu_config.get("mid_sessions", 5)
    recency_days = cu_config.get("recency_days", 30)
    boost_high = cu_config.get("boost_high", 10)
    boost_mid = cu_config.get("boost_mid", 5)

    # Parse session count
    session_count = _parse_number(props.get("db_session_count"))
    if session_count is None or session_count <= 0:
        return 0

    # Parse last active date
    is_recent = _is_recent(props.get("db_last_active_date"), recency_days)

    # Must be both high-usage AND recent to earn a boost (strictly greater than thresholds)
    if session_count > high_sessions and is_recent:
        return boost_high
    elif session_count > mid_sessions and is_recent:
        return boost_mid
    else:
        # Below threshold or not recent — no boost
        return 0


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


# ─── Boost Helpers ───────────────────────────────────────────────────────────

def _message_intent_boost(intent_score, config):
    """Convert Claude's 0-100 intent score to a tiered boost.

    Tiers (from config, sorted descending by min):
      70+ → +20 (strong procurement)
      50+ → +15 (clear buying signals)
      30+ → +8  (real interest)
      <30 → +0  (generic / spam / absent)
    """
    if intent_score is None:
        return 0
    tiers = config.get("message_boost_tiers", [])
    for tier in tiers:
        if intent_score >= tier["min"]:
            return tier["boost"]
    return 0


def _company_usage_boost(blended_score, has_data, config):
    """Convert company usage blended score (0-100) to a tiered boost.

    Tiers (from config, sorted descending by min):
      70+ → +15 (strong adoption)
      40+ → +10 (moderate adoption)
      1+  → +5  (some usage)
      0/no data → +0
    """
    if not has_data:
        return 0
    tiers = config.get("company_boost_tiers", [])
    for tier in tiers:
        if blended_score >= tier["min"]:
            return tier["boost"]
    return 0


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
