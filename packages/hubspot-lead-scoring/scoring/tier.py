"""Tier classification and rationale generation for scored leads."""

from .config import get_tier_config, get_person_role_config, get_inbound_scoring_config
from .score_person_role import classify_title


def classify_tier(score):
    """Map a numeric score (0-100) to a tier label."""
    for tier in get_tier_config():
        if score >= tier['min_score']:
            return tier['label']
    return "D-Baseline"


def format_tier_display(tier_label, score):
    """Format as 'A-Priority [78]'."""
    return f"{tier_label} [{score}]"


# ─── Rationale builder ───────────────────────────────────────────────────────

_SENIORITY_PHRASES = {
    "founder": "a founder/owner",
    "clinical_leader": "a clinical leader",
    "c_suite": "a C-suite executive",
    "vp": "a VP-level contact",
    "director": "a director-level contact",
    "manager": "a manager-level contact",
    "senior": "a senior contributor",
    "individual": "an individual contributor",
    "unknown": "a contact with unclassified role",
}


def build_rationale(record):
    """
    Build a concise, holistic rationale explaining why a lead received its tier.
    Synthesizes all signals into a readable narrative.

    Dispatches to inbound-specific rationale when lead_type == "inbound"
    and the new 5-criteria system was used (detected by "size" in sub_scores).
    """
    sub = record.get('sub_scores', {})
    lead_type = record.get('lead_type', 'other')

    # Inbound leads scored with the new 5-criteria system
    if lead_type == "inbound" and "size" in sub:
        return _build_inbound_rationale(record)

    # Original rationale for non-inbound leads
    return _build_generic_rationale(record)


def _build_generic_rationale(record):
    """Original rationale builder for non-inbound leads (product, event, other)."""
    raw = record.get('raw_inputs', {})
    props = raw.get('merged_properties', {})
    sub = record.get('sub_scores', {})
    lead_type = record.get('lead_type', 'other')
    tier = record.get('tier', '')
    score = record.get('score', 0)

    person_lookup = raw.get('person_lookup')

    # ─── Gather signal fragments ──────────────────────────────────────────
    who = _who_fragment(props, person_lookup)
    engagement = _engagement_fragment(raw, props, sub, lead_type)
    opportunity = _opportunity_fragment(sub, props)
    org_fit = _org_fit_fragment(sub, props)

    # ─── Build holistic sentence ──────────────────────────────────────────
    # Core: who + engagement
    sentence = f"{who} who {engagement}"

    # Append qualifying context
    qualifiers = []
    if opportunity:
        qualifiers.append(opportunity)
    if org_fit:
        qualifiers.append(org_fit)

    if qualifiers:
        sentence += ", " + ", and ".join(qualifiers) if len(qualifiers) > 1 else ", " + qualifiers[0]

    # Wrap with tier-level summary
    if tier == "A-Priority":
        return f"High-priority lead: {sentence}."
    elif tier == "B-Hot":
        return f"Hot lead: {sentence}."
    elif tier == "C-Warm":
        return f"Warm lead: {sentence}."
    else:
        return f"Baseline lead: {sentence}."


def _build_inbound_rationale(record):
    """
    Build rationale for inbound leads scored with the 5-criteria system.
    Covers: size, role, message, contact usage, company usage.
    """
    raw = record.get('raw_inputs', {})
    props = raw.get('merged_properties', {})
    sub = record.get('sub_scores', {})
    tier = record.get('tier', '')

    person_lookup = raw.get('person_lookup')

    # ─── Gather fragments ─────────────────────────────────────────────────
    who = _who_fragment(props, person_lookup)
    engagement = _engagement_fragment(raw, props, sub, "inbound")
    size_frag = _inbound_size_fragment(sub, props)
    usage_frag = _inbound_usage_fragment(sub, props)

    # ─── Build sentence ───────────────────────────────────────────────────
    sentence = f"{who} who {engagement}"

    qualifiers = []
    if size_frag:
        qualifiers.append(size_frag)
    if usage_frag:
        qualifiers.append(usage_frag)

    if qualifiers:
        sentence += ", " + ", and ".join(qualifiers) if len(qualifiers) > 1 else ", " + qualifiers[0]

    # Wrap with tier-level summary
    if tier == "A-Priority":
        return f"High-priority lead: {sentence}."
    elif tier == "B-Hot":
        return f"Hot lead: {sentence}."
    elif tier == "C-Warm":
        return f"Warm lead: {sentence}."
    else:
        return f"Baseline lead: {sentence}."


def _who_fragment(props, person_lookup=None):
    """Return a rich description of the person using lead data + web search enrichment.

    Combines HubSpot properties with the person web search result to build the
    most informative description possible, e.g.:
    'A clinical leader (Chief Medical Officer, physician and key decision-maker) at Mayo Clinic'
    """
    # Title: prefer Lead user_role, then Contact jobtitle, then web search title
    title = (props.get('user_role') or props.get('jobtitle') or '').strip()
    lookup_title = (person_lookup.get('title') or '') if person_lookup else ''
    best_title = title or lookup_title

    company = (props.get('hs_associated_company_name') or props.get('company') or '').strip()
    config = get_person_role_config()

    # Determine seniority from best available title
    if best_title:
        seniority = classify_title(best_title, config)
        # If HubSpot title didn't classify but web search found a seniority, use that
        if seniority == 'unknown' and person_lookup and person_lookup.get('seniority', 'unknown') != 'unknown':
            seniority = person_lookup['seniority']
        phrase = _SENIORITY_PHRASES.get(seniority, f"a {best_title.lower()}")
    else:
        phrase = "an unknown contact"

    # Build parenthetical detail from web search enrichment
    detail_parts = []

    # Include the actual title if web search found one and it differs from seniority phrase
    if lookup_title and lookup_title.lower() not in phrase.lower():
        detail_parts.append(lookup_title)

    if person_lookup:
        if person_lookup.get('is_clinical'):
            detail_parts.append("physician")
        if person_lookup.get('is_decision_maker'):
            detail_parts.append("key decision-maker")
        notable = person_lookup.get('notable_achievements')
        if notable and len(notable) < 80:
            detail_parts.append(notable)

    # Capitalize first letter properly (handle "a VP" → "A VP", not "A vp")
    result = phrase[0].upper() + phrase[1:]

    if detail_parts:
        result += f" ({', '.join(detail_parts)})"

    if company:
        return f"{result} at {company}"
    return result


def _engagement_fragment(raw, props, sub, lead_type):
    """Return engagement verb phrase: 'submitted a strong inbound inquiry'."""
    form_count = raw.get('form_count', 0)
    message = (props.get('message__form_submission_') or props.get('message') or '').strip()
    has_message = bool(message) and len(message) > 10

    if form_count > 0 and has_message:
        # Use message_boost (tiered) for inbound leads, fall back to message_analysis for legacy
        msg_boost = sub.get('message_boost', 0)
        if msg_boost >= 15:
            return "submitted a strong inbound inquiry with clear buying signals"
        elif msg_boost >= 8:
            return "submitted an inbound inquiry with moderate interest signals"
        elif msg_boost > 0:
            return "submitted an inbound inquiry with some interest signals"
        else:
            return "submitted an inbound form with a generic message"

    if form_count > 0:
        return f"submitted {form_count} inbound form{'s' if form_count > 1 else ''}"

    if lead_type == 'product':
        return "signed up through a product or trial flow"
    if lead_type == 'event':
        return "engaged through an event or conference"

    return "has no direct inbound engagement on record"


def _opportunity_fragment(sub, props):
    """Return opportunity size fragment or None."""
    opp_score = sub.get('opportunity_size')
    if opp_score is None:
        return None

    team_size = props.get('team_size')
    employees = props.get('numemployees') or props.get('numberofemployees')

    if opp_score >= 80:
        if team_size:
            return f"with a large opportunity (team of ~{team_size})"
        if employees:
            return f"with a large opportunity (~{employees} employees)"
        return "with a large estimated opportunity"
    elif opp_score >= 50:
        if team_size:
            return f"with a moderate opportunity (team of ~{team_size})"
        return "with a moderate estimated opportunity"
    else:
        return "with limited opportunity size indicators"


def _org_fit_fragment(sub, props):
    """Return specialty/org fit fragment or None."""
    spec_score = sub.get('specialty_company')
    if spec_score is None:
        return None

    specialty = (props.get('contact_specialty__f_2_') or '').strip()
    company = (props.get('hs_associated_company_name') or props.get('company') or '').strip()

    specialty_part = None
    org_part = None

    if specialty:
        if spec_score >= 75:
            specialty_part = f"high-value specialty ({specialty})"
        elif spec_score >= 50:
            specialty_part = f"moderate specialty fit ({specialty})"
        else:
            specialty_part = f"lower-priority specialty ({specialty})"

    if spec_score >= 70 and company:
        org_part = f"notable organization ({company})"

    if specialty_part and org_part:
        return f"with {specialty_part} at a {org_part}"
    elif specialty_part:
        return f"in a {specialty_part}"
    elif org_part:
        return f"at a {org_part}"

    return None


# ─── Inbound-specific fragments ──────────────────────────────────────────────

_SIZE_LABELS = {
    "JUST_ME": "solo practitioner",
    "TWO_TO_FIVE": "small group (2-5)",
    "SIX_TO_TWENTY": "mid-size group (6-20)",
    "TWENTY_ONE_TO_FIFTY": "mid-size group (21-50)",
    "FIFTY_ONE_PLUS": "large group (51+)",
    "FIVEHUNDREDPLUS": "enterprise (500+)",
    "UNKNOWN": "unknown size",
}


def _inbound_size_fragment(sub, props):
    """Describe org size for inbound rationale."""
    size_score = sub.get('size')
    if size_score is None:
        return None

    # Try to get the enum value for a descriptive label (same fallback chain as scoring)
    org_size = (
        (props.get('organization_size') or '').strip().upper()
        or (props.get('organisation_size__product_') or '').strip().upper()
        or (props.get('company_employee_size_range__c_') or '').strip().upper()
    )
    size_label = _SIZE_LABELS.get(org_size, "")

    if size_score >= 60:
        if size_label:
            return f"from a {size_label}"
        return "from a qualifying organization"
    elif size_score > 0:
        if size_label:
            return f"from a {size_label}"
        return "from a smaller organization"
    else:
        # Score is 0 — non-qualifying size or no data
        if org_size in ("JUST_ME", "TWO_TO_FIVE"):
            return f"from a {size_label}" if size_label else "from a small practice"
        elif org_size:
            return f"from a {size_label}" if size_label else "with unrecognized size data"
        return None  # No size data at all — omit from rationale


def _inbound_usage_fragment(sub, props):
    """Describe product usage signals (contact + company) for inbound rationale."""
    parts = []

    # Contact usage boost (thresholds match config: +10 for >10 sessions, +5 for >5)
    contact_boost = sub.get('contact_usage_boost', 0)
    if contact_boost >= 10:
        parts.append("highly active product user")
    elif contact_boost >= 5:
        parts.append("active product user")

    # Company usage boost (thresholds match config: +15 for strong, +10 moderate, +5 some)
    company_boost = sub.get('company_boost', 0)
    if company_boost >= 15:
        parts.append("strong existing product usage at their company")
    elif company_boost >= 10:
        parts.append("moderate product adoption at their company")
    elif company_boost > 0:
        parts.append("some product usage at their company")

    if not parts:
        return None

    return "with " + " and ".join(parts)
