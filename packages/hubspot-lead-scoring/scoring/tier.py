"""Tier classification and rationale generation for scored leads."""

from .config import get_tier_config, get_person_role_config
from .score_person_role import classify_title


def classify_tier(score):
    """Map a numeric score (0-100) to a tier label."""
    for tier in get_tier_config():
        if score >= tier['min_score']:
            return tier['label']
    return "C-Routine"


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
    """
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
    elif tier == "B-Monitor":
        return f"Worth monitoring: {sentence}."
    else:
        return f"Routine lead: {sentence}."


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
        msg_score = sub.get('message_analysis')
        if msg_score is not None and msg_score >= 70:
            return "submitted a strong inbound inquiry with clear buying signals"
        elif msg_score is not None and msg_score >= 40:
            return "submitted an inbound inquiry with moderate interest signals"
        else:
            return "submitted an inbound form with limited purchase intent"

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
