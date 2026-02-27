"""Specialty & Company scoring module.

Evaluates the contact's medical specialty and associated organization
to determine strategic fit. Medical specialties score higher than
allied health; organizations are evaluated via Claude web search.

Hospital systems and academic medical centers only score highly if the
person is also notable (decision-maker, leadership role). A regular
physician at a major system won't inflate the score. Specialty clinics
are grouped with physician groups.
"""

import re
import sys
from functools import lru_cache

from .config import get_specialty_company_config
from .claude_client import lookup_company
from .score_person_role import _looks_like_domain, _best_company_name


@lru_cache(maxsize=500)
def _cached_company_lookup(company_name):
    """Cache company lookups to avoid redundant API calls."""
    return lookup_company(company_name)


def score(context):
    """Compute specialty + company sub-score (0-100)."""
    config = get_specialty_company_config()
    props = context.get("properties", {})
    lead_props = context.get("lead_properties", {})
    company = context.get("company", {})

    specialty = (props.get("contact_specialty__f_2_") or "").strip().lower()
    lead_company = (lead_props.get("hs_associated_company_name") or "").strip()
    hub_company = (company.get("name") or "").strip()
    contact_company = (props.get("company") or "").strip()
    company_name = _best_company_name(lead_company, hub_company, contact_company)

    # Get person lookup result from cross-module enrichment
    person_lookup = context.get("_enrichment", {}).get("person_lookup")

    signals = []

    # ─── Specialty scoring ─────────────────────────────────────────────────
    has_specialty = bool(specialty)
    if has_specialty:
        specialty_score = _score_specialty(specialty, config)
        signals.append(f"specialty='{specialty}' score={specialty_score}")
    else:
        specialty_score = None
        signals.append("specialty=unknown (excluded from weighting)")

    # ─── Company/org scoring ───────────────────────────────────────────────
    if company_name:
        company_score = _score_company(company_name, company, config, person_lookup)
        signals.append(f"company='{company_name}' score={company_score}")
    else:
        company_score = config.get("fallback_company_score", 40)
        signals.append("company=unknown")

    # ─── Composite ────────────────────────────────────────────────────────
    # If specialty is unknown, use company score only (don't penalize for missing data)
    if has_specialty:
        specialty_weight = config.get("specialty_weight", 0.6)
        company_weight = config.get("company_weight", 0.4)
        final = int(specialty_score * specialty_weight + company_score * company_weight)
    else:
        final = company_score

    final = max(0, min(100, final))

    print(f"[specialty_company] score={final} signals={signals}", file=sys.stderr)
    return final


def _match_specialty(keyword, specialty):
    """Check if a keyword matches the specialty using word-boundary start matching.
    The keyword must start at a word boundary, but can be a prefix of a longer word
    (e.g., 'pediatric' matches 'pediatrics'). This avoids false positives like
    'ent' matching inside 'dental' while allowing natural plural/suffix variations."""
    pattern = r'(?:^|[\s,/&\-])' + re.escape(keyword)
    return bool(re.search(pattern, specialty))


def _score_specialty(specialty, config):
    """Score a specialty string against configured tiers."""
    # Check high-priority medical specialties
    high_priority = config.get("high_priority_specialties", [])
    for kw in high_priority:
        if _match_specialty(kw, specialty):
            return config.get("high_priority_score", 90)

    # Check medium-priority specialties
    medium_priority = config.get("medium_priority_specialties", [])
    for kw in medium_priority:
        if _match_specialty(kw, specialty):
            return config.get("medium_priority_score", 65)

    # Check low-priority (allied health, etc.)
    low_priority = config.get("low_priority_specialties", [])
    for kw in low_priority:
        if _match_specialty(kw, specialty):
            return config.get("low_priority_score", 35)

    # Has a specialty but not in our lists — moderate score
    return config.get("other_specialty_score", 50)


def _score_company(company_name, company_props, config, person_lookup):
    """Score the company/organization via Claude web search, considering person's role."""
    result = _cached_company_lookup(company_name)

    if not result or result.get("confidence") == "low":
        print(f"[specialty_company] company lookup low confidence, using size fallback", file=sys.stderr)
        return _score_company_by_size(company_props, config)

    org_type = result.get("org_type", "unknown")
    org_type_scores = config.get("org_type_scores", {})
    base = org_type_scores.get(org_type, org_type_scores.get("unknown", 40))

    # GATE: hospital_system / academic_medical only score high
    # if the person is notable (decision-maker, leadership, etc.)
    if org_type in ("hospital_system", "academic_medical"):
        person_notable = _person_is_notable(person_lookup)
        if not person_notable:
            cap = config.get("large_org_non_notable_cap", 50)
            if base > cap:
                print(
                    f"[specialty_company] large org '{company_name}' ({org_type}) capped {base} → {cap} "
                    f"(person not notable)",
                    file=sys.stderr,
                )
                base = cap

    # Notable org boost
    if result.get("is_notable"):
        base = min(100, base + config.get("notable_boost", 10))

    # Large size boost — only for healthcare provider orgs, not vendors or non-healthcare
    if result.get("estimated_size") == "large" and org_type not in ("healthtech_vendor", "non_healthcare"):
        base = max(base, 75)

    print(
        f"[specialty_company] company lookup: org_type={org_type} "
        f"size={result.get('estimated_size')} notable={result.get('is_notable')} "
        f"person_notable={_person_is_notable(person_lookup)} "
        f"final_company_score={min(100, base)}",
        file=sys.stderr,
    )
    return min(100, base)


def _person_is_notable(person_lookup):
    """Check if the person is notable enough to justify high org scoring."""
    if not person_lookup:
        return False
    if person_lookup.get("is_decision_maker"):
        return True
    seniority = person_lookup.get("seniority", "unknown")
    if seniority in ("founder", "c_suite", "clinical_leader", "vp", "director"):
        return True
    return False


def _score_company_by_size(company_props, config):
    """Fallback company scoring based on employee count when web search fails."""
    employees = None
    try:
        employees = float(str(company_props.get("numberofemployees", "0")).replace(",", ""))
    except (ValueError, TypeError):
        pass

    if employees and employees > 500:
        return 75
    elif employees and employees > 100:
        return 60

    return config.get("fallback_company_score", 40)
